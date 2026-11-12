# Job modes

Spark Pudding exposes three job modes. They describe the user goal; the execution engine is
derived from the mode and is not configured separately.

| Mode | Use case | Topology | Execution |
| --- | --- | --- | --- |
| `batch` | General bounded processing | one or more sources and transforms, and one sink | batch |
| `sync` | One-source/one-sink data synchronization | one source and one sink; transforms allowed with strategy rules | batch |
| `streaming` | Continuous processing | one source and one sink | structured streaming |

Every mode writes exactly one sink. One run has one commit boundary: with multiple sinks, a
failure after an earlier sink commits would leave only part of the pipeline applied, and the
engine does not provide a distributed transaction across sinks. Split a multi-target pipeline
into separate jobs and coordinate them with the external scheduler.

```yaml
job:
  mode: batch | sync | streaming
```

`job.mode` is required and has no implicit default.

## Batch

`batch` is the general bounded-processing mode:

- any number of sources and transforms, but exactly one sink;
- transforms may be filter, column, union, aggregate, dedup, validate, join, limit or SQL;
- the sink uses its own write mode; batch does not inject full-refresh overwrite;
- `source.config.incremental` is not allowed; use `sync` with `strategy: incremental`;
- `job.strategy` and `job.trigger` are not allowed.

```yaml
job:
  mode: batch
```

## Sync

`sync` is a constrained synchronization between one source and one sink. It requires a strategy.

```yaml
job:
  mode: sync
  strategy: full | incremental
```

`job.trigger` is not allowed because sync is batch-only.

### Sync full

- one source and one sink;
- the sink must support full-refresh writes;
- the sink must support atomic replacement, unless the sink config explicitly sets
  `allowNonAtomicReplace: true`; `file-sink` uses a staged backup/swap and still requires that
  opt-in because a plain file directory has no atomic overwrite rename;
- the engine injects `mode=overwrite` and `saveMode=overwrite` into the sink config;
- current atomic sinks are `jdbc-sink`, `mysql-sink`, `iceberg-sink`, `paimon-sink` and
  `hudi-sink`; `file-sink` and `hive-sink` require the explicit non-atomic opt-in;
- transforms are allowed;
- an incremental source configuration is not allowed;
- no cursor state is used.

```yaml
job:
  mode: sync
  strategy: full
```

### Sync incremental

- one source and one sink;
- the source must contain an enabled `source.config.incremental` block;
- `incremental.statePath` or `incremental.state` is required;
- Hudi/Paimon/Iceberg sources use their native incremental reads; `jdbc-source` and
  `mysql-source` additionally require a top-level `cursorField`;
- the sink must support the selected `append`, `upsert` or `cdc` semantics;
- transforms are allowed only when they preserve key, operation and ordering metadata;
- `jdbc-sink` and `mysql-sink` apply append/upsert/CDC in this mode; JDBC streaming
  `cdcOpColumn` remains unsupported;
- `initial: full|latest` is the first-run behavior; later runs continue from the committed cursor;
- cursor progress, pending transactions and the single-writer lock use the incremental state
  backend.

```yaml
job:
  mode: sync
  strategy: incremental
source:
  - type: hudi-source
    config:
      tablePath: hdfs://namenode:8020/hudi/orders
      incremental:
        semantics: cdc
        keyFields: [id]
        opColumn: __op
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
sink:
  - type: paimon-sink
    dependency: source-1
    config:
      table: sales.orders
      primaryKey: [id]
      mode: append
```

## Streaming

`streaming` is continuous Structured Streaming:

- one source and one sink;
- Kafka offsets are stored in the sink's Spark checkpoint;
- `source.config.incremental` is not allowed;
- `job.strategy` is not allowed;
- `job.trigger` is optional; when omitted the sink runs continuously;
- transforms must support streaming execution;
- stateful transforms must declare an event-time watermark and a bounded window or dedup state;
- stateful transforms require a sink with a stable checkpoint; the console sink is rejected;
- a stateful checkpoint is protected by a source/transform/Spark-version contract; a changed
  contract fails the restart before Spark starts the query;
- append-mode consumers receive only windows whose end has passed the watermark;
- `outputMode: update` is supported by `hudi-sink`, `iceberg-sink`, `paimon-sink` and
  `elasticsearch-sink`; it requires key fields and writes changed rows with upsert semantics;
- `outputMode: complete` is supported only by the console display sink;
- file, Kafka and MongoDB streaming sinks are append-only;
- the sink owns its checkpoint and waits for the streaming query to terminate;
- `checkpointRetention` defaults to `keep`; see
  [streaming-checkpoints.md](streaming-checkpoints.md).

```yaml
job:
  mode: streaming
  trigger: "processing-time: 30 seconds"
```

Streaming is not the same as `sync incremental`:

| | `sync incremental` | `streaming` |
| --- | --- | --- |
| Input | bounded source | unbounded source |
| Progress | cursor journal + pending transaction + lock | Spark checkpoint |
| Recovery | recovery policy + state tool | checkpoint replay + sink idempotence |
| Execution | batch | structured streaming |

## Mode fields

| Field | batch | sync | streaming |
| --- | --- | --- | --- |
| `job.mode` | required | required | required |
| `job.strategy` | forbidden | required (`full` / `incremental`) | forbidden |
| `job.trigger` | forbidden | forbidden | optional |

`job.execution` and the old mode names `full`, `incremental` and `etl` are rejected. The engine
does not migrate old job configurations; rewrite the job file for the current mode model before
running it. There is no compatibility switch.

## Scheduling

This project does not schedule jobs and has no internal scheduler. Scheduling is an external
concern. Use Airflow, cron, a Kubernetes CronJob, a YARN scheduling mechanism or another
orchestrator to:

- run `batch` and `sync` jobs on the required cadence;
- restart `streaming` jobs after failure or maintenance;
- decide when a `sync incremental` job retries after a failed run.

A `batch` or `sync` invocation runs once and exits. A `streaming` invocation stays alive until the
query terminates; the orchestrator is responsible for starting it again and for keeping the
checkpoint location stable across restarts. See
[streaming-checkpoints.md](streaming-checkpoints.md) for checkpoint retention and cleanup.

Lakehouse table maintenance (compaction, snapshot expiry, Hudi table services and orphan cleanup)
is also a one-shot external operation, not a job mode. Use
[table-maintenance.md](table-maintenance.md) from the same orchestrator.

## Transforms

| Transform | Batch | Streaming | Preserves incremental metadata |
| --- | --- | --- | --- |
| `filter-transform` | yes | yes | yes |
| `column-transform` | yes | yes | yes if key/op/order columns are kept |
| `union-transform` | yes | yes | yes |
| `aggregate-transform` | yes | yes with `eventTime`, `watermark` and event-time `window` | no |
| `dedup-transform` | yes | yes with `eventTime` and `watermark` | no |
| `join-transform` | yes | inner stream-stream join with both event times and watermarks | no |
| `limit-transform` | yes | no | no |
| `sql-transform` | yes | yes for stateless SQL; stateful SQL needs explicit watermark/state design | no |
| `validate-transform` | yes | yes (`mode: drop`) | yes |
| `schema-contract-transform` | yes | yes (schema is checked at planning time) | yes |

A `sync` with `strategy: full` accepts any transform. A `sync` with `strategy: incremental`
accepts only transforms that preserve key, operation and ordering metadata. `dedup-transform` is
not allowed there: keeping one arbitrary row per key discards the operation and ordering metadata
that the sink needs to propagate deletes. A `streaming` job accepts transforms that declare
streaming support. Use `sql-transform` for stateless SQL plans;
stateful SQL is not covered by the engine's state-contract validation. `aggregate-transform`,
`dedup-transform` and inner `join-transform` are the supported stateful streaming operators; all
require event-time watermarks so the state store is bounded. Outer and semi/anti streaming joins
are not supported. See [streaming-state.md](streaming-state.md).

## Validation summary

Before Spark starts, the engine rejects:

- a missing or unsupported `job.mode`;
- `job.strategy` on batch or streaming;
- a missing `job.strategy` on sync;
- `job.trigger` on batch or sync;
- more than one sink in any mode;
- multiple sources in sync or streaming;
- a sync full sink without full-refresh support;
- a sync full sink without atomic replacement and without `allowNonAtomicReplace: true`;
- incremental source configuration in batch, sync full or streaming;
- sync incremental without an enabled incremental block or state;
- sync incremental transforms that drop key/op/order metadata;
- upstream components that do not support the derived execution mode;
- a stateful streaming transform without its required event-time watermark or bounded state;
- a stateful streaming transform with a sink that cannot persist operator state;
- a streaming output mode the sink does not declare;
- a streaming join that is not inner or lacks one side's event time or watermark;
- `checkpointRetention=delete-on-success` without a finite trigger or a checkpoint;
- the removed `job.enableHiveSupport` key (use
  `job.config.spark.sql.catalogImplementation: hive`);
- `job.execution` and the old `full`, `incremental` and `etl` mode names.
