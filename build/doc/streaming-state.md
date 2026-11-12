# Stateful streaming

Stateful streaming computes results that depend on rows from earlier micro-batches. Spark keeps
the intermediate state in a state store backed by the sink's checkpoint. Spark Pudding supports
three stateful streaming operators:

| Operator | State | Required settings |
| --- | --- | --- |
| `aggregate-transform` | Open event-time windows and their aggregate values | `eventTime`, `watermark`, `window` |
| `dedup-transform` | Keys seen within the watermark horizon | `keys`, `eventTime`, `watermark` |
| `join-transform` | Unmatched rows on both sides of an inner stream-stream join | `leftEventTime`, `rightEventTime`, `leftWatermark`, `rightWatermark`; `joinType: inner` |

Other transforms remain stateless in streaming. `limit-transform` is batch-only. Use
`sql-transform` for stateless SQL plans; a stateful SQL query would bypass the state-contract
validation, so express stateful logic with the dedicated operators above.

## Watermark and state retention

A watermark is an event-time bound. Spark uses it for two things:

1. to decide when a window is final; and
2. to evict state that can no longer affect future output.

There is no separate state-TTL knob for these operators. The `watermark` delay is the retention
boundary:

- an aggregation window is emitted in append mode after the watermark passes `window_end`;
- deduplication state is evicted when the watermark passes a record's event time;
- stream-stream join state is evicted on each side when the corresponding watermark advances;
- records older than the watermark are dropped by the stateful operator.

A larger watermark delay tolerates more late data but keeps more state. A smaller delay closes
windows sooner and uses less state, but drops late records earlier. The watermark must advance
for state to be released; an idle stream with no new event-time data holds its existing state.

Deduplication is bounded, not absolute. A duplicate that arrives after its key was evicted can be
emitted again. Use a primary-key/upsert sink when exact deduplication across the whole history is
required. Which row is retained for a key is not deterministic.

## Event-time input

`eventTime` must name a `timestamp` column. Date, string and processing-time columns are rejected.
`watermark` and `window` accept Spark interval strings such as `30 seconds`, `5 minutes`,
`2 hours` or `1 day`.

```yaml
transform:
  - type: aggregate-transform
    id: window_count
    dependencies: [kafka_orders]
    config:
      eventTime: order_time
      watermark: "10 minutes"
      window: "5 minutes"
      groupBy: [order_city]
      aggregations:
        order_count: "count(*)"
        total_amount: "sum(amount)"
```

The aggregation result has this column order:

```text
window_start, window_end, <groupBy columns>, <aggregation columns>
```

`window` can also be a map. It accepts only `duration` and `slide`:

```yaml
window:
  duration: "5 minutes"
  slide: "1 minute"
```

Without a slide, the window is tumbling. `groupBy` is optional; without it, the result is one
global value per window.

Deduplication keeps the input columns unchanged:

```yaml
transform:
  - type: dedup-transform
    id: dedup_orders
    dependencies: [kafka_orders]
    config:
      keys: [order_id]
      eventTime: order_time
      watermark: "10 minutes"
```

An inner stream-stream join requires both sides to be streaming and declares one event-time
column and watermark per side:

```yaml
transform:
  - type: join-transform
    id: orders_payments
    dependencies: [orders_stream, payments_stream]
    config:
      left: orders_stream
      right: payments_stream
      joinType: inner
      on: [order_id]
      leftEventTime: order_time
      rightEventTime: payment_time
      leftWatermark: "10 minutes"
      rightWatermark: "10 minutes"
```

Outer and semi/anti streaming joins are not part of the current contract: they additionally need
an event-time range condition and a defined state-eviction policy.

## Output modes and sinks

Stateful streaming supports three output modes with different sink requirements:

| Mode | Meaning | Supported sinks |
| --- | --- | --- |
| `append` | only newly produced rows | file, console, Hudi, Iceberg, Paimon, Kafka, MongoDB, Elasticsearch, JDBC, MySQL |
| `update` | rows changed since the previous batch | Hudi, Iceberg, Paimon (primary-key table), Elasticsearch, JDBC, MySQL |
| `complete` | the complete result table each batch | console only |

An `update` sink must identify a row across batches:

- Hudi uses `recordKeyFields` and writes a Hudi upsert commit for each micro-batch. The target
  table must already exist and may be selected by `tablePath` or a catalog location.
- Iceberg uses `recordKeyFields` and runs an idempotent `MERGE INTO` for each micro-batch. The
  target table, an Iceberg catalog and `IcebergSparkSessionExtensions` are required.
- Paimon uses the target table's primary keys and writes full rows through `PaimonSparkWriter`;
  the target must be an existing primary-key table in path or catalog mode.
- Elasticsearch requires `idField`; `operation` must be unset or one of `index`, `upsert` or
  `update`.
- JDBC and MySQL require an existing target table whose primary key or unique index matches
  `recordKeyFields` exactly, and every target column must be present in the batch. Each
  microbatch uses a private staging table and one transaction; PostgreSQL/H2 run `MERGE` and
  MySQL/MariaDB run a keyed `UPDATE ... JOIN` plus `INSERT ... WHERE NOT EXISTS`. Null or
  duplicate keys fail the batch. Rows are never deleted, so CDC operation columns are not
  supported.

`complete` is accepted only by the console display sink. Console has no checkpoint, so a
stateful transform is still rejected there; complete output is therefore limited to stateless
display/debugging. Replacing a lakehouse table on every batch would also delete rows written by
another job and makes ownership of the target ambiguous. Use `update` with keys for a persistent
sink.

File, Kafka and MongoDB streaming sinks are append-only. The engine rejects an output mode a sink
does not declare before the query starts. The sink still validates its own key fields. JDBC
streaming accepts only `mode: append` and uses `outputMode: update` to select key-based upsert.

```yaml
sink:
  - id: window_updates
    type: iceberg-sink
    dependency: window_count
    catalog: iceberg_catalog
    config:
      table: analytics.order_window_count
      outputMode: update
      recordKeyFields: [window_start, window_end, order_city]
      checkpointLocation: s3a://warehouse/checkpoints/order-window-count
```

The update target must have the same columns as the stateful transform output. Stateful update
output normally contains one row per key per micro-batch. If a batch contains duplicate keys, the
lakehouse sinks reduce them to one row before merging; the retained row is not guaranteed to be
the latest, so do not use update output as an out-of-order change log.

## Metrics

The driver registers `StreamingMetrics` on every Spark application. Stateful queries expose:

- `streaming.query.last.watermark.ms` and `streaming.query.watermark.lag.ms`;
- `streaming.state.rows.total`, `.updated`, `.removed`;
- `streaming.state.memory.bytes`;
- `streaming.state.rows.dropped.by.watermark`.

Values aggregate Spark's state operators for the query and are available through JMX and the
structured `streaming.query.state` log event. They are gauges of the latest progress, not
cumulative counters.

Health and source metrics:

- `streaming.query.last.progress.timestamp.ms` is the driver time of the last progress event.
  `now - value` is the progress age; a continuously growing age means the query is stalled.
- `streaming.query.active`, `.started`, `.completed` and `.failed` show query lifecycle state.
- `streaming.source.lag.records`, `.lag.max`, `.partitions` and `.lag.updated.ms` are populated
  by the Kafka source from Spark's consumed offset and an AdminClient latest-offset lookup.
- `streaming.source.healthy` is `1` after a successful lag lookup and `0` after a lookup failure.
  Lag lookup failures are logged without their error text and do not fail the query.

Kafka lag is measured at most once per `lagMetricsIntervalMs` (default 30000 ms). A finite-trigger
job that consumes all available data reports zero lag. The lag lookup uses the same Kafka
security options as the reader but never logs them.

## State contract

A stateful streaming job stores a contract file at
`<checkpointLocation>/_pudding_state_contract.json`. It contains a SHA-256 fingerprint of:

- the source id, type and redacted schema-affecting configuration (including the value schema);
  reader `options` and `lagMetricsIntervalMs` are excluded because they do not change the state
  schema;
- every transform's id, type, dependencies and redacted configuration, in dependency order;
- the Spark version and the contract format version.

The canonical configuration is redacted before hashing, so secrets are neither stored nor part
of the fingerprint; only the hash is written.

On the first start the contract is created. On restart:

- the same fingerprint restores the existing state;
- a different fingerprint fails the job before the streaming query starts, with instructions to
  restore the previous configuration or start a new checkpoint;
- an existing pre-contract checkpoint is adopted with a warning and the current configuration
  becomes its contract, so upgrading does not make an old checkpoint unusable.

The guard starts with the next run for an adopted checkpoint. Stateless streaming jobs do not
write a contract, so filters and projections can change without invalidating the checkpoint.

## Checkpoint and restart

State lives in the sink checkpoint. Keep the checkpoint location stable across restarts. A restart
with the same transform configuration restores the stored windows and deduplication keys.

Changing any of the following is a state-schema change and may make an existing checkpoint
incompatible:

- an event-time column or watermark, including either side of a join;
- the window, group-by columns, keys or aggregation expressions;
- the join keys or join condition;
- the transform type or the order of stateful operators.

There is no automatic state migration. Stop the writer, then either restore the existing
checkpoint (only when the logical state is unchanged) or start with a new checkpoint after
deciding how to handle the existing target data. Use `pudding checkpoint` to inspect or remove an old
checkpoint; see [streaming-checkpoints.md](streaming-checkpoints.md).

Stateful streaming is still single-source and single-sink. Exactly-once behavior depends on the
Spark checkpoint and an idempotent sink commit, as with any other streaming job.

## Validation

Validation happens before Spark resources are allocated. A streaming job is rejected when:

- `aggregate-transform` lacks `eventTime`, `watermark` or `window`;
- `dedup-transform` lacks `keys`, `eventTime` or `watermark`;
- `join-transform` lacks either side's event time or watermark, or uses a non-inner join type;
- one side of a join is batch and the other is streaming;
- `window` or `slide` is used on a non-aggregate transform;
- a streaming state setting is used in batch execution;
- a stateful transform is paired with a sink that cannot persist state (for example, console);
- the sink does not declare the requested streaming `outputMode`;
- the event-time column is missing or is not a timestamp (checked when the DataFrame is built).

At sink start, before the query begins running:

- an `update` sink without its key fields is rejected;
- a changed state contract on an existing checkpoint is rejected;
- the sink's checkpoint retention policy is validated.

## Current limitations

- Update output is supported by the sinks above; complete output is display-only. A
  persistent sink cannot receive complete output without replacing the whole target.
- Update output has no delete semantics. Use the Hudi, Iceberg or Paimon CDC path with an
  operation column when deletes must be propagated.
- Only inner stream-stream joins are supported; outer/semi/anti streaming joins need an
  event-time range condition.
- No processing-time state timers or arbitrary `mapGroupsWithState`/`flatMapGroupsWithState`
  operators.
- No automatic state migration between different transform configurations; start a new
  checkpoint after changing a stateful contract.
- The last watermark, watermark lag and aggregate state rows/memory/removal metrics are exposed
  through the streaming metrics; per-operator state metrics are not exposed separately yet.
- Kafka lag is a driver-side AdminClient measurement, throttled by `lagMetricsIntervalMs`; it is
  not a broker-side consumer-group metric.
