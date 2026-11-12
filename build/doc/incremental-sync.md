# Incremental sync

Spark Pudding supports bounded incremental reads for Hudi, Paimon, Iceberg and JDBC sources. A
job selects one semantic on the source, and every sink receives the same semantic automatically:

| Source `semantics` | Meaning | Sink behavior |
| --- | --- | --- |
| `append` | Every row is new. | Append the batch. |
| `upsert` | Rows are the latest state for their key fields. | Merge on the keys. |
| `cdc` | Every row carries an operation column (`I`, `U` or `D`). | Apply inserts, updates and deletes. |

Capabilities are checked before Spark starts. If a sink cannot apply the selected semantic, the
job fails during pipeline validation with a `PuddingException`; the configuration is never
silently downgraded to append.

## Configuration

Incremental synchronization uses `job.mode: sync` with `job.strategy: incremental`. It is a
one-source/one-sink sync; transforms are allowed only when they preserve incremental metadata.
Incremental settings live under `source.config.incremental`:

```yaml
job:
  mode: sync
  strategy: incremental
source:
  - id: hudi_orders
    type: hudi-source
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      incremental:
        semantics: cdc
        keyFields: [id]
        opColumn: __op
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
sink:
  - id: paimon_orders
    type: paimon-sink
    catalog: paimon_lake
    config:
      table: sales.orders
      primaryKey: [id]
      mode: append
    dependency: hudi_orders
```

The sink configuration does not repeat `semantics`. The selected source semantic is passed to
every sink in the pipeline.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `semantics` | string | required | `append`, `upsert` or `cdc`. |
| `keyFields` | string or list | — | Business keys. Required for `upsert` and `cdc`. The Hudi and Paimon sinks also use them as record/primary keys. |
| `opColumn` | string | `__op` | Operation column for `cdc`. The canonical values are `I`, `U` and `D`. |
| `start` | string | — | Bootstrap cursor. Ignored once incremental state contains a committed cursor. |
| `end` | string | — | Pins the read to an end cursor. The source normally resolves the latest cursor. |
| `initial` | string | `none` | First-read behavior: `none`, `latest` or `full`. |
| `statePath` | string | — | Filesystem state directory. Required when `state` is absent. |
| `state` | map | — | Pluggable state backend: `backend` plus backend options. Required when `statePath` is absent. |
| `recovery` | string | `auto` | Pending-transaction policy: `auto`, `retry` or `fail`. |
| `allowBestEffortLock` | boolean | `false` | Explicitly allow filesystem state on an object store, whose lock is best effort. Prefer JDBC, Redis or etcd in production. |
| `enabled` | boolean | `true` | Set `false` to keep the block but run the source as a batch read. |
| `mode` | string | `batch` | Only `batch` is supported. |

`start` and a non-`none` `initial` are mutually exclusive. The first run must define `start` or
`initial=latest|full`; incremental state stores progress after a successful run and is not a
bootstrap cursor. Incremental jobs require `statePath` or `state` and exactly one sink.
Transforms are allowed only when they preserve incremental metadata.

### Initial reads

- `none`: start from `start` or the committed cursor.
- `latest`: read from the latest cursor to the latest cursor. This normally produces an empty
  batch and is useful to initialize the state file.
- `full`: read the current full snapshot as `append` rows; with `cdc` semantics every row is
  marked `I`. Later runs continue from the committed cursor.

### Cursors and transactions

The source returns a `toCursor`; the engine commits it only after every sink has completed
successfully. The filesystem backend stores one journal and lock per source:

```text
<pudding-state>/<sanitized-source-id>-<hash>.state   # cursor + pending transaction + history
<pudding-state>/<sanitized-source-id>-<hash>.lock    # single-writer lock
```

The hash keeps different source ids from colliding after path sanitization. The state contains
only cursors, plan identity, transaction metadata and timestamps — never credentials or raw
source configuration. A legacy `<sanitized-source-id>.cursor` file is read when no state file
exists and is removed after the next commit.

A job follows this sequence:

1. acquire the source lock;
2. read the source and run transforms;
3. persist a pending transaction (from cursor, target cursor, transaction id and plan fingerprint);
4. write the sink;
5. replace the state with the committed cursor.

The pending transaction records a plan fingerprint: source/sink type and id, semantics, key
fields, operation column, ordering fields, SHA-256 hashes of the redacted source connector and
sink configuration, referenced connection and catalog names, state backend and state schema
version. When transforms or connection/catalog references are present, the fingerprint also
covers the transform chain and the resolved non-secret connection/catalog settings. Raw
configuration values and credentials are never stored. Operational incremental settings such as
`recovery`, `initial` and the state selection are not part of the fingerprint. Recovery refuses
to run if the current plan no longer matches, so a changed target, connector or transform chain
can not consume an old pending batch by accident.

Each state also exposes observability fields: `attempts`, `lastTransactionId`,
`lastCommittedAt`, `lastFailureAt` and a redacted `lastFailureReason`. Write failures are recorded
best-effort while the lock is held.

A crash before step 3 leaves no sink write. A crash after step 3 leaves a pending transaction and
a lock, so the job never advances the cursor without a recorded sink attempt. The JDBC, Redis and
etcd backends take over an expired lease when `lockTtlMs` is positive. The filesystem backend has
no atomic compare-and-swap and rejects `lockTtlMs`; after verifying that the old process is gone,
remove its lock with `bin/pudding state unlock <state-target> <source-id>`. The next run then
applies `incremental.recovery`:

- `auto` (default): replay only when the sink declares the selected semantics idempotent
  (Hudi and Paimon upsert/CDC, and Hudi, Paimon and Iceberg append through commit markers);
  otherwise fail with an operator instruction;
- `retry`: always replay; use this only when the sink is known to be idempotent;
- `fail`: never replay automatically; resolve the pending transaction manually.

The journal prevents silent cursor advancement, but it does not make an arbitrary sink
transactional. Append writes to Hudi, Paimon and Iceberg carry a deterministic transaction marker
and a replay of the same transaction is skipped; other append sinks can still duplicate data
after an operator chooses `--abort`. Exactly-once behavior requires an
idempotent/transactional sink.

### State backends

`statePath` selects the built-in filesystem backend. The `state` block selects a pluggable backend:

```yaml
incremental:
  semantics: append
  initial: latest
  state:
    backend: jdbc
    url: jdbc:postgresql://state-db:5432/pudding
    user: pudding
    password: ${secret:env/STATE_DB_PASSWORD}
    driver: org.postgresql.Driver
    table: pudding_incremental_state
```

Backends:

| Backend | Selection | Notes |
| --- | --- | --- |
| `filesystem` | `statePath`, or `state.backend: filesystem` + `statePath` option | HDFS/local atomic rename; object-store fallback is documented below. `lockTtlMs` is rejected because the backend has no atomic compare-and-swap; unlock a crashed writer explicitly. |
| `jdbc` | `state.backend: jdbc` + `url` and optional `user`, `password`, `driver`, `table`, `lockTtlMs` | Strong row updates; the table must be provisioned by the operator. |
| `redis` | `state.backend: redis` + `mode` (`single`, `sentinel`, `cluster`), `host`/`port`, or `masterName` + `sentinels`, or `clusterNodes`; optional `user`, `password`, `database`, `ssl`, `timeoutMs`, `prefix`, `lockTtlMs` | Provider jar `redis-state-provider`; Lua scripts compare the lock token and keys share a cluster hash tag. |
| `etcd` | `state.backend: etcd` + optional `endpoint`, `apiPrefix`, `user`, `password`, `prefix`, `timeoutMs`, `lockTtlMs` | Provider jar `etcd-state-provider`; compare-and-swap on the lock value; a positive `lockTtlMs` attaches a native etcd lease. |

The Redis and etcd providers are optional jars in `libs`. The client jar and `pudding submit`
load every jar in `libs`; `bin/pudding state` adds the `*state-provider-*.jar` files
automatically. Backend options support `${secret:...}` references, resolved on the driver before
the connection is opened.

JDBC table DDL (adjust types for your database if needed):

```sql
CREATE TABLE pudding_incremental_state (
  source_id VARCHAR(255) PRIMARY KEY,
  state_json TEXT NOT NULL,
  lock_owner VARCHAR(255),
  lock_host VARCHAR(255),
  lock_pid VARCHAR(255),
  lock_token VARCHAR(64),
  lock_acquired_at VARCHAR(64),
  lock_fencing_token BIGINT,
  lock_expires_at_ms BIGINT
);
```

The JDBC backend opens one short connection per operation; use the driver's pool settings or a
proxy when connection establishment is expensive. It does not create tables during a normal job
start. A state write is `UPDATE ... WHERE source_id = ? AND lock_token = ?`, so a writer whose
lease was taken over can not overwrite the new state. Existing tables created before TTL support
must add the `lock_fencing_token` and `lock_expires_at_ms` columns shown above, or run:

```shell
bin/pudding state migrate @/path/to/jdbc-state.properties
```

`migrateSchema=true` also creates or upgrades the table when a backend is opened. Run either
form while no incremental writer uses the table; it issues DDL and does not take the source lock.

Redis modes:

```properties
backend=redis
mode=sentinel
masterName=mymaster
sentinels=redis-1:26379,redis-2:26379
lockTtlMs=600000
```

```properties
backend=redis
mode=cluster
clusterNodes=redis-1:6379,redis-2:6379,redis-3:6379
lockTtlMs=600000
```

Redis keys use a source hash tag (`{h<hash>}:<source>:state` and `:lock`), so both keys of a source
land in the same cluster slot. The state-format change is internal; use `pudding state` to
inspect it. `ssl` is supported in all three modes. Sentinel master authentication uses
`user`/`password`; use `sentinelUser`/`sentinelPassword` when Sentinel itself requires a different
ACL account (both fall back to the data credentials). Sentinel and cluster replication is asynchronous, so a failover can lose the newest
state write; enable Redis persistence/durability appropriate to the recovery point objective, or
use JDBC/etcd when the journal and lock must be strongly consistent.

etcd with a positive `lockTtlMs` grants a native lease, attaches it to the lock key and replaces
the lease on every renewal. If the driver dies, the etcd server removes the key when the lease
expires; no keepalive stream is required. The lock document also stores the lease id so a
takeover can revoke a stale lease. The backend uses the etcd v3 HTTP/JSON gateway (enabled by
default in etcd 3.4+); set `apiPrefix=/v3beta` for an older gateway, or use JDBC/Redis when the
gateway is disabled.

For a filesystem state directory on S3, the distribution includes
`libs/hadoop-aws-runtime-<version>.jar`. It bundles `hadoop-aws` 3.3.4 and the AWS SDK modules
needed by S3A (S3, DynamoDB, STS and dependencies). OSS, Azure ABFS/WASB, GCS and COS packs are
also available and selected from the state URI scheme. Remove a pack from `libs` when the Spark
image already provides a compatible client. Configure `fs.*` through `spark.hadoop.*` job
settings or the cluster's `core-site.xml`; object stores provide a best-effort lock only.

For the filesystem backend, state replacement uses an overwrite rename where supported (HDFS and
local). Only recognized object-store schemes (`s3a`, `s3`, `oss`, `abfs`, `abfss`, `wasb`, `wasbs`
and `gs`) fall back to delete + rename; a failure on HDFS/local propagates without deleting the
previous committed state. The object-store fallback is not crash-atomic, and object stores do not
support exclusive create, so filesystem locking is best effort there; use JDBC, Redis or etcd when
strict single-writer fencing is required. A job that points `statePath` or a filesystem state
backend at an object-store scheme is rejected unless
`incremental.allowBestEffortLock=true` (or the same option under `state`) is set explicitly.

### Lock leases and fencing

The JDBC, Redis and etcd backends support leases. Set `lockTtlMs` to a positive number of
milliseconds to turn the source lock into a lease:

- the running driver renews the lease in the background and before every state write;
- an expired lease can be taken over by a new run without a manual unlock;
- each acquisition increments a fencing token; state writes are rejected when the writer's token
  was replaced, with a `lock ... was lost` error;
- the last fencing token is kept in the state journal even when the cursor is cleared, so it
  keeps increasing across normal lock releases and resets.

A TTL of `0` or an absent `lockTtlMs` keeps a non-expiring lock; remove it with
`bin/pudding state unlock` after a crash. A TTL should be several times longer than a normal
state-write interval; the background renewal runs at one third of the TTL. The filesystem backend
(including HDFS and S3A state paths) always uses a non-expiring lock and rejects a positive
`lockTtlMs`, because it has no atomic compare-and-swap for takeover. The state tool `status`/`show`
reports `expiresAt` and `fencingToken` for the current lock.

### State tool

The tool works without starting a Spark job. A state target is a filesystem path or a
properties file prefixed with `@`. A properties target selects `backend` and backend options:

```properties
backend=jdbc
url=jdbc:postgresql://state-db:5432/pudding
user=pudding
password=${secret:env/STATE_DB_PASSWORD}
table=pudding_incremental_state
lockTtlMs=600000
```

```properties
backend=redis
host=redis.internal
port=6379
password=${secret:env/REDIS_PASSWORD}
prefix=pudding:incremental:
lockTtlMs=600000
```

```shell
# show every source, or one source (human readable)
bin/pudding state show <state-target> [source-id]

# machine-readable JSON for the backend
bin/pudding state status <state-target> [source-id] --json

# drop a pending transaction and replay it on the next run
bin/pudding state resolve <state-target> <source-id> --abort

# the sink write succeeded; advance the cursor without writing it again
bin/pudding state resolve <state-target> <source-id> --commit

# set a cursor explicitly, or remove all state for a re-bootstrap
bin/pudding state reset <state-target> <source-id> --cursor <cursor>
bin/pudding state reset <state-target> <source-id> --clear

# --clear writes an empty journal, keeps the fencing generation and releases the lock

# remove a lock left by a failed process after verifying no writer is alive
bin/pudding state unlock <state-target> <source-id>

# create or upgrade a JDBC state table
bin/pudding state migrate <jdbc-state-target>
```

`status --json` returns source, location, committed cursor, pending transaction and plan
fingerprint, attempts, last transaction/commit/failure fields, the last fencing token and the lock
owner. Without a source id it returns an array of all sources. A record has this shape (values
abbreviated):

```json
{
  "sourceId": "hudi_orders",
  "location": "hdfs:///pudding/state",
  "committedCursor": "20260101000000",
  "pending": null,
  "attempts": 0,
  "lastTransactionId": "a1b2...",
  "lastCommittedAt": "2026-10-29T...",
  "lastFailureAt": null,
  "lastFailureReason": null,
  "updatedAt": "2026-10-29T...",
  "fencingToken": 4,
  "lock": null
}
```

`bin/pudding state` requires `SPARK_HOME` for Hadoop classes unless `hadoop` is on `PATH`.
Set `PUDDING_STATE_CLASSPATH` to a JDBC driver jar when the JDBC backend is not already on the
Spark classpath. Redis and etcd provider jars are loaded from `libs` automatically. A properties
target may contain `${secret:...}` references; they are resolved by the tool. Keep a properties
file containing a plaintext credential at mode `0600`.

### Metrics and audit

Incremental jobs publish counters through JMX at
`org.pudding.core.incremental:type=IncrementalMetrics`. The `Snapshot` attribute is a JSON map;
the `getCounter(name)` operation reads one counter. Counters include:

- `incremental.lock.acquired`, `renewed`, `takeover`, `lost`, `released`, `forced`
- `incremental.transaction.begin`, `commit`, `abort`, `reset`, `attempt`, `replay`, `failure`
- `incremental.state.write.failure`
- `incremental.run.started`, `completed`, `failed`
- `incremental.sink.fencing.rejected`

The driver logs single-line audit events such as
`event=incremental.lock.takeover source=... fencingToken=...` and
`event=incremental.sink.fencing.rejected sink=...`. Audit events never include the private lock
token or configuration values. The completion log also contains the counter snapshot.

### Container and CI tests

The repository includes opt-in container tests. Set `PUDDING_TESTCONTAINERS=true` to start Redis,
etcd, HDFS and MinIO with Testcontainers. Set `PUDDING_TESTCONTAINERS_REQUIRED=true` to fail
instead of skipping when a container can not start; the GitHub Actions workflow sets both.
`PUDDING_TESTCONTAINERS_SKIP_S3=true` runs a local HDFS-only iteration without pulling MinIO.
`PUDDING_TEST_REDIS_SENTINEL_ENDPOINTS` + `PUDDING_TEST_REDIS_SENTINEL_MASTER` and
`PUDDING_TEST_REDIS_CLUSTER_NODES` point the Sentinel/Cluster contract tests at an existing
deployment.

## Source support

| Source | `append` | `upsert` | `cdc` |
| --- | --- | --- | --- |
| `hudi-source` | — | latest state of changed keys | Hudi CDC relation |
| `paimon-source` | append-only tables | — | primary-key table changelog |
| `iceberg-source` | append-only snapshot range | latest complete row per key | changelog metadata table |

### Hudi

Hudi incremental reads accept `tablePath`, or resolve the base path of a Hudi catalog table.
`upsert` uses Hudi's incremental latest-state query. `cdc` additionally requires these table
properties:

```properties
hoodie.table.cdc.enabled=true
hoodie.table.cdc.supplemental.logging.mode=DATA_BEFORE_AFTER
```

The source parses Hudi's `before`/`after` images into ordinary rows, adds `__op`, and exposes the
commit timestamp as source ordering. Hudi emits operations `i`, `u`, `d`; they become `I`, `U`,
`D`. CDC-log images are Avro JSON. Base-file and replace-commit images (including
`bulk_insert`/insert-only commits) use Hudi's InternalRow JSON, which stores DECIMAL only as a
lossy zero/non-zero marker. The source fails closed on that DECIMAL value rather than writing a
wrong number. Binary values are decoded from base64 in that encoding; a table with
`hoodie.populate.meta.fields=false` rejects binary CDC because the two encodings are ambiguous.

### Paimon

`append` requires an append-only table. `cdc` requires a primary-key table and the configured
`keyFields` must match the table primary keys. With a changelog producer the source scans
changelog files; without one it uses `incremental-between-scan-mode=diff`. Paimon's audit log is
used to expose row kinds, so updates can appear as `I` (an upsert for the same key) rather than
`U`. Both are safe for an upsert-capable sink.

Paimon's audit log does not expose source ordering. If a range contains several changes for the
same key, use smaller ranges to keep the final state deterministic.

### Iceberg

`append` reads snapshots from `start` (exclusive) to `end` (inclusive) with Iceberg's incremental
append scan. The snapshot range must contain only append snapshots; an overwrite, delete or
replace snapshot makes the source fail and ask for `cdc` semantics.

`cdc` reads the `table.changes` metadata table and requires a catalog table. The source maps the
commit snapshot to its monotonic snapshot sequence number and combines it with
`_change_ordinal` and an insert-over-delete priority as the ordering contract, then normalizes
`_change_type` to `__op`. Iceberg can encode an update as a delete/insert pair; CDC sinks
deduplicate by key and keep the last row. Path-mode `.changes` reads are not available, so use a
catalog for Iceberg CDC.

`upsert` also reads the catalog changelog. It removes `UPDATE_BEFORE` rows, prefers the
replacement row when Iceberg encodes an update as a delete/insert pair with tied ordering, and
returns the latest complete row per key. If the last change for a key is a delete, the read fails
and asks for `cdc` semantics rather than silently dropping the delete.

## Sink support

| Sink | `append` | `upsert` | `cdc` |
| --- | --- | --- | --- |
| `hudi-sink` | insert | Hudi upsert | delete + upsert |
| `paimon-sink` | append | Paimon merge | row-kind write |
| `iceberg-sink` | append | Iceberg `MERGE` | delete + `MERGE` |
| other sinks | append | rejected | rejected |

Hudi, Iceberg and Paimon CDC writes deduplicate the batch by key when the source provides
ordering. The order columns are internal metadata and are removed before writing. When a batch
source does not expose ordering metadata, Hudi, Paimon, Iceberg and JDBC accept `cdcOrderFields`
on the sink. Paimon CDC and upsert require the target table to exist and its primary keys to
match `keyFields`; create the table before the first incremental run. Iceberg upsert/CDC create a
missing target from the first batch. Hudi and Iceberg also accept full snapshots on the initial
run. Incremental writes to these lakehouse sinks are batch only; a sink configured with
`isStreaming: true` rejects an incremental write.

Append writes carry the deterministic transaction id in the lakehouse commit metadata:

- Hudi stores `_pudding.transaction.id` in `HoodieCommitMetadata`. A custom
  `hoodie.datasource.write.commitmeta.key.prefix` other than `_` is rejected because it would hide
  the marker.
- Paimon prefixes the commit user with `<transaction-id>_` through `commit.user-prefix`.
- Iceberg stores `pudding.transaction.id` in the snapshot summary; a transaction append to a
  missing table creates the empty table first.

They also store the source id and fence generation:

- Hudi: `_pudding.source.id`, `_pudding.fencing.token`.
- Paimon: the commit user becomes `<transaction>-f<fence>-s<source-hash>_<uuid>`.
- Iceberg: `pudding.source.id`, `pudding.fencing.token` in append snapshots. MERGE paths also
  store `pudding.fencing.<source-id>` and `pudding.fencing.transaction.<source-id>` as table
  properties because Spark SQL MERGE can not set a snapshot summary.

Before writing, each sink scans its timeline/snapshot metadata and skips a transaction id that is
already present. It also rejects a writer whose fence is not newer than the newest fence recorded
for the same source; an equal fence is accepted only when the same transaction marker is already
committed, so a replay remains a no-op while a different writer that reused the generation is
rejected. For an Iceberg MERGE, an equal fence is accepted only when the stored transaction id
matches. This narrows the window in which a writer whose lease expired can still commit.
Snapshot/commit metadata retention can eventually remove the markers; the sinks log a warning when
a resumed source finds no retained fence, so keep the lakehouse retention window (Hudi commit
retention, Paimon snapshot retention, Iceberg snapshot expiration) longer than the maximum delayed
writer lifetime. The sink capability is exposed as
`syncIncrementalFencing` in `capabilities.yaml`. Fencing is not an atomic uniqueness constraint on the
lakehouse commit itself, so it does not replace a transactional sink or an idempotent marker when
two writers race past the check at the same time.

## Transforms

Incremental pipelines allow only transforms that declare the
`preservesSyncIncrementalMetadata` capability (`preservesIncrementalMetadata` on the transform
SPI):

- `filter-transform`
- `column-transform`
- `union-transform`

Aggregations, joins, limits, deduplication and SQL transforms are rejected. Deduplication is not
metadata-preserving: keeping one arbitrary row per key can discard the operation and ordering that
must reach the sink, including a delete. A column transform must keep the key fields, the operation
column and any source ordering columns; the engine validates the transform output and fails before
the sink if metadata is missing.

Incremental sync accepts metadata-preserving transforms; a transform may not change keys, the
operation column or ordering metadata. A simple lake-to-lake sync has the smallest topology and
one writer for its cursor.

## Streaming Kafka ingestion is a separate mechanism

Continuous Kafka ingestion uses `job.mode: streaming`, not `sync`/`incremental`. Kafka offsets
are stored in each sink's Spark checkpoint; the incremental state journal, pending transaction,
fencing and state tool are not used.

- `kafka-source` supports `valueFormat: json`, a generic operation column and a Debezium
  `before`/`after` envelope.
- Streaming jobs accept exactly one sink. Hudi uses its native streaming writer with batch
  delete/upsert `foreachBatch` for CDC; Paimon uses `_row_kind_`; Iceberg uses native append or
  `foreachBatch` + `MERGE INTO`.
- Hudi append commits an empty microbatch to the checkpoint but skips the Hudi write. Hudi updates
  its metadata table on every commit and rejects a zero-record commit, and empty microbatches are
  normal while a CDC snapshot starts or a Kafka backlog drains.
- Hudi and Paimon streaming accept catalog tables with a resolvable storage location. Paimon
  1.3.2 has no catalog `toTable` streaming sink, so append uses the resolved path and update/CDC
  use the direct primary-key writer. Iceberg streaming requires an Iceberg catalog and an
  existing table. All three formats require the target table to exist before the query starts.
- Hudi, Iceberg and Paimon accept `outputMode: append` or `update`. `update` requires record
  keys and merges changed stateful-aggregation rows; CDC remains `append` because the operation
  column carries deletes. The `continuous` trigger is rejected by these lakehouse sinks.
## JDBC sources

`jdbc-source` and `mysql-source` support `append` and `upsert` in `sync` incremental mode. They
require a top-level `cursorField` and read:

```text
cursorField > start AND cursorField <= end
```

`start` comes from incremental state, `incremental.start`, or `initial=latest`; `end` defaults to
the current maximum. The cursor column metadata determines numeric, date, time, timestamp or
string quoting. `upsert` keeps the latest complete row per key in the range. A table snapshot
does not expose deletes; use `mysql-cdc-source` or another CDC source when deletes matter.

`jdbc-sink` and `mysql-sink` apply `append`, `upsert` and `cdc` in sync incremental mode. CDC
keeps the latest operation per key, then applies deletes and upserts in one database transaction.
The target must have a primary key or unique index matching the incremental key fields. The same
`cdcOpColumn` contract is available for streaming writes: configure `recordKeyFields`,
`checkpointLocation` and `outputMode: append`; each micro-batch applies deletes and upserts in one
database transaction.

- CDC is at-least-once with idempotent key-based writes. Kafka producer duplicates remain; use
  `cdcOrderFields` or the emitted Kafka/Debezium ordering metadata to resolve multiple changes
  for one key in a micro-batch.
- Exactly-once delivery is checkpoint-bound and requires a single sink; all job modes allow one
  sink, and there is no global transaction across multiple sinks.
- Checkpoint retention defaults to `keep`. `delete-on-success` is opt-in, requires `once` or
  `available-now`, and removes the checkpoint only after the query produces completion evidence
  that it drained its finite input. An external stop before that evidence keeps the checkpoint.
  Use `bin/pudding checkpoint` for operational inspection and age-based cleanup. See
  [streaming-checkpoints.md](streaming-checkpoints.md).

See the examples under `build/examples/stream_kafka_to_*.yaml`.

## Limitations

- One incremental source per pipeline, and no mixing of incremental and batch sources.
- The engine does not deduplicate across successful runs. A retry after a partial sink write can
  duplicate data unless the sink is idempotent (upsert/CDC, or append with a transaction marker)
  or the write is otherwise transactional. Hudi, Paimon, Iceberg, JDBC and MySQL implement
  idempotent upsert/CDC paths; append-only paths do not.
- JDBC incremental reads use one table and a single cursor column. Composite cursors, delete
  detection and resumable snapshots are not provided.
- The Paimon CDC sink uses Paimon's direct writer instead of `DataFrameWriter` because Spark's
  Paimon writer does not accept row-kind input. Table options configured on the sink are not
  applied to this write path; they belong on the target table.
- Hudi incremental reads accept a catalog table and resolve its storage location to the
  path-based reader. Hudi table options that only exist in path mode still require `tablePath`.
