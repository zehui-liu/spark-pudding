# Native database CDC sources

This module provides three Structured Streaming sources:

| Type | Engine | Prerequisite |
| --- | --- | --- |
| `mysql-cdc-source` | Debezium MySQL connector (binlog) | `binlog_format=ROW`, `binlog_row_image=FULL` |
| `postgres-cdc-source` | Debezium PostgreSQL connector (`pgoutput`) | `wal_level=logical`, replication/publication privileges |
| `mongodb-cdc-source` | MongoDB Java driver change streams | Replica set or sharded cluster |

The sources are streaming-only. `job.mode: streaming` gives one source and one sink; use an
idempotent upsert or CDC sink because a checkpoint restart may replay the last uncommitted batch
and a MySQL/PostgreSQL snapshot interrupted by a restart is deliberately re-run from the
beginning. The source is unbounded and requires a continuous or processing-time trigger;
`once`/`available-now` are rejected because the source has no bounded end offset that Spark
could use to finish a finite query.

## Row contract

The configured `schema` is the data-column contract. Internally the source adds:

| Column | Meaning |
| --- | --- |
| `__op` (or configured `opColumn`) | `I`, `U`, `D` |
| `_cdc_timestamp` | Change ordering value; `0` for MongoDB snapshot rows |
| `_cdc_sequence` | Monotonic tie-breaker for changes that share a database timestamp |
| `_cdc_table` | Captured table or collection label |

CDC sinks remove `_cdc_*` columns automatically. Use `cdcOrderFields` only when a sink needs a
different ordering column.

The schema is explicit on purpose. Structured Streaming requires the same schema in every
micro-batch, and a checkpoint is tied to the DataFrame schema. A column added or renamed in the
database must be reflected in the job configuration and started with a new checkpoint (or a
compatible migration step).

## Configuration

See the module resources for complete YAML examples. The common keys are:

| Key | Required | Meaning |
| --- | --- | --- |
| `schema` | yes | Spark SQL DDL or a map of column name to Spark SQL type |
| `table` / `collection` | yes | Captured resource |
| `database` | yes for MongoDB; MySQL/PostgreSQL may derive it from the JDBC URL | Database name |
| `databaseSchema` | PostgreSQL only, default `public` | PostgreSQL schema |
| `snapshotMode` | no, default `initial` | Connector-specific snapshot policy; MongoDB accepts `initial` or `latest` |
| `opColumn` | no, default `__op` | Operation column name |
| `maxRecordsPerBatch` | no, default `1000` | Upper bound on rows in one micro-batch |
| `queueCapacity` | no, default `max(10000, maxRecordsPerBatch)` | Driver buffer size before the connector is back-pressured |
| `options` | no | Advanced Debezium properties |

A top-level `mysql`, `jdbc` (PostgreSQL URL) or `mongodb` connection is preferred for
credentials. Inline `host`/`port`/`user`/`password` or `url`/`uri` are also accepted; use
`${secret:env/NAME}` or `${secret:file:/path#key}` so credentials are resolved on the driver.

MySQL offsets are binlog file/position pairs. PostgreSQL offsets are LSNs in the replication
slot. MongoDB offsets are either snapshot `_id` positions or change-stream resume tokens. Only
these cursor values are stored in the Spark checkpoint; credentials and connection settings are
not written to checkpoint state.

On restart the driver reads the latest Spark commit-log entry and starts the poller at that
committed cursor, not at a newer offset-log entry whose batch was never committed. Spark then
replays the uncommitted batch; replay matching uses the synthetic sequence, so a connector cursor
that is re-emitted after resume does not stall the replay.

## Schema history

MySQL needs a schema history across restarts. By default the source stores it as immutable
files under
`<checkpointLocation>/sources/<source-id>/cdc-schema-history/<logical-server-name>` using the
Spark Hadoop FileSystem, so it works on local disks, HDFS and configured object stores
(S3A/OSS/ABFS/GCS/COS). Do not delete that directory separately from the checkpoint. Set
`options.database.history` to a custom Debezium history implementation when a different store is
required.

PostgreSQL recovers schema from its replication slot. MongoDB has no separate schema history.

## Operational notes

- PostgreSQL replication slots retain WAL until consumed. Monitor `pg_replication_slots` and
  remove unused slots explicitly. The source never drops a slot on stop.
- MongoDB delete events contain only `_id`; other columns are null on a `D` row. Pre-images are
  not enabled. Use `_id` (or a sink key that can be derived from it) for deletes.
- The driver-side queue is bounded. A slow sink back-pressures the connector; it never grows an
  unbounded in-memory queue. One Spark partition carries each micro-batch to preserve order.
- The connector runs on the driver, so driver memory should account for
  `queueCapacity * average row size`.
- The schema history lives under the checkpoint. With `checkpointRetention: delete-on-success`
  the next run performs a fresh snapshot, which is the intended disposable-run behavior.
