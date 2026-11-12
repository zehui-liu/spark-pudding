# Native database CDC

`mysql-cdc-source`, `postgres-cdc-source` and `mongodb-cdc-source` read database changes directly
from the database and expose them as a Structured Streaming DataFrame. They do not require Kafka,
Kafka Connect, Debezium Server or a schema registry.

The source names and connections follow the normal Pudding model:

```yaml
job:
  mode: streaming
  appName: orders-cdc
  master: local[*]
  deployMode: client

source:
  - type: mysql-cdc-source
    id: orders
    connection: mysql
    config:
      database: inventory
      table: orders
      schema: "id bigint, name string, updated_at timestamp"
      snapshotMode: initial

sink:
  - type: paimon-sink
    id: orders_paimon
    dependency: orders
    config:
      table: orders
      path: "s3a://warehouse/paimon/default.db/orders"
      primaryKey: [id]
      outputMode: append
      checkpointLocation: "s3a://warehouse/checkpoints/orders"
      cdcOpColumn: __op
```

See the [source README](../../source/database-cdc-source/README.md) for the complete option
table.

## How it works

The Spark V2 provider `pudding-cdc` has a driver-side poller and a bounded hand-off buffer:

1. Spark asks for the next micro-batch end offset.
2. The poller reads snapshot/change records from Debezium or the MongoDB change stream and
   converts them to a canonical JSON row.
3. The V2 reader ships one ordered slice of JSON rows to one Spark partition.
4. The Pudding adapter applies the declared schema with `from_json` and adds `__op`,
   `_cdc_timestamp`, `_cdc_sequence` and `_cdc_table`. The sequence is a monotonic
   tie-breaker so same-key changes that share a database timestamp stay ordered inside a
   micro-batch.

Only the provider-neutral Spark classes and a string column are needed on executors; connector
classes run on the driver. A failed batch is retained in the driver buffer until Spark commits
it, so a retry can re-plan the same start/end range. The buffer is capped by
`queueCapacity` and the connector is blocked when it is full. The effective capacity is always at
least `maxRecordsPerBatch + 1`: Spark commits the previous source batch only while constructing a
batch that has new data, so a queue that can fill to exactly one batch would deadlock the planner
and the poller.

## Checkpoint and delivery semantics

The Spark checkpoint contains only the source cursor:

- MySQL: binlog file, position and snapshot markers;
- PostgreSQL: LSN, transaction id and slot position;
- MongoDB: `{mode: snapshot, startAt, lastId}` or `{mode: stream, resumeToken}`.

Configuration, connection strings and credentials are not stored in the checkpoint. The source
schema is part of the DataFrame schema and therefore part of the checkpoint contract. Changing
the schema requires a new checkpoint or a controlled migration.

Delivery is at-least-once across driver failure: the last uncommitted micro-batch can be
replayed. CDC and upsert sinks are idempotent for the same keys, while append-only sinks are not.
An interrupted MySQL/PostgreSQL initial snapshot is re-run by Debezium on restart; rows already
written by the previous run are replayed. This is intentional: replay beats silently skipping
part of a snapshot.

## Snapshot and change ordering

- MySQL/PostgreSQL initial snapshots emit `I` rows with `_cdc_timestamp` from the connector.
  The connector switches to binlog/WAL after the snapshot.
- MongoDB's snapshot is read in `_id` order. A change stream is opened at the snapshot's
  operation time before the scan starts, so changes during the snapshot are not lost. Snapshot
  rows use timestamp `0`, and update/insert/delete rows use cluster time; a later change wins in
  a key-based sink.
- MongoDB updates use `updateLookup`, so the full document is emitted. Deletes contain only
  `_id`; other schema columns are null on `D` rows. The schema must contain `_id`.
- MongoDB requires a replica set or sharded cluster. Standalone servers cannot open change
  streams.

## MySQL requirements

The server must enable row-based binary logging and full row images:

```text
--log-bin=mysql-bin
--binlog-format=ROW
--binlog-row-image=FULL
--server-id=<unique-server-id>
```

The Debezium user needs `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE` and
`REPLICATION CLIENT` (or equivalent). The schema history is stored under the checkpoint by
default; see [streaming-checkpoints.md](streaming-checkpoints.md).

`database.server.id` must be unique among MySQL clients. The source derives a stable value from
the captured table when `serverId` is omitted; set it explicitly when multiple jobs capture
tables from the same server or when the deployment requires a managed server id.

## PostgreSQL requirements

Set `wal_level=logical`, allow enough WAL senders and replication slots, and grant the connector
role the ability to create a publication and a replication slot. The default plugin is
`pgoutput` (PostgreSQL 10+).

The source creates a filtered publication and a stable replication slot named from the captured
table. It never drops a slot on stop. A stopped job therefore retains WAL; monitor the slot and
remove it only after the checkpoint is retired. `slot.name` and `publication.name` can be set in
`source.config.options`.

## MongoDB requirements

The deployment must be a replica set or sharded cluster. The source does not create the
collection; create it and its indexes first. The driver URI can include replica-set and
authentication options through a `mongodb` connection or `uri` config.

The initial scan uses `find().sort({_id: 1})` and resumes an interrupted scan after the last
emitted `_id`. Collections whose `_id` values mix incompatible BSON types are not recommended;
use a single `_id` type (normally ObjectId) or a string key. The snapshot must fit within the
oplog retention window because the change stream is opened at the snapshot's operation time
before the scan begins; increase the oplog size or use an external snapshot when the collection
is larger than the retention window.

## Dependencies and packaging

The client selects `database-cdc-source` plus the shared `cdc-runtime` jar for any of the three
source types. `cdc-runtime` contains Debezium MySQL/PostgreSQL, Kafka Connect API, the JDBC
drivers and the MongoDB Java driver. Jackson and Guava are shaded to avoid replacing Spark's
versions; Debezium and Kafka Connect class names stay unchanged because the Pudding source adapter
implements the Connect `SourceTask` interface directly.

For a direct `spark-submit`, add both jars to `--jars`. The runtime is a support artifact and does
not appear in `capabilities.yaml`.

## Lakehouse regression

Two opt-in integration suites run a real MySQL binlog source through the lakehouse CDC sinks:

```shell
PUDDING_TESTCONTAINERS=true PUDDING_TESTCONTAINERS_REQUIRED=true \
  mvn -pl integration/database-cdc-hudi-paimon-test -am test \
    -DwildcardSuites=org.pudding.integration.DatabaseCdcHudiPaimonTest

PUDDING_TESTCONTAINERS=true PUDDING_TESTCONTAINERS_REQUIRED=true \
  mvn -pl integration/database-cdc-iceberg-test -am test \
    -DwildcardSuites=org.pudding.integration.DatabaseCdcIcebergTest
```

Each case covers the initial snapshot, insert/update/delete, stopping and restarting from the
same checkpoint, and the final target rows. Hudi/Paimon are tested together; Iceberg is in a
separate Maven module because the Paimon Spark artifact bundles compatibility classes that
conflict with Iceberg 1.5.0's `MergeRows` on one classpath.

## Limits

- The source is streaming-only and unbounded. Use `job.mode: batch` with JDBC or another bounded
  source for a one-time copy. A finite `once`/`available-now` trigger is rejected because there
  is no bounded end offset.
- One source instance captures one table/collection and emits one Spark partition per
  micro-batch. This preserves change order and simplifies checkpoint offsets; very high
  throughput deployments should use Kafka/Connect ingestion instead.
- Schema evolution is explicit. Add the column to `schema` and restart with a new checkpoint, or
  apply a compatible sink migration first.
- MySQL/PostgreSQL DDL events are not exposed as rows. Schema history records the DDL for
  restart, but downstream transformations see only row changes.
- Debezium incremental snapshots/signals are not driven by this source. The initial snapshot and
  streaming phases are supported.
