# spark pudding

English | [简体中文](README.zh-CN.md)

A micro framework for data integration and processing based on Apache Spark.

- Define data pipelines with JSON, YAML or HOCON configuration.
- Read and write multiple data sources through pluggable source, transform and sink modules.
- Run the same job with the local Spark installation, Apache YARN or Kubernetes.
- Submit jobs through the reusable client SDK or the command line client.

## NOTE

This project code is independently developed by individuals. No affiliation with the creator's
company.

## Release notes

The capability timeline and upgrade notes for the unreleased `1.0.0` line are recorded in
[RELEASE_NOTES.en.md](RELEASE_NOTES.en.md) (English) and
[RELEASE_NOTES.md](RELEASE_NOTES.md) (Chinese).

## Building from source

Prerequisites:

- Unix-like system (Linux or macOS)
- JDK 11 or 17 (JDK 11 is the primary CI baseline; JDK 17 is also tested; the build rejects
  JDK 8 and JDK 18+ at the `validate` phase)
- Maven 3.8.7
- Apache Spark 3.4.0, Hadoop 3.x and Scala 2.12 for the runtime

Build and test everything:

```shell
mvn clean verify
```

Tests are run with ScalaTest. `verify` also assembles the distribution and checks that every
connector, provider and runtime module produced a jar in `libs`; the build fails if a component
jar is missing.

The build produces:

```text
build/target/spark-pudding-1.0.0/
build/target/spark-pudding-1.0.0.tar.gz
```

`spark.version` in the root `pom.xml` is the version the project is built and tested against. The
Kubernetes image and the submit host's Spark installation must use the same version. The Iceberg,
Hudi and Paimon dependencies use `iceberg.spark.binary.version`, `hudi.spark.binary.version` and
`paimon.spark.binary.version`; `elasticsearch.version` and `mongodb.spark.version` pin the
Elasticsearch and MongoDB connectors, and the Kafka connector follows `spark.version`. Update
these together when changing the Spark baseline. `pudding k8s build-image` compares the Spark
distribution version with the client jar manifest when both are available and refuses a
mismatch; otherwise it warns and continues. The tested JDK, Spark, Hadoop, lakehouse and
connector versions, the CI matrix, and the external-service test setup are documented in
[build/doc/compatibility.md](build/doc/compatibility.md).

## Submission SDK and execution targets

The client module exposes a reusable submission SDK with `local`, `yarn` and `kubernetes`
targets. The CLI infers the target from the job master or accepts `--target`. For a REST backend,
`SubmissionManager` persists submission records, exposes recent logs, reconciles state and
reattaches Kubernetes driver Pods after a client restart. See
[client/README.md](client/README.md) for the API, CLI options, resource layout and deployment
rules. Image build, kind setup and Kubernetes submission commands are documented in
[build/k8s/README.md](build/k8s/README.md).

## Job modes

The required `job.mode` selects the user goal; the execution engine is derived from it:

| Mode | Use case | Topology | Execution |
| --- | --- | --- | --- |
| `batch` | General bounded processing | one or more sources and transforms, and one sink | batch |
| `sync` | One-source/one-sink synchronization | one source and one sink; transforms allowed | batch |
| `streaming` | Continuous processing | one source and one sink | structured streaming |

Every mode allows exactly one sink. A run has one commit boundary, and the engine has no
distributed transaction across multiple sinks.

`sync` requires a strategy:

```yaml
job:
  mode: sync
  strategy: full        # or incremental
```

- `sync` + `full` refreshes the target and injects overwrite mode; it requires an atomic
  replacement sink, or an explicit `config.allowNonAtomicReplace: true` opt-in.
- `sync` + `incremental` uses the cursor journal and requires incremental state; transforms must
  preserve key, operation and ordering metadata.
- `batch` is for arbitrary bounded processing and does not use cursor state.
- `streaming` uses Structured Streaming and the single sink's checkpoint.

`job.strategy` is valid only for sync. `job.trigger` is valid only for streaming. `job.execution`
and the old `full`, `incremental` and `etl` modes are rejected; old job files are not migrated and
must be rewritten for the current model.

Scheduling is external. spark-pudding has no internal scheduler: run batch and sync jobs from
Airflow, cron, a Kubernetes CronJob or another orchestrator, and use the same mechanism to keep
streaming jobs alive. See [build/doc/job-modes.md](build/doc/job-modes.md) for the complete
matrix, validation rules and trigger syntax.

## Quick start

1. Unpack the distribution and set the environment:

```shell
mkdir -p spark-pudding-1.0.0
tar -xzf spark-pudding-1.0.0.tar.gz -C spark-pudding-1.0.0
export PUDDING_HOME="$PWD/spark-pudding-1.0.0"
export JAVA_HOME=/path/to/jdk-11
# JDK 17 is also supported: export JAVA_HOME=/path/to/jdk-17
export PATH="$JAVA_HOME/bin:$PATH"
```

2. Configure `conf/env.properties`. It is a properties file whose entries are exported to the
   Spark submission process. For YARN include the cluster configuration directories:

```properties
HADOOP_CONF_DIR=/path/to/hadoop/etc/hadoop
YARN_CONF_DIR=/path/to/hadoop/etc/hadoop
```

For a local job the file can be empty when `JAVA_HOME` and `SPARK_HOME` are already exported;
otherwise set them here. The client and drivers require JDK 11 or 17.

3. Adjust `examples/read_csv_example.yaml` for the environment. At minimum set
   `job.sparkHome`, `job.sparkPuddingHome`, `job.master` and the source path. A local job uses:

```yaml
job:
  mode: batch
  sparkHome: '/path/to/spark-3.4.0-bin-hadoop3'
  sparkPuddingHome: '/path/to/spark-pudding-1.0.0'
  master: local[*]
  deployMode: client
source:
  - type: file-common-source
    config:
      path: '/path/to/spark-pudding-1.0.0/examples/data/demo.csv'
      fileType: csv
```

4. Submit the job. With a log file `pudding submit` returns after starting the job in the
   background; without it the client stays attached and returns the job exit status:

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml" \
  "$PUDDING_HOME/log/my-job.log"
```

5. Follow the job:

```shell
tail -f "$PUDDING_HOME/log/my-job.log"
```

Several examples use `${SPARK_HOME}` and `${PUDDING_HOME}` placeholders. The engine does not
expand environment variables in job configuration; substitute the values before submission (for
example with `envsubst`) or edit the example. `read_csv_example.yaml` already uses explicit paths.
See [build/examples/README.md](build/examples/README.md) for the full example index.

`pudding submit` derives the execution target from `job.master`. Use
`bin/pudding k8s submit` for Kubernetes jobs that need an image, namespace or service account, and
the client CLI directly when target options need to be overridden.

## Unified CLI

The distribution exposes all operational entry points through `bin/pudding`. Arguments are
passed through unchanged. The implementation scripts live under `bin/lib/` and are internal.

| Command | Purpose |
| --- | --- |
| `bin/pudding submit <env.properties> <job-file> [log-file]` | Submit a job through the bundled client |
| `bin/pudding checkpoint <show\|status\|delete\|cleanup> ...` | Inspect or remove a streaming checkpoint |
| `bin/pudding state <show\|status\|list\|reset\|resolve\|unlock\|migrate> ...` | Inspect or repair incremental state |
| `bin/pudding capabilities <verify\|show\|diff> ...` | Verify or compare the capability manifest |
| `bin/pudding maintenance <iceberg\|paimon\|hudi> <action> ...` | Run a lakehouse table-maintenance action |
| `bin/pudding maintenance-server ...` | Start the HTTP maintenance control plane |
| `bin/pudding k8s submit ...` | Submit a job to Kubernetes |
| `bin/pudding k8s build-image ...` | Build the Spark Kubernetes image |
| `bin/pudding k8s kind-setup ...` | Create the local kind cluster |
| `bin/pudding verify <distribution-dir>` | Verify an assembled distribution from the source tree |

For example:

```shell
"$PUDDING_HOME/bin/pudding" checkpoint status s3a://bucket/checkpoints/orders --json
"$PUDDING_HOME/bin/pudding" state show hdfs:///pudding/state/orders
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg status \
  --catalog lake --table db.orders --conf-file /etc/pudding/iceberg.properties
```

`bin/pudding help` lists the commands. The unified entry point is the only supported public CLI;
the implementation scripts under `bin/lib/` are internal and should not be called directly.
Every command, including all options and runnable examples, is documented in
[build/doc/cli.md](build/doc/cli.md).

Spark configuration is passed both to the submission process and to the driver session. The
effective precedence and the resolved session flow are documented in
[build/doc/spark-conf.md](build/doc/spark-conf.md).

## Connections and catalogs

A pipeline can define connections and catalogs once and reference them from sources and sinks:

- a **connection** describes how to reach a system: endpoint, credentials and native client
  properties. It applies to table systems and to Kafka, Elasticsearch, MongoDB and future
  connectors.
- a **catalog** describes table metadata: Hive, Iceberg, Hudi, Paimon and JDBC catalogs are
  installed into the Spark session before it starts. Gravitino is supported as a metadata-only
  provider: its REST API is read to resolve the underlying catalog, and the native Spark catalog
  is used. The Gravitino Spark connector and Java client are not required.

Built-in provider implementations live outside `core` in `provider/connection-providers` and
`provider/catalog-providers`. They are discovered by `ServiceLoader`, so new providers can be
added without changing core or the job configuration format. HOCON nested maps under
`job.config`, `options`, `connection.properties` and `catalog.properties` are flattened to
dotted keys, so `security { protocol = "SASL_SSL" }` is equivalent to
`security.protocol = "SASL_SSL"`. The client adds both provider jars
to a submission automatically. A direct `spark-submit` invocation must add the provider jars
itself when the job uses a connection or catalog; see
[provider/README.md](provider/README.md).

Hudi, Paimon, Iceberg, Elasticsearch, MongoDB, Kafka and object-store (S3A/OSS/Azure/GCS/COS) runtimes are packaged
under `runtime/` and shared by their source and sink implementations or state paths. The client
selects one runtime jar per connector and de-duplicates jobs that use both the source and sink.
See [runtime/README.md](runtime/README.md). A direct `spark-submit` invocation must add the
matching runtime jar as well.

Shared connector support code lives outside core under `connector/`. The JDBC and MySQL source
and sink connectors use `connector/jdbc-common` for JDBC option parsing; the client adds that jar
automatically when one of those connectors is selected. A direct `spark-submit` invocation must
add `libs/jdbc-common-<version>.jar` as well. See [connector/README.md](connector/README.md).

```yaml
connection:
  - name: kafka-prod
    type: kafka
    endpoint: broker-1:9092,broker-2:9092
    auth:
      mechanism: SCRAM-SHA-512
      username: pudding
      password: ${secret:env/KAFKA_PASSWORD}
    properties:
      security.protocol: SASL_SSL

  - name: hms-prod
    type: hive-metastore
    endpoint: thrift://hms-1:9083,thrift://hms-2:9083
    properties:
      metastore.sasl.enabled: 'true'

  - name: pg-meta
    type: jdbc
    endpoint: jdbc:postgresql://meta-db:5432/lake
    auth:
      username: pudding
      password: ${secret:file:/etc/pudding/secrets/lake-db#password}
    properties:
      driver: org.postgresql.Driver

  - name: mongo-prod
    type: mongodb
    endpoint: mongodb://mongo-1:27017,mongo-2:27017
    auth:
      username: pudding
      password: ${secret:env/MONGO_PASSWORD}
    properties:
      authSource: admin
      replicaSet: rs0

  - name: gravitino-prod
    type: gravitino
    endpoint: http://gravitino:8090
    auth:
      token: ${secret:env/GRAVITINO_TOKEN}

catalog:
  - name: hive_prod
    type: hive
    connection: hms-prod
    properties:
      warehouse: hdfs://nn/warehouse

  - name: lake
    type: gravitino
    connection: gravitino-prod
    properties:
      metalake: prod
      catalog: lakehouse

  - name: pg_catalog
    type: jdbc
    connection: pg-meta

source:
  - id: events
    type: kafka-source
    connection: kafka-prod
    config:
      subscribe: order-events
      options:
        startingOffsets: latest

  - id: orders
    type: hive-source
    catalog: hive_prod
    config:
      table: sales.orders

  - id: mongo_orders
    type: mongodb-source
    connection: mongo-prod
    config:
      database: app
      collection: orders
```

Connection and catalog names must be unique within a job. A source or sink can reference at most
one of them. Connection and catalog references are validated before Spark starts, and a reference
on a connector that does not support it is rejected instead of being ignored.

Resolution order:

1. the job parser keeps connection and catalog references as names;
2. `PipelineValidator` checks names, provider types, references and component capabilities;
3. the driver resolves secret references, resolves catalog definitions (including the Gravitino
   REST call) and installs catalog settings into the Spark configuration;
4. each source and sink receives its resource config plus the resolved connection and catalog.

Connection settings take precedence over source/sink `options` for endpoint and credentials.
Resource selectors (`table`, `subscribe`, `esIndex`, `database`, `collection`) stay in the
component config.

### Secret references

Credentials in `job.config`, connection, catalog and component configuration can use:

- `${secret:env/NAME}` — an environment variable;
- `${secret:file:/path/to/file#key}` — a key in a properties file;
- `${secret:file:/path/to/file}` — the trimmed contents of a file.

References are resolved on the driver and are never resolved by the client. Do not put plaintext
credentials in a job file; use a reference and keep the file itself at mode `0600`.

### Gravitino

Gravitino is used as a metadata catalog only. The `gravitino` catalog provider calls
`GET /api/metalakes/{metalake}/catalogs/{catalog}` and maps the returned Hive, Iceberg, JDBC, Hudi
or Paimon catalog to a native Spark catalog. Catalog properties are merged over the Gravitino
properties before installation. Kafka messaging catalogs and systems that Gravitino does not model
as table catalogs (Elasticsearch, MongoDB) are still configured through connections.

Supported Gravitino providers are Hive, Iceberg, JDBC, Hudi and Paimon. The REST API requires
Gravitino's vendor media type; the client sends `Accept: application/vnd.gravitino.v1+json` and
supports a bearer token or basic authentication. The Gravitino catalog name is read from the
`catalog` property (`gravitinoCatalog` is also accepted). A source or sink can reference the
Gravitino catalog; its declared vendor type is resolved on the driver and checked against the
native catalog type before use. Verify the exact Gravitino version's catalog properties in a
staging job before rolling it out.

### Hudi, Paimon and Iceberg

Hudi, Paimon and Iceberg are available as catalogs and as source/sink connectors. The bundled
modules are built against Hudi 0.14.0, Paimon 1.3.2 and Iceberg 1.5.0 for Spark 3.4. See
[build/doc/lakehouse-matrix.md](build/doc/lakehouse-matrix.md) for source/sink semantics,
catalog/path behavior, full-refresh atomicity and test coverage.

#### Hudi

Hudi's `HoodieCatalog` is a Spark **session** catalog, so a job can configure only one Hudi
catalog and it cannot be combined with another session catalog (for example a generic Paimon
catalog). The catalog provider also installs `HoodieSparkSessionExtension` and the Kryo
serializer/registrar. A `hive-metastore` connection can supply the metastore URI.

```yaml
connection:
  - name: hms-prod
    type: hive-metastore
    endpoint: thrift://hms-1:9083,thrift://hms-2:9083

catalog:
  - name: hudi_lake
    type: hudi
    connection: hms-prod
    properties:
      warehouse: hdfs://namenode:8020/warehouse

source:
  - type: hudi-source
    id: source-1
    catalog: hudi_lake
    config:
      table: sales.orders
      filterExpr: "dt >= '2026-01-01'"

sink:
  - type: hudi-sink
    id: sink-1
    catalog: hudi_lake
    config:
      table: sales.orders
      operation: upsert
      recordKeyFields: [id]
      partitionBy: [dt]
      preCombineField: ts
    dependency: source-1
```

Set `tablePath` instead of `catalog` to use Hudi's path-based data source. Path mode supports
snapshot, incremental, read-optimized and CDC queries, time travel, schema evolution and arbitrary
Hudi options. Catalog mode uses the Spark table API; advanced read options still require
`tablePath`, while catalog incremental reads and streaming writes resolve the table's base path
and reuse the path reader/writer. The sink supports insert, upsert, bulk insert, delete, insert
overwrite, atomic whole-table replacement, bootstrap, COW/MOR tables, Hive sync and streaming
writes. See
[source/hudi-source/README.md](source/hudi-source/README.md) and
[sink/hudi-sink/README.md](sink/hudi-sink/README.md).

Hudi catalog properties include `warehouse`, the catalog-level `enableHiveSupport` (default `true`), `setSerializer`
(default `true`), `serializer`, `setKryoRegistrator` (default `true`), `kryoRegistrator` and
`metastore.uris`/`hive.metastore.uris`. Other `spark.*` and `hive.*` entries are forwarded to the
session.

The Hudi connector jars bundle Hudi's HFile/metadata-table support and use Spark's Hadoop and
Hive runtime rather than Hudi's older Hadoop 2.x copies. This keeps Spark's classpath authoritative.

#### Paimon

Paimon can be installed as a **named** catalog (`SparkCatalog`) and coexist with Iceberg, JDBC or
a Hudi session catalog, or as a **generic session** catalog
(`SparkGenericCatalog`, via `sessionCatalog: true` or `mode: generic`). A Paimon catalog requires
`warehouse`; its metadata can live on the filesystem, in a Hive metastore or in a JDBC metastore.

```yaml
catalog:
  - name: paimon_lake
    type: paimon
    properties:
      warehouse: hdfs://namenode:8020/paimon/warehouse
      metastore: filesystem

source:
  - type: paimon-source
    id: source-1
    catalog: paimon_lake
    config:
      table: sales.orders
      scanMode: latest-full
      filterExpr: "dt >= '2026-01-01'"

sink:
  - type: paimon-sink
    id: sink-1
    catalog: paimon_lake
    config:
      table: sales.orders
      primaryKey: [id, dt]
      partition: [dt]
      bucket: 4
    dependency: source-1
```

The source supports batch and streaming reads, snapshot/tag/branch/time-travel scans and Paimon
scan options. The sink creates primary-key, partitioned and bucketed tables and supports batch and
streaming writes. Streaming appends to a catalog table resolve its location because Paimon 1.3.2
has no native catalog `toTable` streaming sink; update/CDC use the direct primary-key writer.
Paimon table names are qualified with the catalog's Spark name unless the catalog is a generic
session catalog. See
[source/paimon-source/README.md](source/paimon-source/README.md) and
[sink/paimon-sink/README.md](sink/paimon-sink/README.md).

Both Hudi and Paimon examples use a `sales` database. The database must exist before the first
write; create it with Spark SQL (or the catalog's tooling) as part of the table provisioning step.
Sample configurations are in `examples/read_hudi_catalog_example.yaml`,
`examples/write_hudi_catalog_example.yaml`, `examples/read_paimon_example.yaml`,
`examples/write_paimon_example.yaml` and `examples/read_iceberg_example.yaml`. Replace the
`${SPARK_HOME}` and `${PUDDING_HOME}` placeholders before submitting them.

A Paimon catalog requires `warehouse`. Other properties include `metastore` (`filesystem`, `hive`
or `jdbc`), `uri`, `jdbc.user`, `jdbc.password`, `token`, `sparkCatalogName`/`catalogName`,
`sessionCatalog`/`mode` and `table-default.*` defaults. A `hive-metastore`, `jdbc` or `mysql`
connection supplies the metastore URI and credentials automatically.

#### Iceberg

Iceberg is installed as a **named** Spark catalog by default, or as a Hive session catalog when
its Spark name is `spark_catalog`. `iceberg-source` reads by table or path and supports batch
snapshot/timestamp/branch/tag selectors, append/upsert/CDC incremental reads and Structured
Streaming snapshot reads; Iceberg's `.changes` requires a catalog table. `iceberg-sink` writes
through the same catalog and supports append/upsert/CDC for both streaming and sync incremental
jobs. See [source/iceberg-source/README.md](source/iceberg-source/README.md) for the source
configuration.
The `iceberg-runtime` jar is selected by the client and must be added manually for direct
`spark-submit` jobs.

### Native database CDC

`mysql-cdc-source`, `postgres-cdc-source` and `mongodb-cdc-source` read database changes
directly through Debezium (MySQL binlog, PostgreSQL logical replication) or MongoDB change
streams. They are streaming-only and require an explicit source column schema. Rows contain the
declared columns plus `__op` (`I`/`U`/`D`), `_cdc_timestamp`, `_cdc_sequence` and `_cdc_table`.
Checkpoint offsets contain only the binlog position, LSN or change-stream resume token.

```yaml
source:
  - type: mysql-cdc-source
    id: orders
    connection: mysql
    config:
      database: inventory
      table: orders
      schema: "id bigint, name string"
      snapshotMode: initial
```

MySQL requires row-based binlog with full row images; PostgreSQL requires `wal_level=logical`;
MongoDB requires a replica set or sharded cluster. MySQL schema history is stored under the
checkpoint by default through Hadoop FileSystem (local, HDFS or object store). Use an upsert or
CDC sink because a restart may replay the last uncommitted batch and an interrupted initial
snapshot is re-run. See [build/doc/database-cdc.md](build/doc/database-cdc.md) and
[source/database-cdc-source/README.md](source/database-cdc-source/README.md).

### Connector support

| Connector | Connection | Catalog |
| --- | --- | --- |
| `kafka-source`, `kafka-sink` | kafka | — |
| `elasticsearch-source`, `elasticsearch-sink` | elasticsearch | — |
| `jdbc-source`, `jdbc-sink` | jdbc, mysql | — |
| `mysql-source`, `mysql-sink` | jdbc, mysql | — |
| `mongodb-source`, `mongodb-sink` | mongodb | — |
| `mysql-cdc-source` | mysql, jdbc | — |
| `postgres-cdc-source` | jdbc | — |
| `mongodb-cdc-source` | mongodb | — |
| `hive-source`, `hive-sink` | — | hive |
| `iceberg-source`, `iceberg-sink` | — | iceberg |
| `hudi-source`, `hudi-sink` | — | hudi |
| `paimon-source`, `paimon-sink` | — | paimon |
| `file-common-source`, `file-sink` | — | — |
| `console-sink` | — | — |

Catalog providers:

| Catalog | Connection | Spark catalog |
| --- | --- | --- |
| `hive` | hive-metastore (optional) | Hive session catalog |
| `iceberg` | hive-metastore, jdbc, mysql (optional) | named, or Hive session catalog when named `spark_catalog` |
| `jdbc` | jdbc, mysql (optional) | named |
| `hudi` | hive-metastore (optional) | Hudi session catalog |
| `paimon` | hive-metastore, jdbc, mysql (optional) | named, or generic session catalog |
| `gravitino` | gravitino | resolves to one of the native catalogs above |

Remote Hive metastore settings are installed by the `hive` catalog. Spark supports only one
session catalog per session; a job with more than one Hive, Hudi, generic Paimon or
`spark_catalog`-named Iceberg catalog is rejected. Iceberg, JDBC and named Paimon catalogs can
otherwise be combined and are installed as named Spark catalogs.

### MongoDB

`mongodb-source` uses the official MongoDB Spark connector. The connection supplies the
connection string (credentials are injected from `auth`), and the source selects the resource:

```yaml
source:
  - type: mongodb-source
    id: orders
    connection: mongo-prod
    config:
      database: app
      collection: orders
      options:
        readPreference.name: secondaryPreferred
```

For compatibility with existing jobs, an inline `config.uri` (or `config.connectionUri`) is
accepted when no connection is referenced. Connection properties that use the `spark.mongodb.`
namespace are passed to the connector unchanged; other properties are appended as MongoDB URI
options.

`mongodb-sink` uses the same connection and writes to a configured `database` and `collection`.
It supports insert/replace/update operations, id fields, upserts, batch and streaming writes. See
[sink/mongodb-sink/README.md](sink/mongodb-sink/README.md).

Kafka and Elasticsearch sinks are documented in
[sink/kafka-sink/README.md](sink/kafka-sink/README.md) and
[sink/elasticsearch-sink/README.md](sink/elasticsearch-sink/README.md).

## Incremental sync

Hudi, Paimon, Iceberg and JDBC sources can run bounded incremental reads. The source selects one
semantic and every sink applies it automatically:

| Semantic | Source output | Sink behavior |
| --- | --- | --- |
| `append` | only new rows | append |
| `upsert` | latest state per key | merge by `keyFields` |
| `cdc` | rows plus an `I`/`U`/`D` operation column | insert, update and delete |

A sink that cannot apply the semantic is rejected during configuration validation, before Spark
starts. `hudi-sink`, `paimon-sink`, `iceberg-sink`, `jdbc-sink` and `mysql-sink` support append,
upsert and CDC. `hudi-source` supports upsert and CDC, `paimon-source` supports append and CDC,
`iceberg-source` supports append, upsert and CDC, and `jdbc-source`/`mysql-source` support append
and upsert with a top-level `cursorField`. Iceberg upsert/CDC changelog reads require a catalog
table; JDBC snapshots cannot observe deletes.

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

The first run needs `start` or `initial: latest|full`; incremental state stores progress and
pending transaction metadata but does not bootstrap a read. The cursor is committed under the
configured incremental state only after every sink succeeds.
A single-writer lock protects the state, and a pending transaction is recorded before the sink
write. The pending entry includes a plan fingerprint over the connector configuration, the
transform chain and the resolved non-secret connection/catalog settings, so changing the source,
sink, transforms, keys or relevant configuration is rejected during recovery. JDBC, Redis and etcd locks can be renewable leases (`lockTtlMs`); a
lease exposes a fencing token, and a writer whose lock was taken over fails instead of overwriting
newer state. The filesystem backend (including HDFS/S3A state paths) has no atomic
compare-and-swap and rejects `lockTtlMs`; unlock a crashed writer with
`pudding state unlock`. Filesystem state on an object-store scheme is rejected unless
`incremental.allowBestEffortLock=true` is set explicitly; prefer JDBC, Redis or etcd when strict
single-writer fencing is required. If the process stops between the sink and the cursor commit,
`incremental.recovery` controls whether the batch is replayed (`auto` retries idempotent
upsert/CDC merges and append transactions that carry the transaction id) or requires
an operator decision:

```shell
bin/pudding state show <state-target> [source-id]
bin/pudding state status <state-target> [source-id] --json
bin/pudding state resolve <state-target> <source-id> --commit|--abort
bin/pudding state reset <state-target> <source-id> --cursor <cursor>
bin/pudding state unlock <state-target> <source-id>
bin/pudding state migrate <jdbc-state-target>
```

The default backend is the filesystem selected by `statePath`. A job can instead select JDBC,
Redis or etcd; all expose attempts, last commit/failure metadata, lock and fencing information
and the same recovery commands:

```yaml
incremental:
  state:
    backend: redis
    host: redis.internal
    port: 6379
    password: ${secret:env/REDIS_PASSWORD}
    prefix: pudding:incremental:
    lockTtlMs: 600000
```

```yaml
incremental:
  state:
    backend: jdbc
    url: jdbc:postgresql://state-db:5432/pudding
    user: pudding
    password: ${secret:env/STATE_DB_PASSWORD}
    driver: org.postgresql.Driver
    table: pudding_incremental_state
    lockTtlMs: 600000
```

Redis supports `mode: single|sentinel|cluster`; Sentinel uses `masterName` + `sentinels` (and
optional `sentinelUser`/`sentinelPassword`), and Cluster uses `clusterNodes`. Etcd attaches a
native lease when `lockTtlMs` is positive. A JDBC state table created before lease support can be
upgraded with `pudding state migrate`; leases are stored as epoch milliseconds in
`lock_expires_at_ms`. The distribution also includes
`libs/hadoop-aws-runtime-<version>.jar` (plus the OSS/Azure/GCS/COS packs) for object-store state paths and sources/sinks; remove a pack if the
Spark image already supplies a compatible Hadoop/AWS SDK pair.
Additional backends can implement the `IncrementalStateBackendProvider` SPI and extend
`AbstractIncrementalStateBackend`. See
[build/doc/incremental-sync.md](build/doc/incremental-sync.md) for the JDBC DDL/migration,
Redis modes, lock lease and fencing rules, properties-file state targets and `status --json`
fields.

Incremental counters are available through JMX at
`org.pudding.core.incremental:type=IncrementalMetrics`; the driver also logs structured
`incremental.*` audit events for lock and transaction transitions.

`job.mode: sync` with `job.strategy: incremental` requires one incremental source and one
incremental sink. Transforms are allowed only when they preserve key, operation and ordering
metadata.
Incremental sync is batch-only and uses one sink so progress has a single writer. See
[build/doc/incremental-sync.md](build/doc/incremental-sync.md) and
[build/doc/job-modes.md](build/doc/job-modes.md) for the full contract.

## Streaming ingestion

Continuous Kafka ingestion uses `job.mode: streaming`. It is a different mechanism from the
batch `sync incremental` journal: cursor progress is stored in the sink's Spark checkpoint, not
in the incremental state backend.

```yaml
job:
  mode: streaming
  trigger: "processing-time: 30 seconds"

source:
  - type: kafka-source
    id: kafka_orders
    config:
      subscribe: orders
      valueFormat: json
      valueSchema:
        id: long
        name: string
        dt: string
        __op: string
      cdcFormat: op-column

sink:
  - type: hudi-sink
    id: hudi_orders
    dependency: kafka_orders
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      operation: upsert
      recordKeyFields: [id]
      cdcOpColumn: __op
      cdcOrderFields: [_kafka_timestamp, _kafka_partition, _kafka_offset]
      checkpointLocation: hdfs://namenode:8020/checkpoints/hudi-orders
      outputMode: append
```

Supported combinations:

| Target | Append | Update / upsert | CDC |
| --- | --- | --- | --- |
| `hudi-sink` | native streaming (path or catalog location) | `foreachBatch` Hudi upsert; requires `recordKeyFields` | `foreachBatch` delete + upsert |
| `paimon-sink` | path or catalog streaming | `foreachBatch` Paimon primary-key merge | `foreachBatch` + `_row_kind_` |
| `iceberg-sink` | native streaming append or `operation: upsert` | `foreachBatch` + `MERGE INTO`; requires `recordKeyFields` | `foreachBatch` + `MERGE INTO` |
| `elasticsearch-sink` | native streaming append | native update; requires `idField` | configure `operation` |
| `jdbc-sink`, `mysql-sink` | `foreachBatch` JDBC append | `foreachBatch` staging + `MERGE` or keyed update/insert; requires `recordKeyFields` | not supported (no deletes) |

Rules and limits:

- Kafka source supports `json`, Confluent Schema Registry `avro`/`protobuf`, and a generic
  `op-column` or Debezium `before`/`after` CDC envelope. A value schema is required; JSON
  `parseMode` defaults to fail-fast. The optional `kafka-schema-registry-runtime` jar is selected
  automatically for Avro/Protobuf. For Confluent wire-format messages the embedded schema id is
  authoritative: `schemaRegistry.subject` fixes the subject used when the deserializer has to
  resolve by subject, but it does not require that subject to exist when the id is present. An
  unknown schema id fails with the topic and id in the error message.
- Native MySQL/PostgreSQL/MongoDB CDC sources emit the same `I`/`U`/`D` operation contract and
  are streaming-only. Their `schema` is required, and CDC/upsert sinks should key on the same
  columns. MongoDB delete rows carry only `_id`, so include `_id` in the declared schema.
- Streaming jobs accept exactly one sink so one checkpoint owns the Kafka offsets. All modes
  accept exactly one sink; split multi-target pipelines into separate jobs.
- Hudi and Paimon streaming accept a catalog table and resolve its storage location. Paimon
  1.3.2 + Spark 3.4 has no native catalog `toTable` streaming writer, so append uses the resolved
  path while update/CDC use the direct primary-key writer. Iceberg streaming requires an Iceberg
  catalog. All three formats require the target table to exist before the query starts.
- `outputMode` is `append` or `update` for Hudi, Iceberg, Paimon, Elasticsearch, JDBC and MySQL.
  `update` requires `recordKeyFields` (or `idField` for Elasticsearch) and is intended for
  stateful aggregation output; file, Kafka and MongoDB streaming sinks are append-only. `complete`
  is limited to the console display sink. `continuous` triggers are rejected by the lakehouse and
  JDBC sinks.
- A stateful streaming checkpoint stores a fingerprint of the source, transform chain and Spark
  version. Changing a stateful setting fails the restart before the query starts; stateless
  streaming jobs are not affected. See
  [build/doc/streaming-state.md](build/doc/streaming-state.md).
- CDC is checkpoint-bound at-least-once with key-idempotent writes; Kafka producer duplicates
  and cross-partition ordering are not removed. A microbatch is de-duplicated by key using
  `cdcOrderFields` or the Kafka/Debezium ordering metadata.
- `_kafka_*` and `_cdc_*` columns are internal bookkeeping and are removed by CDC sinks.
- `checkpointRetention` defaults to `keep`. `delete-on-success` is opt-in and only valid with
  `once` or `available-now`; it deletes the checkpoint only after positive evidence that the
  finite run drained its input. An external `query.stop()` before that evidence, a failure or a
  timeout keeps the checkpoint. Use
  `bin/pudding checkpoint status --json|show|delete|cleanup` to inspect or remove checkpoints outside
  a job; `cleanup --keep-last N` retains the newest checkpoints, and `--conf-file` supplies
  `fs.*` settings for S3A, OSS, GCS and other Hadoop filesystems. See
  [build/doc/streaming-checkpoints.md](build/doc/streaming-checkpoints.md). Prefer HDFS, NFS
  or a persistent volume for checkpoints and keep object storage for lakehouse data; if a
  checkpoint must be on object storage, use a dedicated prefix and one writer.
- Streaming query counters are exposed through JMX at
  `org.pudding.core.streaming:type=StreamingMetrics` and structured
  `streaming.query.started`, `streaming.query.progress`, `streaming.query.completed` and
  `streaming.query.failed` log events. Stateful queries also expose the last event-time
  watermark and its lag (`streaming.query.last.watermark.ms`,
  `streaming.query.watermark.lag.ms`) plus aggregate state rows, memory, removals and
  watermark-dropped rows (`streaming.state.*`).
- Kafka streaming adds driver-side lag and health metrics:
  `streaming.source.lag.records`, `.lag.max`, `.partitions`, `.lag.updated.ms` and
  `.healthy`. `streaming.query.last.progress.timestamp.ms` supports progress-age health checks.
  The latest-offset lookup is throttled by `lagMetricsIntervalMs` and never fails the query.
- Stateful streaming aggregation and deduplication require an event-time column and a
  watermark; see [build/doc/streaming-state.md](build/doc/streaming-state.md).

Examples: `build/examples/stream_kafka_to_hudi.yaml`,
`stream_kafka_to_paimon.yaml`, `stream_kafka_to_iceberg.yaml`,
`stream_window_aggregate.yaml`, `stream_window_update.yaml`, `stream_dedup.yaml` and
`stream_stream_join.yaml`. See [build/examples/README.md](build/examples/README.md) for the
full list.

## Table maintenance

`bin/pudding maintenance` runs one-shot Hudi, Paimon or Iceberg maintenance actions through
native Spark SQL procedures. It is separate from job modes and checkpoint/state tooling; use an
external scheduler for the cadence.

```shell
SPARK_MASTER=local[*] bin/pudding maintenance iceberg status \
  --catalog lake --table db.orders \
  --conf-file examples/maintenance/iceberg-catalog.properties --json

SPARK_MASTER=local[*] bin/pudding maintenance iceberg rewrite-data-files \
  --catalog lake --table db.orders --target-file-size 128MB --min-input-files 5 \
  --conf-file examples/maintenance/iceberg-catalog.properties

SPARK_MASTER=local[*] bin/pudding maintenance hudi compact \
  --path hdfs:///warehouse/orders --option op=scheduleAndExecute
```

Supported actions are `status`, `rewrite-data-files`, `rewrite-manifests`, `expire-snapshots`
and `remove-orphan-files` for Iceberg; `status`, `compact`, `compact-manifests`,
`expire-snapshots` and `remove-orphan-files` for Paimon; and `status`, `clean`, `compact`,
`cluster` and `archive` for Hudi. Destructive actions require `--confirm`; `--dry-run` previews
or uses the native dry-run. The `--conf-file` contains only `spark.*` keys and should be mode
`0600`. See [build/doc/table-maintenance.md](build/doc/table-maintenance.md) for safety rules,
per-format behavior and the programmatic `TableMaintenanceService` API.

## JDBC source and sink

`jdbc-source` and `jdbc-sink` read and write any JDBC table with Spark's built-in JDBC data
source. The distribution embeds the PostgreSQL 42.7.3 and MySQL 8.0.30 drivers; another database
needs its driver available on the Spark driver and executor classpath.

```yaml
source:
  - type: jdbc-source
    id: orders
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_ro
      password: '<read from a secret store; never commit>'
      driver: org.postgresql.Driver    # optional for jdbc:postgresql URLs
      options:
        fetchsize: '500'
sink:
  - type: jdbc-sink
    id: orders_out
    dependency: orders
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders_copy
      user: pudding_rw
      password: '<read from a secret store; never commit>'
      mode: overwrite                 # append | overwrite | ignore | error | errorifexists
      options:
        batchsize: '500'
```

Source keys are `url` (must start with `jdbc:`), `table`, `user`, `password` (may be empty),
optional `driver` and `options`. PostgreSQL URLs infer `org.postgresql.Driver`; other URLs must
set `driver`. `options` accepts string, boolean or number values and is passed to Spark JDBC;
the top-level connection keys take precedence. Both connectors also accept a
`connection: <name>` reference; connection values take precedence, and inline `user`, `password`
and `driver` are fallbacks when the connection does not provide them.

The JDBC source supports batch reads and sync-incremental cursor reads. The sinks support
`batch`, `streaming` and sync-incremental writes. The sink defaults to
`overwrite`; set `mode` to `append`, `overwrite`, `ignore`, `error` or `errorifexists`. In `sync`
with `strategy: full`, `jdbc-sink` and `mysql-sink` write to a staging table and replace the
target rows in one transaction; the target must already exist for the atomic path, and MySQL
tables must use a transactional engine such as InnoDB.

### JDBC incremental source

Add a monotonically increasing cursor column at the top level of the source config and an
`incremental` block:

```yaml
job:
  mode: sync
  strategy: incremental

source:
  - type: jdbc-source
    id: orders
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_ro
      password: ${secret:env/ORDERS_DB_PASSWORD}
      cursorField: updated_at
      incremental:
        semantics: upsert        # append | upsert
        keyFields: [id]          # required for upsert
        initial: full            # full | latest; first run only
        statePath: hdfs://namenode:8020/pudding/state/orders

sink:
  - type: jdbc-sink
    id: orders_copy
    dependency: orders
    config:
      url: jdbc:postgresql://db.internal:5432/warehouse
      table: public.orders_copy
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
```

- The range is `cursorField > start` and `cursorField <= end`. `start` comes from incremental
  state, `incremental.start`, or `initial=latest` (current maximum); `end` defaults to the
  current maximum.
- Numeric, date, time, timestamp and string cursors are supported. The cursor column is quoted
  from the target table metadata; values are escaped and never interpolated as raw SQL.
- `append` returns new rows. `upsert` keeps the latest complete row per key inside the range.
  Table snapshots cannot observe deletes; use a CDC source when deletes must be propagated.
- `cursorField` is source configuration, not part of the state file. Changing it is rejected on a
  pending transaction by the plan fingerprint, like other source configuration.
- `initial=latest` needs at least one non-null cursor value; an empty table must use
  `initial=full` or an explicit `incremental.start`.
- The source reads one table. Use `incremental.start` only for the first run; afterwards the
  committed cursor wins.

### JDBC CDC sink

`jdbc-sink` and `mysql-sink` support `cdc` in `sync` + `incremental` when the source emits an
operation column and ordering metadata:

```yaml
sink:
  - type: jdbc-sink
    id: orders_target
    dependency: orders_cdc
    config:
      url: jdbc:postgresql://db.internal:5432/warehouse
      table: public.orders
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
      cdcOpColumn: __op
      cdcOrderFields: [_cdc_timestamp, _cdc_sequence]  # optional if the source supplies order
```

The sink keeps the latest operation per key, applies deletes and upserts in one database
transaction, and requires a primary key or unique index that matches the incremental key fields.
`append` is not idempotent; `upsert` and `cdc` are replay-safe for the same rows. Streaming
writes accept the same `cdcOpColumn`; see the streaming section below.

Streaming writes use `foreachBatch` and require `checkpointLocation`:

```yaml
sink:
  - type: jdbc-sink
    id: orders_stream
    dependency: orders_stream
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_rw
      password: '<read from a secret store; never commit>'
      checkpointLocation: hdfs://namenode:8020/checkpoints/jdbc-orders
      outputMode: update            # append | update
      recordKeyFields: [id]
```

- `outputMode: append` writes each microbatch with Spark JDBC append. A missing target is created
  from the first non-empty batch.
- `outputMode: update` treats every microbatch as complete rows and upserts them by
  `recordKeyFields`. The target must already exist and have a primary key or unique index whose
  column set exactly matches the configured keys. Every target column must be present in the
  streaming rows; extra internal `_kafka_*`/`_cdc_*` columns are dropped.
- PostgreSQL and H2 use `MERGE`; MySQL and MariaDB use a keyed `UPDATE ... JOIN` followed by
  `INSERT ... WHERE NOT EXISTS`. Each batch is written to a private staging table and merged in
  one database transaction, then the staging table is dropped. The MySQL path matches only
  `recordKeyFields`, unlike `ON DUPLICATE KEY UPDATE`, which also follows unrelated unique
  indexes. Null or duplicate keys fail the batch instead of choosing an arbitrary row.
- `cdcOpColumn` on a streaming sink applies `I`/`U`/`D` in one database transaction per
  micro-batch. It requires `recordKeyFields`, a target table with a matching primary key or unique
  index, `outputMode: append`, and `checkpointLocation`. Without `cdcOpColumn`, `outputMode:
  update` upserts complete rows and deletes are not applied. `mode` in a streaming sink config may
  only be `append`.
- The merge is idempotent for unchanged full rows, so a checkpoint replay after a committed merge
  does not duplicate data. It is not an exactly-once guarantee when another writer changes the
  same keys concurrently; use one writer per target key set.
- Dialect support for streaming upsert is PostgreSQL, MySQL, MariaDB and H2. Other JDBC databases
  remain batch-only unless the `MERGE` path is extended.

Validation errors name the offending key but never echo URLs, passwords or option values, and
job configuration logging uses the core redactor. Keep any job file containing credentials at
mode `0600` and delete it after submission; prefer injecting credentials from a secret store
rather than committing them.

`jdbc-source`, `jdbc-sink`, `mysql-source` and `mysql-sink` share
`libs/jdbc-common-<version>.jar`. The client adds it automatically; direct `spark-submit` jobs
must include it in `--jars`.

## Transform operators

`sql-transform` executes SQL over one or more dependencies. The `basic-transform` module adds
DataFrame operators that are useful for common pipeline steps:

| Type | Purpose | Dependencies |
| --- | --- | --- |
| `filter-transform` | Filter rows with a Spark SQL condition | exactly 1 |
| `aggregate-transform` | Group and aggregate with SQL expressions, including event-time windows in streaming | exactly 1 |
| `dedup-transform` | Keep one row per key, with a watermark-bounded streaming state | exactly 1 |
| `union-transform` | Merge rows with `union` / `unionByName` | 2 or more |
| `column-transform` | Select, rename and cast columns | exactly 1 |
| `limit-transform` | Limit the number of rows | exactly 1 |
| `join-transform` | Join two DataFrames, including inner stream-stream joins with watermarks | exactly 2 |
| `validate-transform` | Validate required columns and SQL rules; fail, drop or quarantine invalid rows | exactly 1 |
| `schema-contract-transform` | Fail when the input schema does not match declared columns and types | exactly 1 |

`filter-transform`, `column-transform`, `union-transform` and `validate-transform` (with
`mode: drop`) also run in `streaming` jobs. `sql-transform` supports stateless streaming SQL.
`aggregate-transform` supports stateful streaming with an explicit `eventTime`, `watermark` and
event-time `window`; `dedup-transform` supports watermark-bounded streaming deduplication.
`join-transform` supports inner stream-stream joins when both sides declare event time and a
watermark; outer and semi/anti streaming joins remain unsupported. `limit-transform` and
stateful SQL remain batch-only. See
[build/doc/streaming-state.md](build/doc/streaming-state.md).

```yaml
transform:
  - type: filter-transform
    id: filter1
    dependencies: [source1]
    config:
      condition: "amount >= 100"

  - type: column-transform
    id: columns1
    dependencies: [filter1]
    config:
      fields:
        - order_id
        - name: city
          source: order_city
        - name: amount
          type: double

  - type: aggregate-transform
    id: aggregate1
    dependencies: [columns1]
    config:
      groupBy: [city]
      aggregations:
        order_count: "count(*)"
        total_amount: "sum(amount)"
```

Every operator supports an optional `numPartitions`. `union-transform` and `join-transform` take
multiple dependencies; `union-transform` in `byPosition` mode and `join-transform` need explicit
input/left/right names because the runtime dependency map is not ordered. The client attaches the
`basic-transform` jar automatically when one of these types is used. See
[transform/basic-transform/README.md](transform/basic-transform/README.md) for every option and
join semantics.

## Capability manifest

Every distribution contains a generated `capabilities.yaml` at its root. It enumerates the
sources, sinks, transforms, connection providers and catalog providers of that exact
distribution, including the jar that provides each component. Provider components point at
`connection-providers-*.jar` or `catalog-providers-*.jar`. The runtime jars for Hudi, Paimon,
Iceberg, Elasticsearch, MongoDB, Kafka and the optional Kafka Schema Registry runtime are support
artifacts rather than components, so they are not listed separately; the client adds the matching
runtime jar for each connector used by a job.

```yaml
schemaVersion: "2.1"
productVersion: "1.0.0"
sparkVersion: "3.4.0"
scalaBinaryVersion: "2.12"
components:
  - kind: source
    type: mongodb-source
    className: org.pudding.source.MongodbSource
    module: mongodb-source
    jar: mongodb-source-1.0.0.jar
    capabilities:
      connectionTypes: [mongodb]
      catalogTypes: []
      executionModes: [batch]
      syncIncrementalSemantics: []
      lakeSyncIncremental: false
```

`executionModes` lists internal batch/streaming support; `streamingOutputModes` lists the sink
streaming modes (`append`, `update`, `complete`) and `streamingUpsert` marks key-based update
semantics; `syncFullRefresh` indicates a sink that can replace its target for `sync` + `full`;
`syncAtomicReplace` indicates that replacement is atomic for the selected backend;
`syncIncrementalSemantics` lists the source or sink contract (`append`, `upsert` or `cdc`) for
`sync` + `incremental`; `idempotentSyncIncrementalSemantics` lists sink semantics that can
safely replay an uncommitted batch; `syncIncrementalFencing`, `lakeSyncIncremental` and
`preservesSyncIncrementalMetadata` describe the remaining sync capabilities. The backend can
preflight a pipeline with the same rules the engine applies. The backend should read the
manifest from the distribution it is about to run, not from its own classpath. Loading is
strict: a missing root manifest is rejected. The manifest is generated at build time from the
actual ServiceLoader registrations, so capability changes do not require a manually maintained
list. Schema `1.0` is rejected without migration; schema `2.0` remains readable. Regenerate the
manifest or rewrite third-party fragments for `2.0` or `2.1`.

Third-party connectors can add a `META-INF/spark-pudding/component.yaml` fragment to a jar placed
under `libs`; the backend scans jars recursively and merges fragments, rejecting duplicate
`kind:type` entries. The capability API in core (`CapabilityManifest.load`,
`CapabilityManifest.verifyDistribution`, `CapabilityManifest.diff`) is bundled in
`bin/pudding-client.jar`, together with the `bin/pudding capabilities verify|show|diff` commands.
Keep a
manifest snapshot per product version to detect added, removed and changed capabilities. See
[build/doc/capability-manifest.md](build/doc/capability-manifest.md) for the schema, fragment
rules, strict-loading behavior and backend usage.

## Submitting jobs with pudding submit

`pudding submit` is the recommended way to run a job. It derives the execution target from
`job.master`, adds the engine, connector, provider and runtime jars the job needs, delivers the
job configuration as a private temporary file or a Kubernetes Secret, and reports the real
application result. The environment file is required and may be empty.

Run attached, which is the right form for a scheduler because the process exits only when the
application finishes:

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml"
```

Run in the background and follow the log with an optional third argument:

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml" \
  "$PUDDING_HOME/log/my-job.log"

tail -f "$PUDDING_HOME/log/my-job.log"
```

The target follows `job.master`: a local master uses the `local` target in client mode, `yarn`
uses YARN (cluster mode by default, client mode accepted) and `k8s://` uses Kubernetes (cluster
mode required). Use `bin/pudding k8s submit` when the submission needs an image, namespace or
service account; it also creates and removes the job-configuration Secret. With a log file the
command returns after starting the job in the background; without one it stays attached and
exits `0` only when the application reaches `FINISHED`.

The unified command exposes the common path only. For `--target`, `--master`, `--resourceBase`,
`--conf` and other target overrides, run `bin/pudding-client.jar` directly:

```shell
"$PUDDING_HOME/bin/pudding-client.jar" \
  -c "$PUDDING_HOME/conf/env.properties" \
  -j "$PUDDING_HOME/examples/read_csv_example.yaml" \
  --target local --conf spark.driver.memory=1g
```

See [client/README.md](client/README.md) for the SDK and all client options, and
[build/doc/cli.md](build/doc/cli.md) for complete examples of every `pudding` subcommand.

## Running the engine with spark-submit

Starting the engine directly is an alternative to `pudding submit`. The pipeline is passed with
`--confType` and exactly one of `--jobText`, `--jobEnv` or `--jobFile`:

```shell
"$SPARK_HOME/bin/spark-submit" \
  --class org.pudding.core.PuddingSpoon \
  --master yarn \
  --deploy-mode cluster \
  --files "$PUDDING_HOME/examples/read_csv_example.json" \
  --jars "$PUDDING_HOME/libs/console-sink-1.0.0.jar,$PUDDING_HOME/libs/file-common-source-1.0.0.jar" \
  "$PUDDING_HOME/libs/pudding-core-1.0.0.jar" \
  --confType json \
  --jobFile read_csv_example.json
```

The example above uses only a file source and a console sink. Jobs that use a connection or
catalog must also add `libs/connection-providers-<version>.jar` and
`libs/catalog-providers-<version>.jar` to `--jars`; the client adds them automatically. Jobs
that use Hudi, Paimon, Iceberg, Elasticsearch, MongoDB or Kafka must additionally add the
matching `libs/*-runtime-<version>.jar`. Kafka jobs with `valueFormat: avro` or `protobuf` also
need `libs/kafka-schema-registry-runtime-<version>.jar`. JDBC and MySQL source/sink jobs must add
`libs/jdbc-common-<version>.jar`.

In cluster mode the application jar, `--jars` entries and data paths must be visible to the
cluster (for example HDFS URIs). Prefer `--jobFile`, staged with `--files`, or `--jobEnv` with
`spark.yarn.appMasterEnv.<NAME>`; a driver in YARN cluster mode does not inherit the submission
host's environment, so a bare `--jobEnv` is only reliable for local or client-mode drivers.
Inline `--jobText` is visible in process listings and logs. A file must be readable by the driver
process; the client SDK writes an owner-only temporary file and removes it when the application
finishes. For Kubernetes, store the configuration in a Secret and inject it into the driver:

```shell
--conf spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG=<secret>:<key>
```

The Kubernetes target does this automatically and never puts the job configuration on the
spark-submit command line. The `sparkPuddingClient` CLI requires `--k8s-job-secret` (unless you
use `bin/pudding k8s submit`, which creates a Secret for you). Long-lived services using
`SubmissionManager` can request an SDK-managed Secret per submission (`managedJobSecret()`); the
manager creates `pudding-job-<submissionId>`, records only its name/key/ownership, and deletes
it after a final state or failed launch, retrying cleanup on reconcile and at startup.

## Tests

`mvn verify` runs the unit and local integration suites. The lakehouse Hudi/Iceberg/Paimon
semantic, catalog/path and atomic-refresh suites are listed in
[build/doc/lakehouse-matrix.md](build/doc/lakehouse-matrix.md). External-service tests are opt-in
and skip when the environment variable is absent:

| Test | Environment | Purpose |
| --- | --- | --- |
| `RedisStateBackendTest` | `PUDDING_TEST_REDIS_HOST`/`_PORT`, or `PUDDING_TESTCONTAINERS=true` | State journal, lock lease, fencing, Sentinel/Cluster config and SPI loading. |
| `EtcdStateBackendTest` | `PUDDING_TEST_ETCD_ENDPOINT`, or `PUDDING_TESTCONTAINERS=true` | The same contract through the etcd v3 HTTP gateway with native leases. |
| `RemoteFileSystemStateBackendTest` | `PUDDING_TEST_STATE_URI`, optional `PUDDING_TEST_STATE_CONF` | HDFS or S3A state directory, explicit unlock recovery, clean-up and checkpoint CLI show/status/delete. Use a disposable base URI. |
| `ContainerRemoteStateTest` | `PUDDING_TESTCONTAINERS=true` | Testcontainers-backed HDFS (NameNode/DataNode) and MinIO S3A state directories, pending-transaction recovery, checkpoint inspection/deletion and checkpoint restart/delete-on-success. |
| `IncrementalStateCrashRecoveryTest` | none | Launches a separate JVM, kills it after the pending transaction is written and recovers the journal and lock. |
| `BuildJobCrashRecoveryTest` | none | Runs the real BuildJob in a child JVM, kills it after the sink commit and verifies one sink write plus cursor recovery. |
| `KafkaSourceStreamingIntegrationTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_KAFKA_IMAGE` | Kafka container, JSON/CDC decoding and Structured Streaming offset processing. |
| `IcebergS3FileIOTest` | `PUDDING_TESTCONTAINERS=true`, or `PUDDING_TEST_S3_ENDPOINT` + optional access/secret keys | MinIO S3FileIO data-layer write/read with an AWS SDK-backed JdbcCatalog. |
| `HudiObjectStoreTest` | `PUDDING_TESTCONTAINERS=true`, or `PUDDING_TEST_S3_ENDPOINT` + optional access/secret keys | Hudi table data, commits, incremental read, full refresh and S3A checkpoints. |
| `PaimonObjectStoreTest` | `PUDDING_TESTCONTAINERS=true`, or `PUDDING_TEST_S3_ENDPOINT` + optional access/secret keys | Paimon catalog tables, snapshots, CDC, full refresh and S3A checkpoints. |
| `JdbcStreamingTest` | none | H2 `MERGE`, JDBC upsert validation and staging cleanup. |
| `JdbcStreamingSinkTest` | none | H2 end-to-end checkpointed `append` and `update` streaming through `jdbc-sink`. |
| `JdbcSinkPostgresContainerTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_POSTGRES_IMAGE` | PostgreSQL container, `jdbc-sink` atomic full-refresh replacement, rollback, streaming upsert and staging cleanup. |
| `MysqlSinkContainerTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL/InnoDB container, `mysql-sink` atomic full-refresh replacement, rollback, streaming upsert and staging cleanup. |
| `MysqlCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL binlog snapshot, insert/update/delete and checkpoint restart. |
| `PostgresCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_POSTGRES_IMAGE` | PostgreSQL `pgoutput` snapshot, insert/update/delete and checkpoint restart. |
| `MongodbCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MONGO_IMAGE` | MongoDB replica-set snapshot, change stream, updateLookup/delete and resume-token restart. |
| `DatabaseCdcHudiPaimonTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL CDC → Hudi/Paimon CDC sinks with snapshot, DML, checkpoint restart and target-row verification. |
| `DatabaseCdcIcebergTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL CDC → Iceberg CDC sink with snapshot, DML, checkpoint restart and target-row verification. |

Set `PUDDING_TESTCONTAINERS_REQUIRED=true` to fail rather than skip when a container can not
start; the CI workflow sets it. `PUDDING_TESTCONTAINERS_SKIP_S3=true` allows a local HDFS-only
iteration without pulling MinIO. `PUDDING_TEST_HDFS_IMAGE`, `PUDDING_TEST_MINIO_IMAGE`,
`PUDDING_TEST_KAFKA_IMAGE`, `PUDDING_TEST_POSTGRES_IMAGE`, `PUDDING_TEST_MYSQL_IMAGE` and
`PUDDING_TEST_MONGO_IMAGE` override the container images for restricted registries. See
[build/doc/compatibility.md](build/doc/compatibility.md) for the real-S3 runbook and version
matrix. The maintenance module also has local Spark integration suites for Iceberg, Paimon and
Hudi; they run as part of `mvn verify` and need no external service.

For example:

```shell
PUDDING_TESTCONTAINERS=true \
  mvn -pl provider/redis-state-provider -am test

PUDDING_TEST_REDIS_SENTINEL_ENDPOINTS=redis-1:26379,redis-2:26379 \
PUDDING_TEST_REDIS_SENTINEL_MASTER=mymaster \
  mvn -pl provider/redis-state-provider -am test

PUDDING_TEST_STATE_URI=hdfs://namenode:8020/tmp/pudding-test \
PUDDING_TEST_STATE_CONF=/etc/hadoop/conf \
  mvn -pl core test
```

## Code check

The project follows Apache Spark's Scala and Java style rules:

```shell
mvn scalastyle:check
mvn -pl client checkstyle:check
```

`scalastyle` is part of the default build. `maven-checkstyle-plugin` is bound to the `validate`
phase of the Java `client` module, so `mvn verify` (or even `mvn -pl client validate`) runs it
automatically. The ruleset is [build/checkstyle.xml](build/checkstyle.xml), and narrow
exceptions belong in
[build/checkstyle/checkstyle-suppressions.xml](build/checkstyle/checkstyle-suppressions.xml). To
find newly added Java modules that should also run Checkstyle, search for `*.java` under
`src/main/java`.

## Security

- Prefer `${secret:env/NAME}` or `${secret:file:/path#key}` references in `connection` and
  component configuration. References are resolved on the driver; the client never resolves them.
- Job files and environment files can contain credentials. Keep them readable only by the
  submitting user (mode `0600`) and delete temporary copies after use.
- The engine redacts credential-like configuration values when it logs configuration. The client
  SDK does not put the job configuration on the command line: local and YARN submissions receive
  an owner-only `--jobFile`, and Kubernetes receives a Secret reference. Verbose command logging
  therefore shows a file path or environment variable name, not the configuration content.
- For Kubernetes, keep credentials in Secrets rather than ConfigMaps or job arguments. The job
  configuration is injected through a Secret key reference; an auto-created Secret is deleted
  when `bin/pudding k8s submit` exits or when the owning submission reaches a final state. See
  [build/k8s/README.md](build/k8s/README.md).

## Core concepts

| Name | Description |
| --- | --- |
| job mode | `batch`, `sync` or `streaming`; selects the processing contract. |
| sync strategy | `full` or `incremental`; required only for `job.mode: sync`. |
| execution mode | Internal `batch` or `streaming`, derived from the job mode. |
| state backend | Cursor/journal storage for incremental jobs: filesystem, JDBC, Redis or etcd, with a provider SPI. |
| connection | Reusable endpoint, credentials and client properties. Referenced by sources and sinks. |
| catalog | Table metadata installed into the Spark session, for example Hive, Iceberg, Hudi, Paimon or Gravitino. |
| source | Reads data through a Spark data source. The interface is defined by `DataSource`. |
| transform | Applies Spark data processing. The interface is defined by `DataTransform`. |
| sink | Writes data through a Spark data sink. The interface is defined by `DataSink`; every mode writes exactly one sink. |

## Repository layout

```text
client/     submission SDK and command line client
core/       stable SPI, configuration model, provider registry and Spark entry point
connector/  shared connector support libraries (currently jdbc-common)
provider/   connection, catalog and optional state-backend provider implementations
runtime/    shared connector runtimes and Hadoop object-store packs (S3A/OSS/Azure/GCS/COS, CDC)
source/     source connectors
transform/  transform implementations
sink/       sink connectors
maintenance/ table-maintenance API, Spark runner and CLI
build/      distribution assembly, launchers and Kubernetes assets
integration/ opt-in object-store, lakehouse and native CDC regression suites
```
