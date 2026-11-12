# Shared connector runtimes

`runtime/` contains the third-party runtimes for Hudi, Paimon, Iceberg, Elasticsearch,
MongoDB, Kafka and native database CDC, plus the Hadoop AWS/S3 runtime used by S3A paths. They
are packaged once and shared by the corresponding source and sink implementations.

```text
runtime/
  hudi-runtime/
  paimon-runtime/
  iceberg-runtime/
  elasticsearch-runtime/
  mongodb-runtime/
  kafka-runtime/
  kafka-schema-registry-runtime/
  cdc-runtime/
  hadoop-aws-runtime/
  hadoop-oss-runtime/
  hadoop-azure-runtime/
  gcs-runtime/
  hadoop-cos-runtime/
```

```text
libs/hudi-source-*.jar       # Pudding adapter only
libs/hudi-sink-*.jar         # Pudding adapter only
libs/hudi-runtime-*.jar      # Hudi Spark bundle, HFile/HBase/Jetty, one copy
libs/paimon-source-*.jar
libs/paimon-sink-*.jar
libs/paimon-runtime-*.jar
libs/iceberg-source-*.jar
libs/iceberg-sink-*.jar
libs/iceberg-runtime-*.jar
libs/elasticsearch-source-*.jar
libs/elasticsearch-sink-*.jar
libs/elasticsearch-runtime-*.jar
libs/mongodb-source-*.jar
libs/mongodb-sink-*.jar
libs/mongodb-runtime-*.jar
libs/kafka-source-*.jar
libs/kafka-sink-*.jar
libs/kafka-runtime-*.jar
libs/kafka-schema-registry-runtime-*.jar # optional Confluent Avro/Protobuf decoding
libs/database-cdc-source-*.jar  # MySQL/PostgreSQL/MongoDB CDC adapter and Spark V2 provider
libs/cdc-runtime-*.jar          # Debezium connectors, JDBC and MongoDB drivers
libs/hadoop-aws-runtime-*.jar   # S3A for source/sink paths and S3 state directories
libs/hadoop-oss-runtime-*.jar   # Aliyun OSS (oss://)
libs/hadoop-azure-runtime-*.jar # Azure ABFS/WASB (abfs://, abfss://, wasb://, wasbs://)
libs/gcs-runtime-*.jar          # Google Cloud Storage (gs://)
libs/hadoop-cos-runtime-*.jar   # Tencent COS (cosn://)
libs/table-maintenance.jar      # shaded maintenance API + Spark runner; format runtime selected by bin/pudding maintenance
```

## Selection

The client selects a runtime once per format, based on the source, sink and catalog types in the
job:

| Job references | Runtime jar |
| --- | --- |
| `hudi-source`, `hudi-sink`, `catalog: hudi` | `hudi-runtime` |
| `paimon-source`, `paimon-sink`, `catalog: paimon` | `paimon-runtime` |
| `iceberg-source`, `iceberg-sink`, `catalog: iceberg` | `iceberg-runtime` |
| `elasticsearch-source`, `elasticsearch-sink` | `elasticsearch-runtime` |
| `mongodb-source`, `mongodb-sink` | `mongodb-runtime` |
| `kafka-source`, `kafka-sink` | `kafka-runtime` |
| `kafka-source` with `valueFormat: avro` or `protobuf` | `kafka-runtime` and `kafka-schema-registry-runtime` |
| `mysql-cdc-source`, `postgres-cdc-source`, `mongodb-cdc-source` | `cdc-runtime` and the shared `database-cdc-source` adapter jar |
| `catalog: jdbc` | `jdbc-common`; adds `jdbc-source` when the pipeline has no JDBC source/sink (its jar carries the embedded drivers) |
| `catalog: hive` | none |
| `catalog: gravitino` | `iceberg-runtime`, `hudi-runtime`, `paimon-runtime`, `jdbc-common`; adds `jdbc-source` when the pipeline has no JDBC source/sink |

Using both a source and a sink of the same connector, or a format catalog plus a connector, still
adds only one runtime jar. A catalog-only job (for example a SQL transform reading a Hudi table) gets
the runtime because it contains the format's Spark extension and data source.

A Gravitino catalog is resolved on the driver to a native Hudi, Paimon, Iceberg, Hive or JDBC
catalog. A source or sink `catalog:` reference to the Gravitino catalog is validated after
resolution against that native type.

`hadoop-aws-runtime` is a base support jar: it is included in every submission because S3 can
appear in a connector path or an incremental state path without a dedicated connector type. The
OSS, Azure, GCS and COS packs are selected when the job configuration contains the corresponding
URI scheme. All packs bundle only the object-store client dependencies; Hadoop and Spark classes
are provided by the runtime. Remove a pack from `libs` when the Spark image already supplies a
compatible client. `redis-state-provider` and `etcd-state-provider` are also base support jars
because they are selected by state configuration, not by a connector type.

| Scheme | Runtime jar |
| --- | --- |
| `s3://`, `s3a://` | `hadoop-aws-runtime` |
| `oss://` | `hadoop-oss-runtime` |
| `abfs://`, `abfss://`, `wasb://`, `wasbs://` | `hadoop-azure-runtime` |
| `gs://`, `gcs://` | `gcs-runtime` |
| `cosn://` | `hadoop-cos-runtime` |

Iceberg's data layer uses `S3FileIO` for `warehouse: s3://...`. The runtime shades
`iceberg-aws` together with the `iceberg-aws-bundle` AWS SDK v2 classes, so S3FileIO can create
its client without an external SDK. S3A checkpoint/state paths still
use the Hadoop filesystem. Set other Iceberg FileIO implementations explicitly in the catalog
properties when required.

A direct `spark-submit` invocation must add the matching runtime jar to `--jars`. The client adds
it automatically.

`kafka-schema-registry-runtime` is selected only for Kafka sources that use the Confluent
Schema Registry Avro/Protobuf wire format. It shades Confluent Schema Registry, Avro, Protobuf
and their Jackson/Guava dependencies under `org.pudding.kafka.schema.shaded.*`; Kafka client
classes are provided by Spark. It is deliberately separate from `kafka-runtime` so JSON-only
Kafka jobs do not carry an 18 MB Avro/Protobuf runtime or its classpath conflicts.

`iceberg-runtime` also shades `scala-collection-compat_2.12`. Iceberg's Spark 3.4 runtime uses
`scala.jdk.CollectionConverters`, which is a Scala 2.13 API; Spark 3.4 runs on Scala 2.12. Do not
replace the runtime jar with the plain Iceberg artifact, or the Iceberg SQL extension will fail
with `ClassNotFoundException: scala.jdk.CollectionConverters$`.

## Capability manifest

Runtime jars are support artifacts, not Pudding components. They do not appear in
`capabilities.yaml` as `kind: source`, `kind: sink` or `kind: catalog`; those entries continue to
point at the thin connector or provider jars. Runtime selection is currently based on the format
mapping above.

## Versioning

A connector and its runtime must be built and released together. The runtime modules use the same
`hudi.version`, `paimon.version`, `iceberg.version`, `elasticsearch.version`,
`mongodb.spark.version`, `debezium.version` and Spark binary version properties as the
connectors. Do not mix a runtime jar from one release with a connector from another.

`cdc-runtime` includes the Debezium MySQL/PostgreSQL connectors, Kafka Connect API, MySQL and
PostgreSQL JDBC drivers, and the MongoDB Java driver pinned to the same 5.1.x line used by
`mongodb-runtime`. Debezium's Jackson and Guava dependencies are relocated under
`org.pudding.cdc.shaded.*`; Debezium and Kafka Connect classes themselves are not relocated
because the source adapter implements the Connect `SourceTask` API directly. See
[build/doc/database-cdc.md](../build/doc/database-cdc.md) for the source contract and database
prerequisites.
