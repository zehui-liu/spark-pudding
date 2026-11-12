# Compatibility and test matrix

Spark Pudding targets one Spark baseline per release. Connector runtimes and lakehouse format
libraries are binary-bound to that baseline, so the versions below form a tested set rather than
independent options.

## Build and runtime matrix

| Component | Tested version | Notes |
| --- | --- | --- |
| JDK | 11, 17 | CI runs the full verify on JDK 11, compiles on JDK 11 and 17, and runs the test phase plus native CDC/lakehouse container regressions on JDK 17. The build rejects JDK 8 and JDK 18+ at the `validate` phase. |
| Scala | 2.12.18 | `scala.binary.version` is `2.12`; Scala 2.13 and Scala 3 are not supported. |
| Apache Spark | 3.4.0 | `spark.version` in the root `pom.xml`. The driver must use the same version. |
| Hadoop client | 3.3.4 | `hadoop.version`; bundled S3A, OSS, Azure ABFS/WASB, GCS and COS runtime packs. |
| HDFS test container | 3.3.6 | `apache/hadoop:3.3.6`; exercises the HDFS state and checkpoint paths. |
| Hudi | 0.14.0 | Built for Spark 3.4 through `hudi.spark.binary.version`. |
| Paimon | 1.3.2 | Built for Spark 3.4 through `paimon.spark.binary.version`. |
| Iceberg | 1.5.0 | Built for Spark 3.4 through `iceberg.spark.binary.version`. |
| Kafka connector | 3.4.0 | Follows `spark.version`; the test container runs Kafka 3.8.0. |
| Confluent Schema Registry | 7.4.0 | Optional `kafka-schema-registry-runtime`; Avro/Protobuf decoding only. |
| Elasticsearch connector | 8.9.1 | `elasticsearch.version`. |
| MongoDB Spark connector | 10.5.0 | `mongodb.spark.version`. |
| Debezium | 1.9.8.Final | Last Java 8-compatible line; `debezium.version`. Used by native MySQL/PostgreSQL CDC. |
| Kafka Connect API | 3.2.0 | `debezium.connect.version`; bundled by `cdc-runtime`. |
| MongoDB Java driver | 5.1.4 | `mongodb.driver.version`; shared by `cdc-runtime` and `mongodb-runtime`. |
| Maven | 3.8.7 | The CI workflow uses the Maven version from the runner setup. |

Not covered: Windows, JDK 8 and JDK 18+, Spark 3.3 or 3.5, Hadoop 2, Scala 2.13/3, and
Kubernetes distributions other than the documented kind-based workflow.

## Changing versions

Do not change `spark.version` in isolation. Update the format runtimes and connectors together:

- `hudi.spark.binary.version`, `paimon.spark.binary.version` and
  `iceberg.spark.binary.version` select the Spark-bound artifacts;
- `spark-sql-kafka-0-10_2.12` follows `spark.version`;
- `elasticsearch.version` and `mongodb.spark.version` pin their connector lines.

Then run `mvn clean verify`, the container-backed tests below, and the Kubernetes image check
described in `README.md`. A capability manifest from a different baseline is not interchangeable.

## Local and container-backed tests

`mvn verify` covers unit tests and local integration tests. The maintenance module's Iceberg,
Paimon and Hudi status/action suites run locally as part of this command and need no external
service. External-service suites are opt-in. They start containers only when
`PUDDING_TESTCONTAINERS=true`; when a container can not start they skip, unless
`PUDDING_TESTCONTAINERS_REQUIRED=true`, which CI sets so a broken environment fails the build.

| Suite | Environment | Coverage |
| --- | --- | --- |
| `JdbcStreamingTest` | none | H2 `MERGE`; JDBC upsert insert/update, composite keys, target-key validation, null/duplicate key rejection, ordered CDC delete/upsert and staging cleanup. |
| `JdbcStreamingSinkTest` | none | H2 end-to-end checkpointed `append` and `update` streaming through `jdbc-sink`. |
| `JdbcSinkPostgresContainerTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_POSTGRES_IMAGE` | PostgreSQL container; `jdbc-sink` atomic full-refresh replacement, rollback, streaming upsert and staging-table cleanup. |
| `MysqlSinkContainerTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL/InnoDB container; `mysql-sink` atomic full-refresh replacement, rollback, streaming upsert and staging-table cleanup. |
| `MysqlCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL binlog snapshot, insert/update/delete and checkpoint restart. |
| `PostgresCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_POSTGRES_IMAGE` | PostgreSQL `pgoutput` snapshot, insert/update/delete and checkpoint restart. |
| `MongodbCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MONGO_IMAGE` | MongoDB replica-set snapshot, change stream, updateLookup/delete and resume-token restart. |
| `DatabaseCdcHudiPaimonTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL binlog CDC source → Hudi/Paimon CDC sinks; snapshot, insert/update/delete, checkpoint restart and final target rows. |
| `DatabaseCdcIcebergTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | MySQL binlog CDC source → Iceberg CDC sink; snapshot, insert/update/delete, checkpoint restart and final target rows. |
| `DebeziumTaskSpikeTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_MYSQL_IMAGE` | Direct Debezium `SourceTask` lifecycle with a custom `SourceTaskContext`. |
| `KafkaSourceStreamingIntegrationTest` | `PUDDING_TESTCONTAINERS=true`, optional `PUDDING_TEST_KAFKA_IMAGE` | Kafka container, JSON/CDC decoding and streaming offsets. |
| `ConfluentValueDecoderTest` | none | Confluent Avro/Protobuf wire payload decoding through the optional runtime. |
| `JdbcIncrementalSourceTest` | none | H2 numeric/timestamp cursor ranges, upsert dedup and first-run modes. |
| `IcebergSourceStreamingTest` | none | Iceberg catalog/path streaming DataFrames, batch-selector rejection and historical `stream-from-timestamp`. |
| `IcebergSourceIncrementalTest` | none | Iceberg append/upsert/CDC changelog reads, update delete/insert normalization and delete rejection for upsert. |
| `IcebergSinkTest` | none | Streaming append/update/CDC and sync incremental append/upsert/CDC with replay. |
| `IcebergS3FileIOTest` | `PUDDING_TESTCONTAINERS=true`, or `PUDDING_TEST_S3_ENDPOINT` + optional access/secret keys | MinIO S3FileIO data-layer write/read with an AWS SDK-backed JdbcCatalog. |
| `HudiObjectStoreTest` | `PUDDING_TESTCONTAINERS=true`, or `PUDDING_TEST_S3_ENDPOINT` + optional access/secret keys | Hudi table data, timeline commits, incremental read, full refresh and S3A checkpoints on object storage. |
| `PaimonObjectStoreTest` | `PUDDING_TESTCONTAINERS=true`, or `PUDDING_TEST_S3_ENDPOINT` + optional access/secret keys | Paimon catalog table creation, snapshots, CDC, full refresh and S3A checkpoints on object storage. |
| `HudiSourceTest` | none | Hudi path/catalog batch and catalog incremental reads. |
| `HudiSinkTest` | none | Hudi path/catalog writes, catalog streaming, empty-microbatch handling, COW/MOR atomic full refresh and failed-refresh rollback. |
| `PaimonSinkTest` | none | Paimon path/catalog streaming, sync incremental semantics and the full-refresh contract. |
| `ContainerRemoteStateTest` | `PUDDING_TESTCONTAINERS=true` | HDFS and MinIO S3A state, checkpoint inspection and restart. |
| `RedisStateBackendTest` | `PUDDING_TEST_REDIS_HOST`/`_PORT`, or `PUDDING_TESTCONTAINERS=true` | State journal, lock lease, fencing and SPI loading. |
| `EtcdStateBackendTest` | `PUDDING_TEST_ETCD_ENDPOINT`, or `PUDDING_TESTCONTAINERS=true` | The same contract through the etcd v3 HTTP gateway. |
| `RemoteFileSystemStateBackendTest` | `PUDDING_TEST_STATE_URI`, optional `PUDDING_TEST_STATE_CONF` | A real HDFS or S3A state directory plus checkpoint show/status/delete through the CLI. |

Run the JDBC containers directly:

```shell
PUDDING_TESTCONTAINERS=true \
  mvn -pl sink/jdbc-sink -am test -DwildcardSuites=org.pudding.sink.JdbcSinkPostgresContainerTest

PUDDING_TESTCONTAINERS=true \
  mvn -pl sink/mysql-sink -am test -DwildcardSuites=org.pudding.sink.MysqlSinkContainerTest
```

The native CDC suites use the same MySQL/PostgreSQL images and `mongo:7.0` by default. The
PostgreSQL container is started with `wal_level=logical`; the MongoDB container is a single-node
replica set. Override the images for restricted registries with `PUDDING_TEST_MYSQL_IMAGE`,
`PUDDING_TEST_POSTGRES_IMAGE` and `PUDDING_TEST_MONGO_IMAGE`.

The defaults are `postgres:16-alpine` and `mysql:8.0`. Override them for restricted registries:

```shell
export PUDDING_TEST_POSTGRES_IMAGE=registry.example.com/postgres:16-alpine
export PUDDING_TEST_MYSQL_IMAGE=registry.example.com/mysql:8.0
```

The MySQL suite requires a transactional engine because the replacement relies on rollback.
The generated table uses `ENGINE=InnoDB`; a server configured with a non-transactional default
still passes because the engine is explicit, but replacing MyISAM tables is not supported.

## Real object storage

MinIO covers the Hadoop S3A API path, Iceberg's S3FileIO data path, and Hudi/Paimon table data
and checkpoints in CI. OSS, Azure, GCS and COS runtime packing is covered by schema/class checks
and client jar-selection tests; provider-specific live endpoints remain environment-driven. Run
the lakehouse data-path suites against an existing disposable endpoint with:

```shell
PUDDING_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
PUDDING_TEST_S3_ACCESS_KEY=pudding \
PUDDING_TEST_S3_SECRET_KEY=pudding-secret \
  mvn -pl integration/lakehouse-object-store-test -am test \
    -DwildcardSuites=org.pudding.integration.HudiObjectStoreTest,org.pudding.integration.PaimonObjectStoreTest
```

The Iceberg data-layer test runs through its module:

```shell
PUDDING_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
PUDDING_TEST_S3_ACCESS_KEY=pudding \
PUDDING_TEST_S3_SECRET_KEY=pudding-secret \
  mvn -pl sink/iceberg-sink -am test -DwildcardSuites=org.pudding.sink.IcebergS3FileIOTest
```

To validate a real S3-compatible endpoint for state and checkpoints, point the
environment-driven state test at a disposable prefix and provide Hadoop client configuration:

```shell
PUDDING_TEST_STATE_URI=s3a://my-bucket/pudding-test \
PUDDING_TEST_STATE_CONF=/etc/pudding/s3a-conf \
  mvn -pl core test -DwildcardSuites=org.pudding.core.incremental.RemoteFileSystemStateBackendTest
```

`PUDDING_TEST_STATE_CONF` is a directory containing `core-site.xml`, `hdfs-site.xml` or
`s3a-site.xml`. Put credentials in that configuration through the Hadoop credential provider
chain, an instance profile or another environment-specific provider. Do not commit credentials
or bake them into images. The test creates a unique run directory and deletes it afterwards;
use a disposable bucket or prefix, not a shared production location.

The same configuration model applies to the checkpoint CLI through `--conf-file`, and to jobs
through their Spark Hadoop configuration. Real S3 is not part of CI because it requires
account-specific credentials and a bucket.
