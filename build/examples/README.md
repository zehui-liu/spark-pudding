# Job examples

These files are complete, runnable job configurations for the current mode model. Every example
declares `job.mode` and exactly one sink.

`${SPARK_HOME}` and `${PUDDING_HOME}` are placeholders, not environment expansion. Replace them
before submission (for example with `envsubst`) or edit the file. Credentials use
`${secret:env/NAME}` references, so keep the referenced variables in the submission environment
and never commit their values.

## Batch reads

| File | Source | Notes |
| --- | --- | --- |
| `read_csv_example.yaml/.json/.conf` | `file-common-source` | Minimal CSV to console job in all three configuration formats |
| `read_hudi_example.json` | `hudi-source` | Path-mode incremental read |
| `read_hudi_catalog_example.yaml` | `hudi-source` | Hudi session-catalog read |
| `read_iceberg_example.yaml/.json` | `iceberg-source` | Iceberg catalog read |
| `read_paimon_example.yaml` | `paimon-source` | Paimon filesystem-catalog read |
| `read_mongodb_example.yaml` | `mongodb-source` | MongoDB connection and collection read |
| `read_kafka_connection_example.yaml` | `kafka-source` | Bounded Kafka read through a named connection |
| `read_kafka_avro_example.yaml` | `kafka-source` | Confluent Schema Registry Avro read (optional runtime) |
| `read_gravitino_example.yaml` | `iceberg-source` | Gravitino catalog resolution |

## Batch writes

| File | Sink | Notes |
| --- | --- | --- |
| `write_hudi_catalog_example.yaml` | `hudi-sink` | Hudi catalog upsert |
| `write_paimon_example.yaml` | `paimon-sink` | Paimon primary-key table creation |
| `write_kafka_example.yaml` | `kafka-sink` | Kafka producer options through a connection |
| `write_mongodb_example.yaml` | `mongodb-sink` | MongoDB update/upsert |
| `write_elasticsearch_example.yaml` | `elasticsearch-sink` | Elasticsearch index write |

## Sync incremental

| File | Notes |
| --- | --- |
| `incremental_hudi_to_paimon.yaml` | Hudi CDC to Paimon with the incremental cursor journal |
| `incremental_jdbc_to_jdbc.yaml` | JDBC cursor upsert to another JDBC table |

## Streaming

| File | Notes |
| --- | --- |
| `stream_kafka_to_hudi.yaml` | Kafka CDC to Hudi upsert/delete |
| `stream_kafka_to_paimon.yaml` | Kafka CDC to Paimon `_row_kind_` |
| `stream_kafka_to_iceberg.yaml` | Kafka CDC to Iceberg `MERGE` |
| `stream_mysql_cdc_to_paimon.yaml` | Native MySQL binlog source to Paimon |
| `stream_dedup.yaml` | Event-time deduplication with a watermark |
| `stream_window_aggregate.yaml` | Windowed aggregation to append output |
| `stream_window_update.yaml` | Windowed aggregation to a primary-key update sink |
| `stream_stream_join.yaml` | Inner stream-stream join with watermarks |

## Maintenance

`maintenance/` contains Spark catalog property files for the table-maintenance CLI. See
[table-maintenance.md](../doc/table-maintenance.md).
