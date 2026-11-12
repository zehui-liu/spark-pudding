# Iceberg sink

`iceberg-sink` writes a DataFrame to an Apache Iceberg table through the configured Iceberg
catalog. It creates a missing table on the first write and uses the catalog from the `catalog:`
reference when one is provided.

## Configuration

```yaml
sink:
  - type: iceberg-sink
    id: sink-1
    catalog: lake
    config:
      table: sales.orders
      mode: append
      partitionBy: [dt]
    dependency: source-1
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | string | required | Iceberg table identifier. |
| `mode` | string | `append` | `append`, `overwrite`, `ignore`, `error` or `errorifexists`. Streaming requires `append`. |
| `partitionBy` | string or list | — | Partition columns, used only when the table is created. |
| `isStreaming` / `streamWrite` / `streamWriter` | boolean | `false` | Uses `writeStream`. |
| `checkpointLocation` | string | — | Required for streaming writes. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `outputMode` | string | `append` | Streaming output mode: `append` or `update`. `update` requires `recordKeyFields` and runs an idempotent `MERGE INTO` per micro-batch. |
| `operation` | string | `append` | Streaming operation with `outputMode: append`: `append` or `upsert`. `upsert` requires `recordKeyFields` and ordering fields, and accepts full rows without an operation column. |
| `cdcOpColumn` | string | — | Enables streaming CDC through `foreachBatch` and `MERGE INTO`; takes precedence over `operation`. |
| `recordKeyFields` | string or list | — | Key columns for streaming CDC/upsert. |
| `cdcOrderFields` | string or list | Kafka/Debezium metadata | Ordering columns for CDC/upsert deduplication. |

The Iceberg catalog must be installed on the Spark session before the sink runs. The client adds
`iceberg-runtime` automatically; a direct `spark-submit` invocation must add that jar to `--jars`.

## Streaming

Streaming requires an Iceberg catalog reference, an existing table and a checkpoint. Append uses
Iceberg's native streaming append; full-row upsert uses `operation: upsert` with
`recordKeyFields` and ordering fields, while delete-aware CDC uses `cdcOpColumn`. Both use
`foreachBatch` plus Spark SQL `MERGE INTO`.

With `outputMode: update` and `recordKeyFields`, the sink merges each changed stateful row
without requiring ordering fields:

```yaml
config:
  table: analytics.order_window_count
  outputMode: update
  recordKeyFields: [window_start, window_end, order_city]
  checkpointLocation: hdfs://namenode:8020/checkpoints/order-windows
```

CDC remains append-only because deletes are carried by the operation column:

```yaml
job:
  mode: streaming
  trigger: "processing-time: 30 seconds"
catalog:
  - name: lake
    type: iceberg
    properties:
      type: hadoop
      warehouse: hdfs://namenode:8020/iceberg
sink:
  - type: iceberg-sink
    id: orders
    catalog: lake
    config:
      table: sales.orders
      outputMode: append
      checkpointLocation: hdfs://namenode:8020/checkpoints/orders
      cdcOpColumn: __op
      recordKeyFields: [id]
      cdcOrderFields: [_kafka_timestamp, _kafka_partition, _kafka_offset]
```

CDC rows must be full after-images for updates; delete rows need the key columns. Delete and
upsert/insert are separate idempotent MERGE statements inside one checkpointed micro-batch.
Exactly-once delivery is checkpoint-bound; Kafka producer duplicates are not removed. See
[incremental-sync.md](../../build/doc/incremental-sync.md) for the distinction from the batch
`incremental` protocol.

## Full refresh

`sync` with `strategy: full` replaces the table through the Iceberg writer while preserving the
existing schema and partition spec. The replacement is atomic: readers see either the old or the
new snapshot.

## Incremental writes

`iceberg-sink` supports `append`, `upsert` and `cdc` for `sync` + `incremental`:

- `append` stores the transaction id, source id and fencing token in the Iceberg snapshot summary
  under `pudding.transaction.id`, `pudding.source.id` and `pudding.fencing.token`. A replay with
  the same transaction id is skipped and a writer whose fence is not newer is rejected (an equal
  fence is accepted only for that already-committed transaction).
- `upsert` merges complete rows by `recordKeyFields`. Duplicate keys are reduced using the
  source ordering when available; otherwise the batch is de-duplicated by key.
- `cdc` applies the operation column as deletes plus upserts. The update/insert rows must be full
  after-images; delete rows need only the key columns.
- A missing target table is created from the batch on its first incremental write.
- Upsert and CDC are idempotent by key, so a replay of a committed batch is safe. They do not
  write the append summary marker or a fencing token; fencing applies to append transactions.

Upsert and CDC require
`org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` for Spark SQL `MERGE`. See
[incremental-sync.md](../../build/doc/incremental-sync.md). Streaming CDC/upsert is configured
with `cdcOpColumn` and `recordKeyFields` as described above.
