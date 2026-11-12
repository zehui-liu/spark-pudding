# Hudi sink

`hudi-sink` writes Apache Hudi tables in two modes:

- **catalog mode** creates or updates a table through a `hudi` catalog (`saveAsTable`);
- **path mode** writes to a Hudi table location with the Hudi Spark data source.

Both COPY_ON_WRITE and MERGE_ON_READ tables are supported. A Hudi catalog installs
`HoodieCatalog`, `HoodieSparkSessionExtension` and the Kryo serializer/registrar before the
Spark session starts.

## Catalog mode

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

sink:
  - id: write_orders
    type: hudi-sink
    catalog: hudi_lake
    config:
      table: sales.orders
      operation: upsert
      recordKeyFields: [id]
      partitionBy: [dt]
      preCombineField: ts
      tableType: MERGE_ON_READ
      mode: append
    dependency: source-1
```

Hudi's catalog is the Spark session catalog. `table` can be a plain table name or
`database.table`. Catalog mode writes with `saveAsTable`, which also registers the table in the Hive metastore
when Hive support is enabled. The `hudi` catalog enables it by default; set the catalog property
`enableHiveSupport: false` to disable it. The target database must already exist; the sink does
not create it.

## Path mode

```yaml
sink:
  - id: write_orders
    type: hudi-sink
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      operation: bulk_insert
      tableType: COPY_ON_WRITE
      recordKeyFields: [id]
      partitionPathField: dt
      mode: append
    dependency: source-1
```

## Incremental writes

When the source selects incremental semantics, the sink applies the same semantic automatically;
no sink setting is required:

- `append` writes with operation `insert` and records the incremental transaction id in
  `HoodieCommitMetadata` under `_pudding.transaction.id`, plus the source id and fencing token
  under `_pudding.source.id`/`_pudding.fencing.token`. A replay with the same transaction id is
  skipped, and a writer whose fence is not newer is rejected (an equal fence is accepted only
  for that already-committed transaction). A custom
  `hoodie.datasource.write.commitmeta.key.prefix` other than `_` is rejected because it would
  hide these markers. The same metadata is recorded for upsert and CDC commits.
- `upsert` writes with operation `upsert` and uses the source `keyFields` as record keys.
- `cdc` validates the operation column (`I`, `U`, `D`), applies deletes first and then upserts,
  and drops the operation column.

If the source provides change ordering (Hudi and Iceberg CDC do), the sink keeps only the last
operation per key in the batch and removes the ordering columns before writing. A batch
`incremental` write cannot run inside a streaming query; use the streaming CDC configuration
below. The target table can be created on the first batch write when the table path/catalog
configuration is complete. See
[incremental-sync.md](../../build/doc/incremental-sync.md).

## Configuration

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | string | required | Table name. The last component is the default `hudiTableName`. |
| `tablePath` | string | — | Hudi table location. Required when no `hudi` catalog is referenced. |
| `operation` | string | `insert` / `upsert` | `insert`, `upsert`, `bulk_insert` (`bulkinsert`), `delete`, `insert_overwrite`, `insert_overwrite_table`, `delete_partition` or `bootstrap`. |
| `tableType` | string | `COPY_ON_WRITE` | `COPY_ON_WRITE` (`cow`) or `MERGE_ON_READ` (`mor`). |
| `recordKeyFields` | string or list | — | Record key columns. Required for `upsert` and `delete`. |
| `partitionPathField` | string | `partitionBy` | Hudi partition path field(s). |
| `partitionBy` | string or list | — | Spark partition columns; also used as the default partition path fields. |
| `preCombineField` | string | — | Hudi pre-combine field for upserts. |
| `hudiTableName` | string | last part of `table` | Hudi table name written as `hoodie.table.name`. |
| `hiveSyncEnabled` | boolean | — | Enables Hudi's Hive sync. |
| `hiveSyncMode` | string | — | Hive sync mode, for example `hms` or `jdbc`. |
| `hiveDatabase` | string | — | Hive database for Hudi sync. |
| `hiveTableName` | string | — | Hive table name for Hudi sync. |
| `hiveMetastoreUris` | string | — | Hive metastore URI for Hudi sync. |
| `options` | map | — | Additional Hudi write options. |
| `mode` | string | `append` | Spark save mode: `append`, `overwrite`, `ignore`, `error` or `errorifexists`. |
| `isStreaming` / `streamWrite` / `streamWriter` | boolean | `false` | Uses `writeStream`. |
| `checkpointLocation` | string | — | Required for streaming writes. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `outputMode` | string | `append` | Streaming output mode: `append` or `update`. `update` requires `recordKeyFields` and writes each changed row as a Hudi upsert. |
| `cdcOpColumn` | string | — | Enables streaming CDC handling using this operation column (`I`/`U`/`D`). |
| `cdcOrderFields` | string or list | Kafka/Debezium metadata | Optional ordering columns for de-duplicating a CDC micro-batch. |

When `operation` is omitted, record keys select the default: `upsert` when `recordKeyFields` is
set and `insert` otherwise. Upsert and delete require `recordKeyFields`; `delete_partition` uses
the incoming partition values and does not require record keys.

`partitionBy` is passed to Spark so the table is partitioned; when `partitionPathField` is not
set, the same columns are used as Hudi partition path fields. `options` are applied first, then
the operation, table type, table name, record keys and pre-combine field are derived from the
explicit configuration keys above, so those keys override the same Hudi options. The sink also
writes the SQL short properties `primaryKey`, `type` and `preCombineField` so a Hudi catalog can
create the table; path-mode writes include them as well.

### Operations

| Operation | Use case |
| --- | --- |
| `insert` | Append records without matching keys. |
| `upsert` | Insert new keys and update existing keys. Requires record keys. |
| `bulk_insert` | Fast load for new tables; no index lookup. |
| `delete` | Delete records by record key. |
| `insert_overwrite` | Replace partitions touched by the incoming data. |
| `insert_overwrite_table` | Replace the whole table. |
| `delete_partition` | Delete partitions selected by the incoming data. |
| `bootstrap` | Bootstrap an existing table into Hudi. |

### Full refresh

`sync` with `strategy: full` uses Hudi's `insert_overwrite_table` operation, which writes a
replace commit. Readers see either the previous snapshot or the replacement, so Hudi no longer
requires `allowNonAtomicReplace` for a full refresh. Both COPY_ON_WRITE and MERGE_ON_READ tables
are supported. In catalog mode the sink resolves the table's base path and writes the replace
commit through the path writer, so the Spark catalog entry stays valid.

### Streaming

Streaming writes accept `tablePath` or a Hudi catalog table. In catalog mode the sink resolves
the table's base path and reuses the native Hudi path writer; the job does not need to repeat
`tablePath` in its config. A checkpoint is mandatory. `outputMode: append` accepts new rows;
`outputMode: update` requires a pre-created table and `recordKeyFields` and writes each changed
row as a Hudi upsert:

```yaml
config:
  table: orders
  tablePath: hdfs://namenode:8020/hudi/orders
  operation: upsert
  tableType: MERGE_ON_READ
  recordKeyFields: [id]
  preCombineField: ts
  partitionPathField: dt
  checkpointLocation: hdfs://namenode:8020/checkpoints/orders
  outputMode: append
```

For a stateful aggregation with `outputMode: update`, keep the record keys in the transform
output and pre-create the table:

```yaml
config:
  table: orders
  tablePath: hdfs://namenode:8020/hudi/orders
  operation: upsert
  recordKeyFields: [order_city, window_start, window_end]
  checkpointLocation: hdfs://namenode:8020/checkpoints/order-windows
  outputMode: update
```

For CDC, set `cdcOpColumn` and `recordKeyFields`, and pre-create the Hudi table. CDC uses
`outputMode: append` because the operation column distinguishes deletes and updates:

```yaml
config:
  table: orders
  tablePath: hdfs://namenode:8020/hudi/orders
  operation: upsert
  recordKeyFields: [id]
  cdcOpColumn: __op
  cdcOrderFields: [_kafka_timestamp, _kafka_partition, _kafka_offset]
  checkpointLocation: hdfs://namenode:8020/checkpoints/orders
  outputMode: append
```

Each micro-batch is normalized and de-duplicated by key. Deletes and inserts/updates are applied
through the batch Hudi delete/upsert writers inside `foreachBatch`; this is at-least-once with an
idempotent final state. The sink waits for the streaming query to terminate, matching the batch
job lifecycle.
