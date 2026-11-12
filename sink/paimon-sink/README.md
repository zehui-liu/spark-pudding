# Paimon sink

`paimon-sink` writes Apache Paimon tables in batch or streaming mode. A table can be created
through a `paimon` catalog or written by an explicit path.

## Catalog mode

Catalog mode creates the table with `saveAsTable` and delegates table creation to Paimon.
Primary keys, partitions, buckets and table properties are passed through. The target database
must already exist; the sink does not create it (Paimon creates its `default` database lazily,
but other databases must be created first).

Streaming catalog writes require an existing table. Full-row `update` and CDC use Paimon's
direct primary-key writer and need no `path`. Paimon 1.3.2 on Spark 3.4 has no native
catalog-based `toTable` streaming sink, so `append` resolves the catalog table's storage
location and uses the path writer. The job does not need to repeat that path in its config.

```yaml
catalog:
  - name: paimon_lake
    type: paimon
    properties:
      warehouse: hdfs://namenode:8020/paimon/warehouse
      metastore: filesystem

sink:
  - id: write_orders
    type: paimon-sink
    catalog: paimon_lake
    config:
      table: sales.orders
      primaryKey: [id, dt]
      partition: [dt]
      bucket: 4
      mode: append
      tableOptions:
        file.format: parquet
    dependency: source-1
```

A Paimon primary-key table requires every partition column to be part of the primary key. In the
example above `dt` is both a partition and a member of the primary key.

`table` is qualified with the catalog name: `orders` becomes
`<sparkCatalogName>.default.orders`, `sales.orders` becomes
`<sparkCatalogName>.sales.orders`, and a three-part name is used as written. A generic/session
Paimon catalog keeps the table name unchanged.

## Path mode

```yaml
sink:
  - id: write_orders
    type: paimon-sink
    config:
      path: hdfs://namenode:8020/paimon/warehouse/sales.db/orders
      mode: append
    dependency: source-1
```

Batch path mode can create a table from the writer options. Streaming, updates and CDC require
an existing table at that location; create it first through catalog mode, SQL DDL or another
batch job with the desired primary keys, partitions and table properties.

## Incremental writes

The sink applies the source semantic automatically:

- `append` and `upsert` append the batch (Paimon merges primary-key rows). Incremental writes
  prefix the Paimon commit user with
  `<transaction-id>-f<fencing-token>-s<source-hash>_`; a replay with the same transaction id is
  skipped by scanning the snapshot timeline, and a writer whose fence is not newer is rejected
  (an equal fence is accepted only for that already-committed transaction). A missing append
  table is created with an empty commit before the transaction write.
- `cdc` validates the operation column, keeps the last operation per key when the source provides
  ordering, and writes `_row_kind_` through Paimon's native writer. Deletes are supported.

Requirements and limits:

- `upsert` and `cdc` require an existing target table, and its primary keys must match the source
  `keyFields`.
- A batch `incremental` write cannot run inside a streaming query; use the streaming CDC configuration below.
- CDC uses Paimon's direct writer, so sink-level table properties are not applied; configure
  them on the target table.
- The target table should use a changelog-compatible primary-key table. See
  [incremental-sync.md](../../build/doc/incremental-sync.md).

## Configuration

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | string | — | Table name. Required when no `path` is set. |
| `path` | string | — | Table location. Takes precedence over `table`. |
| `primaryKey` | string or list | — | Paimon `primary-key` columns. |
| `partition` / `partitionBy` | string or list | — | Partition columns passed to Spark's writer. |
| `bucket` | integer/string | Paimon default | Paimon `bucket` table property. |
| `options` | map | — | Options passed to the Paimon Spark writer. |
| `tableOptions` | map | — | Additional table properties; merged with `options`. |
| `mode` | string | `append` | Spark save mode: `append`, `overwrite`, `ignore`, `error` or `errorifexists`. |
| `isStreaming` / `streamWrite` / `streamWriter` | boolean | `false` | Uses `writeStream`. |
| `checkpointLocation` | string | — | Required for streaming writes. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `outputMode` | string | `append` | Streaming output mode: `append` or `update`. `update` requires an existing primary-key table and merges changed rows through PaimonSparkWriter. |
| `cdcOpColumn` | string | — | Enables streaming CDC handling using this operation column (`I`/`U`/`D`). |
| `cdcOrderFields` | string or list | Kafka/Debezium metadata | Optional ordering columns for de-duplicating a CDC micro-batch. |

`options` and `tableOptions` are both passed to the writer; `tableOptions` is convenient for
Paimon table properties such as `file.format`, `bucket-key` or `changelog-producer`. The explicit
`primaryKey` and `bucket` keys are translated to the `primary-key` and `bucket` options, while
`partition`/`partitionBy` drive Spark's `partitionBy`. All of them can also be expressed directly
under `options`/`tableOptions`; the explicit keys above take precedence.

### Streaming

Streaming writes require a checkpoint and an existing target table in either catalog or path
mode. `outputMode: update` requires a primary-key table whose primary keys are present in the
stateful transform output; each micro-batch is written as full rows and Paimon merges them.
CDC remains `outputMode: append` because deletes are carried by the operation column. For
catalog `append`, the sink resolves `location()` and uses the native path streaming writer; this
is the Paimon 1.3.2 + Spark 3.4 compatibility path for the missing catalog `toTable` streaming.

```yaml
config:
  table: sales.orders
  path: hdfs://namenode:8020/paimon/default.db/orders
  primaryKey: [id, dt]
  partition: [dt]
  checkpointLocation: hdfs://namenode:8020/checkpoints/orders
  outputMode: append
```

For CDC, add `cdcOpColumn`; the sink maps `I`/`U`/`D` to Paimon `_row_kind_` values and writes
through `PaimonSparkWriter` inside `foreachBatch`. The table must be an existing primary-key
table:

```yaml
config:
  path: hdfs://namenode:8020/paimon/default.db/orders
  primaryKey: [id, dt]
  partition: [dt]
  cdcOpColumn: __op
  cdcOrderFields: [_kafka_timestamp, _kafka_partition, _kafka_offset]
  checkpointLocation: hdfs://namenode:8020/checkpoints/orders
  outputMode: append
```

Each micro-batch is normalized and de-duplicated by key; CDC is at-least-once with an idempotent
final state. The sink waits for the streaming query to terminate, matching the batch job
lifecycle.
