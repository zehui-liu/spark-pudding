# Iceberg Source Configuration

The Iceberg source supports reading data from Apache Iceberg tables with various options.

With a named catalog, the catalog settings are installed once for the session and the source only
selects the table:

```yaml
catalog:
  - name: lake
    type: iceberg
    properties:
      type: hadoop
      warehouse: s3://bucket/warehouse

source:
  - type: iceberg-source
    id: orders
    catalog: lake
    config:
      table: sales.orders
```

A named catalog requires `table` to be `database.table` (or a fully qualified
`catalog.database.table`). When the catalog's Spark name is `spark_catalog`, it is installed as
the session catalog and requires a `type` property. The legacy inline form is still supported,
and the source-level `catalog` key inside `config` remains available for existing jobs:

```json
{
  "source": [
    {
      "type": "iceberg-source",
      "describe": "read iceberg table",
      "id": "source-1",
      "config": {
        "table": "database.table_name",
        "catalog": "spark_catalog",
        "filterExpr": "date >= '2023-01-01'",
        "snapshotId": 123456789,
        "forceConvertSchema": true,
        "schema": {
          "column1": "string",
          "column2": "int",
          "column3": "double"
        }
      }
    }
  ]
}
```

## Supported configuration options:
- `table`: The table name in the format `database.table_name` or full table identifier
- `path`: Alternative to table, specify the direct path to the Iceberg table
- `catalog` (inside `config`): Legacy catalog name (default: "spark_catalog"). Prefer the
  top-level `catalog:` reference.
- `filterExpr`: SQL filter expression to apply to the data
- `snapshotId`: Read from a specific snapshot ID
- `asOfTimestamp`: Read from a specific timestamp
- `branch`: Read from a specific branch
- `tag`: Read from a specific tag
- `options`: Additional Iceberg options as key-value pairs
- `streamRead` / `isStreaming`: Use Iceberg's Structured Streaming snapshot reader
- `forceConvertSchema`: Whether to force convert the schema (default: false)
- `schema`: Schema definition for forceConvertSchema (required if forceConvertSchema is true)
- `incremental`: Unified incremental read configuration. See
  [incremental-sync.md](../../build/doc/incremental-sync.md).

## Streaming reads

`streamRead: true` (or a streaming job context) creates an Iceberg Structured Streaming
DataFrame. Iceberg 1.5 streams appended snapshots; it is not a row-level changelog:

- `branch` may select a branch, but `snapshotId`, `asOfTimestamp` and `tag` are batch-only and are
  rejected in streaming mode.
- Use `options` for Iceberg streaming settings such as `stream-from-timestamp`,
  `streaming-skip-overwrite-snapshots`, `streaming-skip-delete-snapshots`,
  `streaming-max-files-per-micro-batch` and `streaming-max-rows-per-micro-batch`.
- A downstream update/delete needs the `sync` incremental CDC path; snapshot streaming is meant
  for append-oriented consumers.

## Incremental reads

```yaml
source:
  - id: orders
    type: iceberg-source
    catalog: lake
    config:
      table: sales.orders
      incremental:
        semantics: cdc
        keyFields: [id]
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
```

- `append` reads snapshots from `start` (exclusive) to `end` (inclusive) with Iceberg's
  incremental append scan. The range must contain only append snapshots; overwrite, delete and
  replace snapshots cause validation to fail and suggest `cdc`.
- `cdc` reads the `table.changes` metadata table. It requires a catalog table; the `.changes`
  metadata table is not available in path mode. The source maps the commit snapshot to its
  snapshot sequence number, combines it with `_change_ordinal` and an insert-over-delete
  priority for deduplication, and normalizes `_change_type` to the canonical `__op` (`I`, `U`,
  `D`). Iceberg may encode an update as a delete/insert pair.
- `upsert` also reads the catalog changelog and returns the latest complete row per key. It
  removes `UPDATE_BEFORE` rows and change metadata. If the latest change for a key is a delete,
  the read fails and asks for `cdc` semantics; deletes are never silently dropped.

The cursor is an Iceberg snapshot id. It is committed to the configured incremental state only
after all sinks succeed. Snapshot and timestamp selectors above are time travel, not incremental reads.

## Runtime

The Iceberg connector runtime is packaged in `iceberg-runtime`. The client selects it
automatically; a direct `spark-submit` invocation must add the runtime jar to `--jars`.
