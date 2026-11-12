# Paimon source

`paimon-source` reads Apache Paimon tables in batch or streaming mode. A table can be addressed
through a `paimon` catalog or by an explicit path.

| Mode | When to use | Required configuration |
| --- | --- | --- |
| catalog | A `paimon` catalog is configured for the job | `table` |
| path | The table location is known | `path` |

## Catalog mode

A named Paimon catalog is the recommended setup. The catalog can use a filesystem, a Hive
metastore or a JDBC metastore for its metadata:

```yaml
connection:
  - name: hms-prod
    type: hive-metastore
    endpoint: thrift://hms-1:9083,thrift://hms-2:9083

catalog:
  - name: paimon_lake
    type: paimon
    connection: hms-prod
    properties:
      warehouse: hdfs://namenode:8020/paimon/warehouse

source:
  - id: orders
    type: paimon-source
    catalog: paimon_lake
    config:
      table: sales.orders
      scanMode: latest-full
      filterExpr: "dt >= '2026-01-01'"
```

For a filesystem catalog no connection is needed:

```yaml
catalog:
  - name: paimon_lake
    type: paimon
    properties:
      warehouse: hdfs://namenode:8020/paimon/warehouse
      metastore: filesystem
```

`table` is qualified with the catalog name: `orders` becomes
`<sparkCatalogName>.default.orders`, `sales.orders` becomes
`<sparkCatalogName>.sales.orders`, and an already qualified three-part name is used as written.
A generic/session Paimon catalog (`sessionCatalog: true` or `mode: generic`) keeps the table name
unchanged.

## Path mode

```yaml
source:
  - id: orders
    type: paimon-source
    config:
      path: hdfs://namenode:8020/paimon/warehouse/sales.db/orders
      scanMode: latest-full
      options:
        scan.parallelism: "4"
```

Set `table` or `path`; when both are present, `path` takes precedence. A path read does not
require a Paimon catalog, but a catalog is still needed when `table` is used.

## Incremental reads

The unified incremental block supports append-only and CDC reads:

```yaml
source:
  - id: orders
    type: paimon-source
    catalog: paimon_lake
    config:
      table: sales.orders
      incremental:
        semantics: cdc
        keyFields: [id, dt]
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
```

- `append` requires an append-only table. The source reads `delta` snapshots between the cursor
  and the latest/end snapshot.
- `cdc` requires a primary-key table. `keyFields` must match the table primary keys. The source
  scans changelog files when a `changelog-producer` is configured, otherwise it computes the
  difference between snapshots. Paimon's audit log is used to expose row kinds; updates can
  therefore appear as `I` (upsert) instead of `U`. Deletes are `D`.
- The cursor is a Paimon snapshot id. It is committed to the configured incremental state only
after all sinks succeed.

Paimon's audit log does not expose change ordering. If a batch contains several changes for the
same key, use smaller ranges to make the final state deterministic. See
[incremental-sync.md](../../build/doc/incremental-sync.md) for the shared contract.

## Configuration

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | string | — | Paimon table name. Required when no `path` is set. |
| `path` | string | — | Table location. Takes precedence over `table`. |
| `incremental` | map | — | Unified incremental read configuration. See [incremental-sync.md](../../build/doc/incremental-sync.md). |
| `scanMode` | string | Paimon default | Passed as `scan.mode`. Common values: `latest-full`, `latest`, `compacted-full`, `from-snapshot`, `from-snapshot-full`, `from-timestamp`, `incremental`. |
| `snapshotId` | string/integer | — | Reads a specific snapshot (`scan.snapshot-id`). |
| `tagName` | string | — | Reads a tag (`scan.tag-name`). |
| `branch` | string | — | Reads a branch (`scan.branch`). |
| `timestampMillis` | string/integer | — | Reads the snapshot at a timestamp (`scan.timestamp-millis`). |
| `streamRead` / `isStreaming` | boolean | `false` | Uses `readStream` instead of `read`. |
| `filterExpr` | string | — | Spark SQL condition applied after the source is read. |
| `numPartitions` | integer | — | Repartitions the resulting DataFrame. Must be greater than zero. |
| `options` | map | — | Additional Paimon scan options. `options` are merged with the convenience keys above. |

### Streaming reads

Set `streamRead: true` (or `isStreaming: true`) to obtain a streaming DataFrame. Paimon's
snapshot/tag/branch options are still passed to the scan:

```yaml
config:
  table: sales.orders
  streamRead: true
  scanMode: latest-full
```

The source returns a streaming DataFrame; the job's sink must be able to consume it. See the
[Paimon sink documentation](../../sink/paimon-sink/README.md) for write options.
