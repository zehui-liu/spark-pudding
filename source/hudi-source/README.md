# Hudi source

`hudi-source` reads Apache Hudi tables. A pipeline can use either a Hudi catalog or a table
path:

| Mode | When to use | Required configuration |
| --- | --- | --- |
| catalog | A `hudi` catalog is configured for the job | `table` |
| path | The table location is known, or no catalog is available | `table`, `tablePath` |

A Hudi catalog installs `HoodieCatalog`, `HoodieSparkSessionExtension` and the Kryo
serializer/registrar into the Spark session. See the Hudi section of the
[main README](../../README.md#hudi-paimon-and-iceberg) for the catalog definition.

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

source:
  - id: orders
    type: hudi-source
    catalog: hudi_lake
    config:
      table: sales.orders
      filterExpr: "dt >= '2026-01-01'"
```

Hudi's catalog is the Spark session catalog, so `table` is resolved as
`database.table` (or a plain table name in the default database).

Catalog mode uses `spark.read.table` / `spark.readStream.table`. As a consequence, `queryType`,
time-travel settings and `options` are only applied in path mode; they are ignored with a warning
when a `tablePath` is not configured. `filterExpr`, `numPartitions` and the schema conversion
options are applied after the table is read in both modes.

## Path mode

```yaml
source:
  - id: orders
    type: hudi-source
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      queryType: read_optimized
      filterExpr: "dt >= '2026-01-01'"
      options:
        hoodie.datasource.read.use.newer.timeline: "true"
```

Path mode passes the options to Hudi's Spark data source. `table` is also passed as
`hoodie.table.name`; use the table name without a database prefix here.

## Incremental reads

Path mode exposes the unified incremental contract. Use `incremental.semantics: upsert` for
Hudi's incremental latest-state query, or `cdc` for Hudi's CDC relation:

```yaml
source:
  - id: orders
    type: hudi-source
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      incremental:
        semantics: cdc
        keyFields: [id]
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
```

`cdc` requires both table properties:

```properties
hoodie.table.cdc.enabled=true
hoodie.table.cdc.supplemental.logging.mode=DATA_BEFORE_AFTER
```

The source flattens Hudi's `before`/`after` JSON images into ordinary rows and emits canonical
`I`, `U`, `D` values in `opColumn`. The source also exposes `__pudding_cdc_commit_time` as an
ordering column; CDC sinks use it to keep the last operation per key and then remove it.

Hudi uses two image encodings. CDC-log images are Avro JSON and are decoded losslessly. Base-file
and replace-commit images (for example a `bulk_insert` or an insert-only commit) use Hudi's
InternalRow JSON, where a DECIMAL is stored only as a lossy zero/non-zero marker. The source
fails the read for those DECIMAL values instead of writing a wrong number; use a source that
emits CDC-log images or read the snapshot when exact DECIMAL values are required. Binary images
are decoded from base64 in the InternalRow encoding. For tables with
`hoodie.populate.meta.fields=false`, the two encodings can not be distinguished for binary data,
so binary CDC is rejected; keep the default metadata fields enabled.

Incremental reads accept either `tablePath` or a Hudi catalog table. With a catalog reference,
the source resolves the table's storage location and then uses the same path-based incremental
reader. The cursor is a Hudi commit time. Incremental jobs require incremental state
(`statePath` or `state`); the cursor is committed after every sink succeeds. See
[incremental-sync.md](../../build/doc/incremental-sync.md) for the shared semantics, cursor and
failure behavior.

`upsert` reads the latest state of changed keys from Hudi's incremental scan; Hudi does not emit
records for keys deleted in the range, so use `cdc` when deletes must be mirrored. The raw
`queryType: incremental` / `queryType: cdc` options below are independent reads for batch
pipelines. The unified incremental block is the one that validates and applies sink semantics.

## Configuration

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | string | required | Hudi table name. In path mode it is also used as `hoodie.table.name`. |
| `tablePath` | string | — | Table location. Required when no `hudi` catalog is referenced. |
| `incremental` | map | — | Unified incremental read configuration. See [incremental-sync.md](../../build/doc/incremental-sync.md). |
| `queryType` | string | `snapshot` | `snapshot`, `incremental`, `read_optimized` (`readoptimized`) or `cdc`. Path mode only. |
| `beginTime` | string | — | Incremental start instant, for example `20260101000000`. Required for `incremental`; optional for `cdc`. |
| `endTime` | string | — | Incremental end instant. Required for `incremental`; optional for `cdc`. |
| `asOfInstant` | string | — | Time travel instant. Path mode only. |
| `schemaEvolution` | boolean | Hudi default | Enables Hudi schema evolution for the read. Path mode only. |
| `extractPartitionValues` | boolean | Hudi default | Extracts partition values from the partition path. Path mode only. |
| `streamRead` / `isStreaming` | boolean | `false` | Uses `readStream` instead of `read`. |
| `filterExpr` | string | — | Spark SQL condition applied after the source is read. |
| `numPartitions` | integer | — | Repartitions the resulting DataFrame. Must be greater than zero. |
| `forceConvertSchema` | boolean | `false` | Selects and casts columns using `schema`. |
| `schema` | map | — | Column name to Spark SQL type. Required when `forceConvertSchema` is `true`. |
| `options` | map | — | Additional Hudi read options. Path mode only. |

### Query types

| `queryType` | Hudi query | Notes |
| --- | --- | --- |
| `snapshot` | snapshot | Latest snapshot of the table. |
| `incremental` | incremental | Reads changes between `beginTime` and `endTime`; both are required. |
| `read_optimized` | read optimized | Reads only parquet base files. `readoptimized` is accepted as an alias. |
| `cdc` | incremental CDC | Sets `hoodie.datasource.query.incremental.format=cdc`; begin/end instants are optional. |

### Schema conversion

With `forceConvertSchema: true`, the source projects the DataFrame to the configured `schema` and
casts each column. Columns that are not listed are dropped, and the configured type wins over the
table schema. This is useful when a downstream sink needs a stable schema:

```yaml
config:
  table: orders
  tablePath: hdfs://namenode:8020/hudi/orders
  forceConvertSchema: true
  schema:
    id: string
    name: string
    dt: date
```

### Streaming reads

Set `streamRead: true` (or `isStreaming: true`) to use a streaming DataFrame. Hudi's incremental
streaming read still uses the options above; for example:

```yaml
config:
  table: orders
  tablePath: hdfs://namenode:8020/hudi/orders
  queryType: incremental
  beginTime: "20260101000000"
  streamRead: true
```

The sink decides how the stream is written. See [hudi-sink](../../sink/hudi-sink/README.md) for
Hudi's write options.
