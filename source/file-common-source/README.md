# File source

`file-common-source` reads CSV, text, JSON, Parquet or ORC files from a local filesystem or a
Hadoop-compatible filesystem. Batch reads accept one or more comma-separated paths.

## Configuration

```yaml
source:
  - type: file-common-source
    id: source-1
    config:
      path: hdfs:///data/orders.csv
      fileType: csv
      streamRead: false
      options:
        header: 'true'
        delimiter: ','
      forceConvertSchema: true
      schema:
        order_id: string
        amount: double
        dt: date
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | string | required | One or more comma-separated paths. |
| `fileType` | string | required | `csv`, `text`, `json`, `parquet` or `orc`. |
| `streamRead` | boolean | `false` | Reads new files as a streaming DataFrame. Exactly one path is allowed. |
| `options` | map | — | Spark reader options, for example `header`, `delimiter` or `mergeSchema`. |
| `forceConvertSchema` | boolean | `false` | Selects and casts columns using `schema`. |
| `schema` | map | — | Column name to Spark SQL type. Required for streaming CSV/JSON and for `forceConvertSchema`. |
| `textDelimiter` | string | `,` | Delimiter for `text` files when `forceConvertSchema` is enabled. |

Streaming CSV and JSON reads require an explicit `schema` because Spark cannot infer a streaming
schema. The source detects new files only; it does not process in-place file updates or deletes.
