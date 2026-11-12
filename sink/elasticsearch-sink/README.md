# Elasticsearch sink

`elasticsearch-sink` writes a DataFrame to Elasticsearch through the official Elasticsearch
Hadoop connector. Batch and structured streaming writes are supported.

## Configuration

```yaml
connection:
  - name: es-prod
    type: elasticsearch
    endpoint: https://es-1:9200,es-2:9200
    auth:
      username: elastic
      password: ${secret:env/ES_PASSWORD}
    properties:
      nodes.wan.only: "true"

sink:
  - type: elasticsearch-sink
    id: sink-1
    connection: es-prod
    config:
      esIndex: logs-2026
      operation: index
      idField: id
      mode: append
      options:
        es.batch.size.entries: 1000
    dependency: source-1
```

Without a connection, the legacy keys `esNodes`, `esPort`, `esUser` and `esPass` are accepted.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `esIndex` | string | required | Destination index or index pattern. |
| `operation` | string | connector default | `index`, `create`, `update`, `upsert` or `delete`; maps to `es.write.operation`. |
| `idField` | string | — | Document id field; maps to `es.mapping.id`. |
| `options` | map | — | Additional Elasticsearch writer options. |
| `mode` | string | `append` | `append`, `overwrite`, `ignore`, `error` or `errorifexists` for batch writes. |
| `isStreaming` / `streamWrite` / `streamWriter` | boolean | `false` | Uses `writeStream`. |
| `checkpointLocation` | string | — | Required for streaming writes. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `outputMode` | string | `append` | `append` or `update` for streaming writes. |

Connection options override sink-level `options` for connection keys. The destination index is
always taken from `esIndex`.

For `outputMode: update`, `idField` is required and `operation` must be unset or one of `index`,
`upsert` or `update`. The document id is what makes repeated update rows target the same
document.

## Runtime

The Elasticsearch runtime is packaged in `elasticsearch-runtime` and is selected by the client.
A direct `spark-submit` invocation must add that jar to `--jars`. No Elasticsearch cluster is
needed to validate the sink configuration, but an actual write requires a reachable cluster.
