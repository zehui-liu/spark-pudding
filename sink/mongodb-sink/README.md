# MongoDB sink

`mongodb-sink` writes a DataFrame to a MongoDB collection through the official MongoDB Spark
connector. Batch and structured streaming writes are supported.

## Configuration

```yaml
connection:
  - name: mongo-prod
    type: mongodb
    endpoint: mongodb://mongo-1:27017,mongo-2:27017
    auth:
      username: pudding
      password: ${secret:env/MONGO_PASSWORD}
    properties:
      authSource: admin
      replicaSet: rs0

sink:
  - type: mongodb-sink
    id: sink-1
    connection: mongo-prod
    config:
      database: app
      collection: orders
      operation: update
      idFields: [id, dt]
      upsert: true
      mode: append
      options:
        maxBatchSize: 512
        writeConcern.w: majority
    dependency: source-1
```

Without a connection, an inline `uri` or `connectionUri` is accepted.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | string | required | Target database. |
| `collection` | string | required | Target collection. |
| `operation` | string | connector default (`replace`) | `insert`, `replace` or `update`. |
| `idField` / `idFields` | string or list | connector default (`_id`) | Fields used to identify a document; maps to `spark.mongodb.write.idFieldList`. `idField` wins when both are set. |
| `upsert` | boolean | connector default | Upsert matched documents for replace/update operations. |
| `options` | map | — | Additional `spark.mongodb.write.*` options, for example `maxBatchSize`, `ordered`, `writeConcern.w`. |
| `mode` | string | `append` | `append`, `overwrite`, `ignore`, `error` or `errorifexists`. |
| `isStreaming` / `streamWrite` / `streamWriter` | boolean | `false` | Uses `writeStream`. |
| `checkpointLocation` | string | — | Required for streaming writes. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `outputMode` | string | `append` | Streaming output mode; `append` is supported. |

Read-only options from a shared MongoDB connection (for example `spark.mongodb.read.*`) are not
forwarded to the writer. Put write settings in the sink `options` map.

## Runtime

The MongoDB runtime is packaged in `mongodb-runtime` and is selected by the client. A direct
`spark-submit` invocation must add that jar to `--jars`.
