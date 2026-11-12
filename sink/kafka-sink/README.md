# Kafka sink

`kafka-sink` writes a DataFrame through Spark's Kafka connector. Batch and structured streaming
writes are supported.

Spark's Kafka sink expects these columns:

- `value` (required): string or binary;
- `key` (optional): string or binary;
- `topic` (required when no fixed `topic` is configured): string;
- `partition` (optional): integer;
- `headers` (optional): the Spark Kafka headers array.

## Configuration

```yaml
connection:
  - name: kafka-prod
    type: kafka
    endpoint: broker-1:9092,broker-2:9092
    auth:
      mechanism: SCRAM-SHA-512
      username: pudding
      password: ${secret:env/KAFKA_PASSWORD}
    properties:
      security.protocol: SASL_SSL

sink:
  - type: kafka-sink
    id: sink-1
    connection: kafka-prod
    config:
      topic: orders
      valueColumn: payload
      keyColumn: order_id
      mode: append
      options:
        acks: all
    dependency: source-1
```

Without a connection, `kafkaBootstrapServers` is accepted and mapped to
`kafka.bootstrap.servers`.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `topic` | string | — | Fixed destination topic. If omitted, the DataFrame must contain a `topic` column. |
| `valueColumn` | string | `value` | Column selected and renamed to `value`. |
| `keyColumn` | string | `key` if present | Column selected and renamed to `key`. |
| `options` | map | — | Additional Kafka producer options. |
| `mode` | string | `append` | `append`, `error` or `errorifexists`; Kafka does not support overwrite or ignore. |
| `isStreaming` / `streamWrite` / `streamWriter` | boolean | `false` | Uses `writeStream`. |
| `checkpointLocation` | string | — | Required for streaming writes. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `outputMode` | string | `append` | Streaming output mode; `append` is supported. |

`valueColumn` and `keyColumn` must resolve to string or binary columns. Extra DataFrame columns
are dropped. When `topic` is configured, a DataFrame `topic` column is ignored.

## Runtime

The Kafka connector runtime is packaged in `kafka-runtime` and is selected by the client. A direct
`spark-submit` invocation must add that jar to `--jars`.
