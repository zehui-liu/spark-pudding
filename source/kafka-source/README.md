# kafka-source

`kafka-source` reads a bounded batch or a Structured Streaming query through Spark's Kafka
connector. It can keep the raw key/value strings, decode a JSON value schema, decode Confluent
Schema Registry Avro/Protobuf values, or normalize CDC operations from a generic `op` column or
a Debezium JSON envelope.

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

source:
  - type: kafka-source
    id: events
    connection: kafka-prod
    config:
      subscribe: order-events
      options:
        startingOffsets: earliest
```

Without a connection, `kafkaBootstrapServers` is accepted and mapped to
`kafka.bootstrap.servers`. Exactly one subscription mode is required: `subscribe`, `assign` or
`subscribePattern`. `assign` accepts Spark's JSON partition assignment, for example
`{"orders":[0,1]}`. The three modes are mutually exclusive.

### JSON values

```yaml
source:
  - type: kafka-source
    id: orders
    config:
      subscribe: orders
      valueFormat: json
      valueSchema:
        id: long
        name: string
        amount: decimal(12,2)
      parseMode: failfast
      includeMetadata: true
```

- `valueFormat`: `raw`/`string` (default), `json`, `avro` or `protobuf`.
- `valueSchema`: required for JSON, Avro and Protobuf. It is either a map of field names to Spark
  SQL types or a full DDL string such as `id BIGINT, name STRING`. Avro/Protobuf field values are
  matched to these columns by name; the source does not infer the Spark schema from the registry
  schema.
- `parseMode`: `failfast` (default), `dropMalformed`, or `permissive`.
- `includeMetadata`: when true, adds `_kafka_topic`, `_kafka_partition`, `_kafka_offset`,
  `_kafka_timestamp`, `_kafka_timestamp_type`, `_kafka_key`.
- `keepRawValue`: when true, keeps the original value as `_kafka_value` (a string for JSON,
  binary for Avro/Protobuf).
- `badRecordsPath`: batch-only quarantine path for malformed records with
  `parseMode: dropMalformed`. The bad records are written as JSON before being dropped and
  contain the raw key/value, Kafka metadata and `_error`.
- `includeCorruptRecord`: permissive-mode output column `_kafka_corrupt_record` containing the
  raw malformed value. It is useful for a separate streaming quarantine job; it can not be
  combined with `dropMalformed` or Debezium.
- `lagMetricsIntervalMs`: minimum interval between Kafka lag measurements, default `30000`.
  Streaming only; the value must be positive.

### Schema Registry Avro/Protobuf

Avro and Protobuf values use the Confluent wire format and Schema Registry. The optional
`kafka-schema-registry-runtime` jar supplies the decoder; the client selects it automatically
when `valueFormat` is `avro` or `protobuf`.

```yaml
source:
  - type: kafka-source
    id: orders
    connection: kafka-prod
    config:
      subscribe: orders
      valueFormat: avro
      valueSchema:
        id: long
        name: string
        amount: decimal(12,2)
      schemaRegistry:
        url: https://schema-registry.internal
        username: ${secret:env/SCHEMA_REGISTRY_USER}
        password: ${secret:env/SCHEMA_REGISTRY_PASSWORD}
        subject: orders-value       # optional; default is the Kafka topic
        properties:
          basic.auth.credentials.source: USER_INFO
```

- `schemaRegistry.url` is required; `schemaRegistryUrl` is accepted as a top-level shorthand.
- `schemaRegistry.subject` is optional and is the full registry subject name (for example
  `orders-value`). Without it, the subject is derived from the topic using the deserializer's
  configured subject strategy.
- `schemaRegistry.properties` is passed to the Confluent deserializer, including TLS and
  subject-name strategy settings.
- `schemaRegistry.username`/`password` set Confluent basic authentication. They must be supplied
  together; prefer secret references.
- Avro supports nested records, arrays, maps, logical date/timestamp/decimal values and unions.
  Protobuf supports scalar, enum, bytes, nested message, repeated and map fields, plus
  `google.protobuf.Timestamp`.
- `cdcFormat: op-column` works with Avro/Protobuf when the configured schema contains the
  operation field. Debezium's `before`/`after` envelope remains JSON-only.
- `badRecordsPath` and `includeCorruptRecord` remain JSON-only. A malformed Avro/Protobuf value
  fails the batch; route rejected records with a separate job if needed.

A direct `spark-submit` invocation must add both `kafka-runtime` and
`kafka-schema-registry-runtime` to `--jars`. The schema-registry runtime is shaded and does not
add its Avro/Protobuf/Confluent dependencies to a JSON-only Kafka job.

### Malformed records

For batch jobs, `badRecordsPath` writes rejected JSON records to a Hadoop-compatible path before
they are removed from the pipeline. For streaming jobs, use `parseMode: permissive` and
`includeCorruptRecord: true`, then route the non-null `_kafka_corrupt_record` column to a
separate quarantine job. The source does not start a second streaming query or send records to
Kafka automatically.

## CDC

`cdcFormat: op-column` adds the operation column plus the ordering metadata needed by CDC sinks.
It works with `json`, `avro` and `protobuf` values when the configured schema contains the
operation field. `cdcFormat: debezium` requires JSON and a `before`/`after` envelope. Setting
`cdcFormat` with a raw value is rejected.

### Generic operation column

```yaml
config:
  subscribe: orders
  valueFormat: json
  valueSchema:
    id: long
    name: string
    dt: string
    op: string
  cdcFormat: op-column
  opField: op          # input field, default is opColumn
  opColumn: __op       # output field, default __op
```

The source accepts `I/U/D` case-insensitively and the common `c/i/u/d/r` spellings. An unknown
value fails the query rather than being dropped.

### Debezium

```yaml
config:
  subscribe: orders
  valueFormat: json
  valueSchema:
    id: long
    name: string
    dt: string
  cdcFormat: debezium
  opColumn: __op
```

`valueSchema` describes the `before`/`after` row. The source maps `c/r` to `I`, `u` to `U`,
`d` to `D`, uses `after` for inserts/updates and `before` for deletes, drops null tombstones, and
exposes `_cdc_timestamp` from the envelope `ts_ms`. Additional Kafka metadata is added for
ordering.

### Ordering

CDC rows need a deterministic order for deduplication inside a micro-batch. The source exposes
`_kafka_timestamp`, `_kafka_partition` and `_kafka_offset`; Debezium also exposes
`_cdc_timestamp`. A sink can override the order with `cdcOrderFields`. Kafka ordering is only
guaranteed per partition; use an event-time field if keys are not co-partitioned.

## Execution

The source declares `batch` and `streaming` execution modes. In a streaming job, Kafka offsets
are stored by Spark in the sink's checkpoint, so each streaming job must use exactly one sink.
`endingOffsets` and `endingOffsetsByTimestamp` are batch-only and are rejected in streaming mode.

Streaming example:

```yaml
job:
  mode: streaming
  trigger: "processing-time: 30 seconds"

source:
  - type: kafka-source
    id: kafka_orders
    config:
      subscribe: orders
      valueFormat: json
      valueSchema:
        id: long
        name: string
        __op: string
      cdcFormat: op-column
      options:
        startingOffsets: earliest
```

## Lag and health metrics

In streaming mode the source registers a query listener that compares Spark's consumed end
offset with the latest Kafka offset through an AdminClient. The lookup is throttled by
`lagMetricsIntervalMs` and never fails the query. It exposes:

- `streaming.source.lag.records`: total records behind the latest offset;
- `streaming.source.lag.max`: largest per-partition lag;
- `streaming.source.partitions`: partitions in the measurement;
- `streaming.source.lag.updated.ms`: time of the last measurement;
- `streaming.source.healthy`: `1` on a successful lookup, `0` on failure.

Only aggregate lag is stored as metrics; per-partition values are available at DEBUG in the
driver log. The AdminClient uses the same `kafka.*` security options as the reader; no broker
configuration or credentials are logged. A failed lookup logs only the exception class.

The listener is registered per Spark application, and a streaming job has one source, so the
latest registration is authoritative. Lag is measured from Spark's progress end offset, not from
a consumer group; a finite trigger that consumes all available data reports zero lag.

The source does not implement source-side checkpointing; Spark's checkpoint stores Kafka offsets.
The Kafka runtime is packaged in `kafka-runtime`; a direct `spark-submit` invocation must add that
jar and, for Avro/Protobuf values, `kafka-schema-registry-runtime` to `--jars`.
