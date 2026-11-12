# Basic Transform Operators

This module provides nine DataFrame transforms. Every operator accepts an optional
`numPartitions` and repartitions its result when set.

| Type | Purpose | Dependencies |
| --- | --- | --- |
| `filter-transform` | Filter rows with a Spark SQL condition | exactly 1 |
| `aggregate-transform` | Group and aggregate with SQL expressions, including event-time windows in streaming | exactly 1 |
| `dedup-transform` | Keep one row per key, with a bounded streaming state | exactly 1 |
| `union-transform` | Merge rows with `union` / `unionByName` | 2 or more |
| `column-transform` | Select, rename and cast columns | exactly 1 |
| `limit-transform` | Limit the number of rows | exactly 1 |
| `join-transform` | Join two DataFrames | exactly 2 |
| `validate-transform` | Validate required columns and SQL rules; drop, fail or quarantine invalid rows | exactly 1 |
| `schema-contract-transform` | Fail when the input schema does not match declared columns and types | exactly 1 |

`aggregate-transform`, `dedup-transform` and inner `join-transform` are stateful in streaming
execution and require event-time watermarks. Their state is bounded by those watermarks. A
stateful job's checkpoint records a source/transform/Spark-version contract, so a changed
config fails the restart instead of silently resuming incompatible state. See
[streaming-state.md](../../build/doc/streaming-state.md) for the contract, output modes and
current limitations.

The client attaches the `basic-transform` jar automatically when a pipeline uses any of these
types. The identifiers are also listed in `ScalaInterop.BASIC_TRANSFORM_TYPES`; keep that list in
sync when adding an operator to this module.

## validate-transform

```yaml
transform:
  - type: validate-transform
    id: validate1
    dependencies: [source1]
    config:
      requiredColumns: [id, amount]
      rules:
        amount_positive: "amount > 0"
        id_present: "id is not null"
      mode: drop
      quarantinePath: hdfs:///data/quarantine/orders
```

`requiredColumns` is checked against the input schema and fails when a column is missing. At
least one `condition` or entry in `rules` is required; multiple rules are combined with `AND`,
and a null result is treated as invalid. `mode: error` fails a batch and reports only the invalid
row count. `mode: drop` filters invalid rows. `quarantinePath` is batch-only, requires
`mode: drop`, and writes the invalid rows as JSON. Streaming supports `mode: drop` only, because
a streaming query can not perform a driver-side count or side write.

## Why these operators share one module

Unlike `sql-transform`, these operators live in one module instead of one module per operator.
That is intentional, because they are a single family of small DataFrame operations with the same
dependency profile:

- they only depend on core and Spark SQL/DataFrame APIs; no external clients or version matrix;
- most operators are small and stateless; `aggregate-transform` and `dedup-transform` keep only
  bounded event-time state in streaming, and all operators share `TransformSupport` parsing;
- they have one release cadence and can share one test SparkSession;
- the whole jar is well under 100 KB, so module and packaging overhead is larger than the code
  itself;
- `sql-transform` stays separate because it has its own SQL parsing, temporary views and
  extension semantics; the basic operators do not need that layer.

The trade-off is deliberate: the client selects connector jars by type prefix, so a module with
several identifiers needs an explicit type-to-module mapping (`BASIC_TRANSFORM_TYPES`). If these
operators later gain external dependencies or need independent release cadence, split them into
one module per operator instead.

## Why sources and sinks should normally stay one per module

Technically a single jar can register multiple `DataSource` or `DataSink` implementations, but
source and sink modules are dependency and deployment boundaries, not just code organization.
Putting unrelated connectors in one module would:

- force every job to carry unrelated runtime jars (the Elasticsearch runtime is about 2 MB, the
  Hudi runtime about 100 MB and the Kafka runtime about 8 MB); the connector jars themselves are
  thin adapters of roughly 10–20 KB;
- lose the client's per-job jar selection and increase driver/executor classpath and startup cost;
- couple independent upstream connector and driver upgrades, so a MongoDB driver change would
  require re-releasing unrelated connectors;
- make shading, `META-INF/services` merging and classloader isolation harder to reason about;
- weaken test/CI isolation and make connector-specific failures harder to localize;
- increase the security and license surface of deployments that only need one connector.

Grouping sources or sinks is reasonable only for a small connector family with identical
dependencies and one release cadence, and only after replacing the type-prefix jar selection with
an explicit registry or manifest. Otherwise the type-to-module mapping grows without bound.

A useful rule of thumb: **transforms are pure logic, so group them by family; sources and sinks
are dependency boundaries, so keep them one per connector.**

## filter-transform

```yaml
transform:
  - type: filter-transform
    id: filter1
    dependencies: [source1]
    config:
      condition: "amount >= 100 and order_city = 'beijing'"
      numPartitions: 2
```

`condition` is a Spark SQL expression evaluated by `DataFrame.filter`.

## aggregate-transform

```yaml
transform:
  - type: aggregate-transform
    id: aggregate1
    dependencies: [filter1]
    config:
      groupBy: [order_city]          # optional; a comma-separated string is also accepted
      aggregations:
        order_count: "count(*)"
        total_amount: "sum(amount)"
        distinct_users: "count(distinct user_id)"
```

The list form preserves output order:

```yaml
aggregations:
  - name: order_count
    expr: "count(*)"
  - name: total_amount
    expr: "sum(amount)"
```

When `groupBy` is absent, the result is one global aggregate row. Expressions may use any Spark SQL
aggregate function.

### Streaming aggregation

Streaming aggregation is stateful. It requires an event-time column, a watermark and a bounded
event-time window:

```yaml
transform:
  - type: aggregate-transform
    id: window_count
    dependencies: [source1]
    config:
      eventTime: order_time
      watermark: "10 minutes"
      window: "5 minutes"        # or {duration: "5 minutes", slide: "1 minute"}
      slide: "1 minute"          # optional when window is a string
      groupBy: [order_city]
      aggregations:
        order_count: "count(*)"
        total_amount: "sum(amount)"
```

- `eventTime` must name a timestamp column.
- `window` is required in streaming; `slide` is optional. A window map may contain only
  `duration` and `slide`.
- The result column order is `window_start`, `window_end`, the group-by columns and the
  aggregations.
- Append-mode consumers receive a window after the watermark passes its end. Spark then evicts
  the window state.
- `eventTime`, `watermark`, `window` and `slide` are rejected in batch execution.
- Update/complete output and stream-stream joins are not part of this contract yet.

## dedup-transform

`dedup-transform` keeps one row per key. In batch mode only `keys` is required:

```yaml
transform:
  - type: dedup-transform
    id: dedup1
    dependencies: [source1]
    config:
      keys: [order_id]
```

Streaming deduplication is stateful and additionally requires an event-time column and a
watermark:

```yaml
config:
  keys: [order_id]
  eventTime: order_time
  watermark: "10 minutes"
```

The watermark evicts deduplication state. A duplicate that arrives after the watermark has
passed its event time can be emitted again; use a sink primary key when exact cross-window
deduplication is required. Which row is retained for a key is not deterministic. The operator
keeps all input columns, so it preserves incremental key/operation/ordering metadata.

## schema-contract-transform

Fails the pipeline when the input schema drifts from a declared contract:

```yaml
transform:
  - type: schema-contract-transform
    id: contract1
    dependencies: [source1]
    config:
      columns:
        order_id: long
        order_time: timestamp
        amount: decimal(12,2)
      allowExtraColumns: false
```

- Column names are compared case-insensitively; nullability is not part of the contract.
- Missing columns and type mismatches always fail.
- Extra columns fail unless `allowExtraColumns: true`.
- The DataFrame is returned unchanged, so the operator preserves incremental metadata.
- The schema is available for streaming DataFrames at planning time, so streaming jobs fail
  before consuming data.

## union-transform

```yaml
transform:
  - type: union-transform
    id: union1
    dependencies: [source1, source2]
    config:
      inputs: [source1, source2]     # required for byPosition, optional for byName
      mode: byName                   # byName (default) | byPosition
      allowMissingColumns: true      # byName only
      distinct: true
```

- `byName` aligns columns by name and uses the first input's column order.
- `byPosition` aligns columns by position and **requires** `inputs` because dependency maps do not
  have a stable order.
- `inputs` must contain every dependency exactly once.
- `allowMissingColumns` maps missing columns to null; it is only valid with `byName`.
- `distinct` applies `dropDuplicates` to the union result.

## column-transform

```yaml
transform:
  - type: column-transform
    id: columns1
    dependencies: [source1]
    config:
      fields:
        - order_id                          # keep the column as is
        - name: user_name                   # output name
          source: user_id                   # input column
          type: string                      # optional cast
        - name: amount
          type: double
```

Fields are applied in declaration order. Each entry is either a column name or a map with:

- `name`: output column name (required)
- `source`: input column name (defaults to `name`)
- `type`: Spark SQL type for `cast`, for example `string`, `int`, `decimal(10,2)`,
  `array<string>`

Duplicate output names (case-insensitive) and unknown field properties are rejected.

## limit-transform

```yaml
transform:
  - type: limit-transform
    id: limit1
    dependencies: [aggregate1]
    config:
      limit: 100
```

`limit` must be greater than or equal to zero. Spark applies the limit to the whole result.

## join-transform

```yaml
transform:
  - type: join-transform
    id: join1
    dependencies: [orders, users]
    config:
      left: orders
      right: users
      joinType: left                # inner (default) | left | right | full | left_semi | left_anti
      on: [user_id]                 # common column names
      condition: "l.amount >= 10"   # optional extra predicate
```

`on` can also map different column names:

```yaml
on:
  user_id: uid
```

The transform:

- requires exactly two dependencies, and `left` / `right` must name them;
- emits join keys once, using the left column name; for `right` and `full` joins the key is
  coalesced so right-only rows keep their key;
- removes the right-side key columns from the result;
- keeps all other columns from both sides, in left-then-right order. If both sides contain the
  same non-key column name, both columns are kept; add a `column-transform` after the join to
  select or rename them.

`condition` is an extra predicate on top of key equality. Reference columns as `l.<name>` or
`r.<name>`; join key equality belongs in `on`. Since keys are renamed internally during the join,
do not reference key columns in `condition`.

`left_semi` and `left_anti` return only left-side columns. Supported join type aliases include
`left_outer`, `right_outer`, `full_outer`, `semi` and `anti`.

### Streaming inner join

Both dependencies must be streaming. Supply one event-time column and watermark for each side:

```yaml
transform:
  - type: join-transform
    id: orders_payments
    dependencies: [orders_stream, payments_stream]
    config:
      left: orders_stream
      right: payments_stream
      joinType: inner
      on: [order_id]
      leftEventTime: order_time
      rightEventTime: payment_time
      leftWatermark: "10 minutes"
      rightWatermark: "10 minutes"
```

The transform applies both watermarks before the join and Spark evicts unmatched state as the
watermarks advance. Only `joinType: inner` is supported in streaming; outer joins need an
event-time range condition and are not part of the current contract. `left_semi` and
`left_anti` remain batch-only.

## Example pipeline

```yaml
transform:
  - type: filter-transform
    id: filter1
    dependencies: [source1]
    config:
      condition: "amount >= 100"

  - type: column-transform
    id: columns1
    dependencies: [filter1]
    config:
      fields:
        - order_id
        - name: city
          source: order_city
        - name: amount
          type: double

  - type: aggregate-transform
    id: aggregate1
    dependencies: [columns1]
    config:
      groupBy: [city]
      aggregations:
        order_count: "count(*)"
        total_amount: "sum(amount)"

  - type: limit-transform
    id: limit1
    dependencies: [aggregate1]
    config:
      limit: 100
```
