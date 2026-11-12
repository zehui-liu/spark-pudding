# Connector support modules

`connector/` contains small support libraries shared by source and sink connectors. They are not
core SPIs, providers or runtime bundles: they hold connector-specific parsing and option logic
that would otherwise leak into `core` or be duplicated across connector modules.

## Layout

```text
connector/
  jdbc-common/   # JDBC/MySQL shared configuration, atomic replace and streaming upsert
```

`jdbc-common` depends on `core` for shared contracts and utilities (exceptions, execution modes,
checkpoint validation and CDC helpers) and on Spark SQL for DataFrame and Structured Streaming
APIs. Both dependencies use `provided` scope. The JDBC and MySQL source and sink modules depend on
`jdbc-common` with `provided` scope.

## Dependency direction

```text
jdbc-source ─┐
mysql-source ┼─ provided ─> jdbc-common ─ provided ─> core, spark-sql
jdbc-sink ───┤
mysql-sink ──┘
```

Do not put generic runtime contracts here. New support libraries should be narrow and named for
the connector family they serve; if only one connector needs a helper, keep it in that connector.

## Packaging

`jdbc-common` is a support artifact, not a Pudding component. It does not appear in
`capabilities.yaml`. The distribution copies `jdbc-common-<version>.jar` into `libs`; the client
adds it automatically when a job selects `jdbc-source`, `jdbc-sink`, `mysql-source` or
`mysql-sink`. A direct `spark-submit` invocation must add it to `--jars` for those connectors.

Do not shade `jdbc-common` into individual connector jars: that would create duplicate copies of
the same classes and allow their validation behavior to drift.
