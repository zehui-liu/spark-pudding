# Provider modules

Provider modules implement the connection and catalog SPIs defined by `core`. They are
discovered at runtime with `ServiceLoader`, so adding a provider does not require a change to
`core`.

## Layout

```text
provider/
  connection-providers/   # built-in ConnectionProvider implementations
  catalog-providers/      # built-in CatalogProvider implementations
  redis-state-provider/   # optional Redis IncrementalStateBackendProvider
  etcd-state-provider/    # optional etcd IncrementalStateBackendProvider
```

`connection-providers` contains Kafka, JDBC, MySQL, Elasticsearch, MongoDB, Hive Metastore and
Gravitino connections. `catalog-providers` contains Hive, Iceberg, JDBC, Hudi, Paimon and
Gravitino catalogs.

## Dependency direction

```text
provider/*  --provided-->  core
```

`core` never depends on a provider module. It keeps only the contracts and runtime plumbing:

- `ConnectionProvider`, `ConnectionRegistry`, `ResolvedConnection`
- `CatalogProvider`, `CatalogManager`, `ResolvedCatalog`, `CatalogTableName`
- `SecretResolver`
- the job engine and configuration model

Provider modules use `provided` scope for `core`, Scala and (for catalogs) Spark. They must not
add a reverse dependency to `core` or to a source/sink module.

## Adding a provider

1. Add the implementation to the appropriate provider module, or create a new module under
   `provider/` if it needs independent dependencies or release cadence. Put connection
   implementations in `org.pudding.provider.connection` and catalog implementations in
   `org.pudding.provider.catalog`.
2. Register the class in
   `META-INF/services/org.pudding.core.connection.ConnectionProvider` or
   `META-INF/services/org.pudding.core.catalog.CatalogProvider`.
3. Add tests for resolution and validation to the provider module.
4. If the module is separate, add it to `provider/pom.xml`, `build/package.xml` and, if it has
   its own jar rather than reusing an existing provider jar, client jar selection. The capability
   generator and `build/bin/verify-distribution.sh` discover provider module directories
   automatically.
5. Run `mvn clean verify` and a packaged smoke test.

Third-party providers can use the same SPI and capability fragment mechanism from a jar in
`libs`; they do not need to modify `core`.

## Incremental state backends

Incremental cursor/journal storage is also pluggable. The built-in filesystem backend is selected
with `statePath`, and the built-in JDBC backend is selected through `incremental.state`. The
optional `redis-state-provider` and `etcd-state-provider` modules implement the same contract
without adding their dependencies to core:

- `redis-state-provider` shades and relocates Jedis and commons-pool2; it supports
  single-node, Sentinel and Cluster modes. Cluster keys share a source hash tag and all
  token-compare operations are Lua scripts, so no `WATCH`/`MULTI` is required.
- `etcd-state-provider` uses the etcd v3 HTTP/JSON gateway and has no client dependency. A
  positive lock TTL is backed by a real etcd lease that is replaced on renewal.

A third-party backend implements
`org.pudding.core.incremental.IncrementalStateBackendProvider` and registers it in
`META-INF/services/org.pudding.core.incremental.IncrementalStateBackendProvider`. Most backends
should extend `AbstractIncrementalStateBackend`, which supplies the transaction transitions,
lock lease renewal, expiry takeover and fencing checks; the provider implements the storage
primitives (`readState`, `persistState`, `createLock`, `renewLock`, `deleteLock`, ...). The
provider receives the resolved `incremental.state` options. `bin/pudding state` loads
`libs/*state-provider-*.jar` automatically. State backend providers are not connection or
catalog components and are not listed in `capabilities.yaml`.

## Runtime packaging

The distribution includes the built-in provider jars in `libs`. The client adds
`connection-providers` and `catalog-providers` to every submission. A direct `spark-submit`
invocation must add them explicitly when a job uses a connection or catalog:

```shell
--jars "$PUDDING_HOME/libs/connection-providers-<version>.jar,$PUDDING_HOME/libs/catalog-providers-<version>.jar"
```

A job that references a connection or catalog without the corresponding provider jar fails before
Spark starts with an `unknown connection type` or `unknown catalog type` error.
