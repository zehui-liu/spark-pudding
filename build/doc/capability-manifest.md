# Capability Manifest

Every spark-pudding distribution contains a generated `capabilities.yaml` at its root. It
enumerates the sources, sinks, transforms, connection providers and catalog providers in that
distribution. The file is metadata for the backend and for tooling; the runtime ServiceLoader
registrations remain the source of truth.

## Strict loading

The root manifest is mandatory. `CapabilityManifest.load(distributionRoot)` throws when the file
is absent instead of treating the distribution as having no components. This is the default
behaviour; there is no silent fallback.

The loader:

1. reads `<distributionRoot>/capabilities.yaml`;
2. validates the schema version, required fields and unique `kind:type` keys;
3. scans every jar under `libs` (recursively) for third-party fragments;
4. merges fragments and rejects duplicate components or fragments that declare a different jar;
5. exposes a stable `capabilitySetHash` for caching and change detection.

`CapabilityManifest.verifyDistribution(distributionRoot)` additionally verifies that every
component's `jar` exists in the distribution.

## Root manifest

```yaml
schemaVersion: "2.1"
productVersion: "1.0.0"
sparkVersion: "3.4.0"
scalaBinaryVersion: "2.12"
generatedAt: "2026-10-25T02:57:47Z"
components:
  - kind: source
    type: mongodb-source
    className: org.pudding.source.MongodbSource
    module: mongodb-source
    jar: mongodb-source-1.0.0.jar
    capabilities:
      connectionTypes: [mongodb]
      catalogTypes: []
      executionModes: [batch]
      syncIncrementalSemantics: []
      lakeSyncIncremental: false
  - kind: transform
    type: filter-transform
    className: org.pudding.transform.FilterTransform
    module: basic-transform
    jar: basic-transform-1.0.0.jar
    capabilities:
      executionModes: [batch, streaming]
      preservesSyncIncrementalMetadata: true
```

Fields:

| Field | Meaning |
| --- | --- |
| `kind` | `source`, `sink`, `transform`, `connection` or `catalog` |
| `type` | stable component identifier used in job configuration |
| `className` | implementation class, informational |
| `module` | Maven module / connector family |
| `jar` | jar that must be present in the distribution |
| `capabilities.connectionTypes` | connection types accepted by a source/sink |
| `capabilities.catalogTypes` | catalog types accepted by a source/sink |
| `capabilities.executionModes` | internal execution engines supported by a source/sink/transform: `batch`, `streaming`; `sync` uses batch execution |
| `capabilities.streamingOutputModes` | sink streaming output modes: `append`, `update`, `complete` |
| `capabilities.streamingUpsert` | sink `update` output is key-based upsert rather than append-only |
| `capabilities.syncFullRefresh` | sink can replace its target contents for `job.mode: sync` with `job.strategy: full` |
| `capabilities.syncAtomicReplace` | sink replaces the target atomically for the selected backend; otherwise `allowNonAtomicReplace` is required |
| `capabilities.syncIncrementalSemantics` | semantics supported by a source/sink for `job.mode: sync` with `job.strategy: incremental`: `append`, `upsert`, `cdc` |
| `capabilities.idempotentSyncIncrementalSemantics` | sink semantics that can safely replay the same batch after an uncommitted transaction |
| `capabilities.syncIncrementalFencing` | sink records the source/fencing token in commit metadata and rejects a writer whose token is not newer (an equal token is accepted only for an already committed transaction) |
| `capabilities.lakeSyncIncremental` | source/sink participates in `job.mode: sync` with `job.strategy: incremental` |
| `capabilities.preservesSyncIncrementalMetadata` | transform keeps key, operation and ordering columns for `sync` with `strategy: incremental` |
| `description` | optional |
| `since` | optional |
| `deprecated` | optional |
| `replacement` | optional |

`type` is the stable key. A component is identified by `kind + ":" + type`; class names can change
without being interpreted as a removal.

## Backend usage

The backend should read the manifest from the exact distribution version it will execute, not
from its own classpath:

```scala
val manifest = CapabilityManifest.load(distributionRoot)
val byKey = manifest.byKey
val hash = manifest.capabilitySetHash
```

The backend should keep a snapshot per `productVersion` and compare snapshots when a new
distribution arrives:

```scala
val diff = CapabilityManifest.diff(previousManifest, currentManifest)
// diff.added, diff.removed, diff.changed
```

This is how removals are detected: the current manifest only lists current capabilities, so the
backend compares it with the stored previous snapshot. `deprecated` and `replacement` can be used
to warn about upcoming removals before they happen.

Use the manifest to:

- validate source/sink/transform types before submission;
- validate connection types declared by sources and sinks;
- render connector lists in a UI;
- select connector and provider jars;
- select runtime jars with the rules in `runtime/README.md` (runtime jars are not components);
- select shared connector-support jars with the rules in `connector/README.md` (support jars are
  not components);
- block or warn before removing a capability still used by active jobs.

Catalog providers currently publish no connection-type capabilities, and runtime jars,
connector-support jars (`jdbc-common`) and JDBC drivers are support artifacts that are
intentionally absent from `capabilities.yaml`.

## Third-party connectors

A third-party connector can join an existing distribution without regenerating the root manifest.
Ship a jar in `libs` (or a subdirectory of `libs`) containing
`META-INF/spark-pudding/component.yaml`:

```yaml
schemaVersion: "2.1"
components:
  - kind: source
    type: hbase-source
    className: com.example.source.HbaseSource
    module: hbase-source
    jar: hbase-source-1.0.jar
    capabilities:
      connectionTypes: [hbase]
      catalogTypes: []
      executionModes: [batch]
```

Rules:

- the fragment must use the same `schemaVersion` as the root manifest;
- `jar` must match the file name of the jar that contains the fragment;
- the fragment jar is scanned without loading its classes;
- every component must have a unique `kind:type` across the root manifest and all fragments;
- duplicate or conflicting components are rejected;
- the jar is included in `verifyDistribution`'s jar existence check.

The backend scans `libs` recursively, so extension jars can be placed in a subdirectory such as
`libs/extensions/` without changing configuration.

## Tooling

The capability API lives in core and is bundled, with the Scala runtime, in
`bin/pudding-client.jar`. The unified CLI exposes the tool as `bin/pudding capabilities`:

```shell
# validate the root manifest, fragments and jar references
bin/pudding capabilities verify /path/to/spark-pudding

# print the merged manifest
bin/pudding capabilities show /path/to/spark-pudding

# compare two distributions
bin/pudding capabilities diff /path/to/previous /path/to/current
```

The wrapper invokes
`org.pudding.core.capability.CapabilityTool` from `pudding-client.jar`, so a backend can run the
same class directly or use `CapabilityManifest` as a library. The tool does not need Spark or
connector classes on its classpath; it reads YAML and jar metadata only. If you use the standalone
core jar instead, add the Scala runtime to the classpath.

## Build generation

`build` runs `CapabilityGenerator` during `prepare-package`:

- it scans the built module jars and their `META-INF/services` registrations;
- it loads each implementation in an isolated classloader to read `identifier` and
  connection/catalog capabilities;
- it writes `<build>/target/capabilities.yaml`;
- the assembly copies it to the distribution root;
- `mvn verify` runs `CapabilityTool verify` and `build/bin/verify-distribution.sh` checks the
  manifest.

Do not edit `capabilities.yaml` by hand. A capability added to a module's SPI file is picked up by
the generator. If a connector jar is missing, the generator fails the build.

## Schema compatibility

`schemaVersion` is independent of `productVersion`. The current schema is `2.1`, which adds
streaming output-mode declarations to the `2.0` batch / `sync` / `streaming` mode model.
Schema `2.0` manifests and fragments remain readable; new output-mode fields default to an empty
list and `false`.

Schema `1.0` is rejected; there is no automatic manifest migration. Regenerate the distribution
manifest, and rewrite a third-party fragment for schema `2.0` or `2.1`. The retired `1.0`
capability field names are rejected explicitly even when a fragment declares a supported schema
version, so a copied old fragment cannot silently lose capabilities:

| Retired `1.0` field | `2.0` field |
| --- | --- |
| `incrementalSemantics` | `syncIncrementalSemantics` |
| `idempotentIncrementalSemantics` | `idempotentSyncIncrementalSemantics` |
| `incrementalFencing` | `syncIncrementalFencing` |
| `fullRefresh` | `syncFullRefresh` |
| `lakeIncremental` | `lakeSyncIncremental` |
| `preservesIncrementalMetadata` | `preservesSyncIncrementalMetadata` |

Unknown future fields are ignored within a supported schema version; required fields are
validated strictly.
