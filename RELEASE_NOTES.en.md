# spark-pudding Release Notes

English | [简体中文](RELEASE_NOTES.md)

## v1.0.0 — 2026-11-12

> Status: committed on branch `v-1.0` (commit `b08f4fa`). No release tag or push has been
> created yet.
> Branch: `v-1.0`

v1.0.0 expands the project from a single-purpose batch framework into a data integration
framework with batch, incremental sync and streaming execution.

The release is not primarily about adding more connectors. It fixes the following boundaries:

- **One component model**: sources, transforms and sinks declare capabilities that are checked
  before Spark starts.
- **One resource model**: connections hold connection settings, catalogs hold table metadata,
  and secrets are stored only as references.
- **One incremental contract**: `append`, `upsert` and `cdc` are selected by the source and
  applied automatically by the sink.
- **One job model**: `batch`, `sync` and `streaming`.
- **One commit boundary**: every mode accepts exactly one sink.
- **Runtime isolation**: thin connector adapters are separated from third-party runtime jars.
- **One operations entry point**: `bin/pudding` exposes checkpoint, state, capabilities,
  maintenance and Kubernetes commands.
- **Capability discovery**: every distribution generates and verifies `capabilities.yaml`.

> Entries after 2026-10-24 are based on development-session and workspace records. Work that
> crosses midnight is grouped under its main completion day, and one capability may span
> multiple days. Entries on or before 2026-09-02 follow git commit dates.

---

## Contents

- [Capability overview](#capability-overview)
- [Historical versions](#historical-versions)
- [Daily development log](#daily-development-log)
- [Compatibility and upgrade notes](#compatibility-and-upgrade-notes)
- [Known limitations](#known-limitations)
- [Quality verification](#quality-verification)
- [Next steps](#next-steps)

---

## Capability overview

### Job modes

| Mode | Purpose | Sources | Sinks | Transforms | Progress |
| --- | --- | --- | --- | --- | --- |
| `batch` | Bounded batch processing | Multiple | One | Free composition | No durable cursor |
| `sync` + `full` | Full synchronization | One | One with full-refresh support | Allowed | No cursor |
| `sync` + `incremental` | Bounded incremental sync | One with incremental support | One with matching semantics | Metadata-preserving only | Incremental state backend |
| `streaming` | Continuous processing | One | One | Validated against streaming capabilities | Spark checkpoint |

Every mode accepts exactly one sink, which gives each job one commit boundary.

The framework does not include a scheduler. Scheduling belongs to Airflow, cron, Kubernetes
CronJob or another external orchestrator.

### Source capability matrix

| Source | Batch | Streaming | Sync incremental |
| --- | --- | --- | --- |
| `file-common-source` | Yes | No | No |
| `hive-source` | Yes | No | No |
| `jdbc-source` / `mysql-source` | Yes | No | `append` / `upsert` (`cursorField`) |
| `mongodb-source` | Yes | No | No |
| `elasticsearch-source` | Yes | No | No |
| `kafka-source` | Yes | Yes | No |
| `hudi-source` | Yes | Yes | `upsert` / `cdc` |
| `iceberg-source` | Yes | Yes | `append` / `upsert` / `cdc` |
| `paimon-source` | Yes | Yes | `append` / `cdc` |
| `mysql-cdc-source` | No | Yes | Not applicable (unbounded) |
| `postgres-cdc-source` | No | Yes | Not applicable (unbounded) |
| `mongodb-cdc-source` | No | Yes | Not applicable (unbounded) |

### Sink capability matrix

| Sink | Batch | Streaming | Sync incremental | Full refresh |
| --- | --- | --- | --- | --- |
| `console-sink` | Yes | Yes (display) | No | No |
| `file-sink` | Yes | Yes | `append` | Non-atomic, explicit opt-in |
| `hive-sink` | Yes | No | No | Non-atomic, explicit opt-in |
| `kafka-sink` | Yes | Yes | No | No |
| `elasticsearch-sink` | Yes | Yes | No | No |
| `mongodb-sink` | Yes | Yes | No | No |
| `jdbc-sink` / `mysql-sink` | Yes | Yes | `append` / `upsert` / `cdc` | Atomic replacement |
| `hudi-sink` | Yes | Yes | `append` / `upsert` / `cdc` | Atomic replacement |
| `iceberg-sink` | Yes | Yes | `append` / `upsert` / `cdc` | Atomic replacement |
| `paimon-sink` | Yes | Yes | `append` / `upsert` / `cdc` | Non-atomic opt-in |

### Catalogs and connections

| Type | Support |
| --- | --- |
| Connection | Kafka, JDBC/MySQL, MongoDB, Elasticsearch, Hive Metastore, Gravitino |
| Catalog | Hive, Iceberg, JDBC, Hudi, Paimon, Gravitino |
| Secret | `${secret:env/NAME}`, `${secret:file:/path#key}`, `${secret:file:/path}` |

Gravitino is metadata-only. The project does not introduce the Gravitino SDK, bind to a
Gravitino version or add Gravitino dependencies to core.

### Incremental state backends

| Backend | Locking | Lease / fencing | Recommended use |
| --- | --- | --- | --- |
| Filesystem / HDFS / S3A | Best effort | Limited | Single writer, testing |
| JDBC | Strong | Yes | Production |
| Redis | Strong | Yes | Production |
| etcd | Strong | Native lease | Production |

State stores contain only cursors, pending transactions, plan fingerprints, attempt counts,
failure metadata and fencing tokens. They never contain credentials or source configuration.

### Runtime isolation

| Component | Runtime |
| --- | --- |
| Hudi | `hudi-runtime` |
| Paimon | `paimon-runtime` |
| Iceberg | `iceberg-runtime` |
| Elasticsearch | `elasticsearch-runtime` |
| MongoDB | `mongodb-runtime` |
| Kafka | `kafka-runtime` |
| Kafka Schema Registry | `kafka-schema-registry-runtime` (optional) |
| Native CDC | `cdc-runtime` |
| S3A/OSS/Azure/GCS/COS | Corresponding Hadoop object-store runtime |

The client selects runtimes from source, sink, catalog and `valueFormat` settings. A direct
`spark-submit` call must add the required jars manually.

---

## Historical versions

### 2026-09-02

- Added `elasticsearch-source`:
  - Elasticsearch 8;
  - authentication parameters;
  - field selection;
  - scroll reads;
  - optional `limit`;
  - `elasticsearch-source.yaml` example and module README.
- Split Hudi, Iceberg and Elasticsearch configuration docs into module-level READMEs.
- Fixed a Scala Map initialization issue in IntelliJ IDEA.

### 2026-03-31

- Added the initial `iceberg-source`.
- Added Iceberg table/path reads and basic configuration.

### 2025-03-06

- Fixed a Kafka source compilation issue.

### 2025-03-05

- Moved to `0.0.2-SNAPSHOT` (historical; the current development line is `1.0.0-SNAPSHOT`).
- Added the initial Pudding client.

### 2024-12-31

- Initialized the project.
- Added job configuration parsing and the source / transform / sink extension points.
- Added the first Maven modules and README.

---

## Daily development log

### 2026-10-24

Theme: connection and metadata-catalog model

Background:

Early sources stored endpoints, users and passwords directly in each source config. Every new
source repeated connection settings and could not reuse a Hive Metastore, Kafka cluster or JDBC
database connection.

Completed:

- Designed a unified connection model:
  - `name`;
  - `type`;
  - `endpoint`;
  - `auth`;
  - `properties`;
  - `options`.
- Designed a unified catalog model:
  - `name`;
  - `type`;
  - optional `connection`;
  - `properties`.
- Allowed sources and sinks to reference resources with `connection: <name>` and
  `catalog: <name>`.
- Defined the resolution order:
  1. the parser keeps references as names;
  2. `PipelineValidator` validates references and component capabilities;
  3. the driver resolves secrets, connections and catalogs;
  4. components receive only resolved resource information.
- Added Gravitino with explicit boundaries:
  - metadata-only catalog;
  - REST API;
  - maps Hive, Iceberg, JDBC, Hudi and Paimon to native Spark catalogs;
  - no Gravitino SDK;
  - no version binding;
  - no Gravitino dependency in core.
- Decided that Kafka, MongoDB and Elasticsearch are configured through connections rather than
  forced into a table-catalog model.
- Established the same extension path for future systems such as HBase.

Example:

```yaml
connection:
  - name: kafka-prod
    type: kafka
    endpoint: broker-1:9092,broker-2:9092
    auth:
      mechanism: SCRAM-SHA-512
    properties:
      security.protocol: SASL_SSL

catalog:
  - name: lake
    type: iceberg
    properties:
      type: rest
      uri: https://catalog.example
```

Provider implementations, secret resolution and Spark catalog installation followed in later
days.

### 2026-10-25

Theme: basic transforms and Hudi/Paimon support

Completed:

- Added the `basic-transform` module with:
  - `filter-transform` for SQL conditions;
  - `aggregate-transform` for group-by and SQL aggregations;
  - `union-transform` for `union` and `unionByName`;
  - `column-transform` for select, rename and cast;
  - `limit-transform` for row limits;
  - `join-transform` for two-input joins.
- Every operator supports optional `numPartitions`.
- `union-transform` supports positional and by-name modes.
- `join-transform` supports explicit left/right input names instead of relying on map order.
- Added `basic-transform/README.md` covering:
  - each operator;
  - dependency count;
  - join semantics;
  - streaming constraints.
- Extended Hudi and Paimon catalog, source and sink support:
  - catalog mode and path mode;
  - table properties;
  - Paimon primary-key tables;
  - groundwork for streaming, incremental reads and CDC.
- Defined the capability manifest direction:
  - generate `capabilities.yaml` in the distribution;
  - backend scans a fixed file;
  - third-party connectors add
    `META-INF/spark-pudding/component.yaml`;
  - unknown capabilities are rejected by default.

Design rules:

- transforms are pure logic and are grouped by family;
- sources and sinks are dependency boundaries and stay one connector per module;
- unrelated connectors are not placed in one runtime.

Tests:

- unit tests for basic transforms;
- console sink display tests;
- basic Iceberg source read tests.

### 2026-10-26

Theme: lightweight core, runtime split and new sinks

Completed:

- Moved catalog and connection implementations out of core:
  - `provider/connection-providers`;
  - `provider/catalog-providers`.
- Kept only the following in core:
  - SPI definitions;
  - `ConnectionConf` / `CatalogConf`;
  - configuration models;
  - registry/resolver interfaces;
  - Spark job entry points.
- Added shared runtime modules:
  - `hudi-runtime`;
  - `paimon-runtime`;
  - `iceberg-runtime`.
- Runtime jars own third-party libraries, Spark extensions and Hudi/Parquet/HFile
  implementations. Connector jars keep only Pudding adapters.
- Defined client runtime selection:
  - `hudi-source`, `hudi-sink` or Hudi catalog → `hudi-runtime`;
  - Paimon and Iceberg follow the same rule;
  - a runtime is added at most once per job;
  - direct `spark-submit` jobs must add runtime jars manually.
- Analyzed Hudi HBase/HFile dependencies:
  - Hudi metadata tables use HFile;
  - the runtime needs these dependencies;
  - they must not enter core or the connector adapter.
- Added sinks:
  - `elasticsearch-sink`;
  - `mongodb-sink`;
  - `kafka-sink`.
- Defined manifest/capability registration for sinks.
- Removed unused code, updated comments and READMEs.

Related docs:

- `runtime/README.md`;
- `provider/connection-providers`;
- `provider/catalog-providers`;
- sink READMEs.

### 2026-10-27

Theme: unified incremental semantics, JDBC support module and quality gates

Completed:

- Added incremental reads and writes for Hudi, Paimon and Iceberg.
- Defined three semantics:
  - `append`: every row is new;
  - `upsert`: every row is the latest state for its key;
  - `cdc`: rows contain an `I`, `U` or `D` operation.
- The source selects the semantics and the sink applies it automatically.
- Added source/sink capability declarations:
  - `supportedIncrementalSemantics`;
  - `isIdempotentIncremental`;
  - `supportsLakeIncremental`, later extended to `supportsIncrementalSync` for JDBC
    incremental support.
- Unsupported source/sink combinations fail during validation rather than falling back to
  append.
- Added key field, operation column and metadata preservation checks.
- Added incremental metadata preservation rules for transforms. A transform that drops key,
  operation or ordering columns cannot be used in sync incremental mode.
- Extracted `connector/jdbc-common`:
  - URL, user, password and driver;
  - table;
  - save mode;
  - Spark JDBC options;
  - connection-type validation.
- Added checkstyle to Maven.
- Ran code review, test expansion, dead-code cleanup and README updates.
- Fixed the single-sink design: one job has one commit boundary.
- Evaluated dependencies between incremental jobs; scheduling remains external.

Modules:

- `connector/jdbc-common`;
- `source/{hudi,iceberg,paimon}-source`;
- `sink/{hudi,iceberg,paimon}-sink`.

### 2026-10-28

Theme: initial job modes and Gravitino catalog

Completed:

- Added initial job modes:
  - `full`: full integration, no free-form transforms;
  - `incremental`: lakehouse-oriented incremental integration;
  - `etl`: free-form transforms for complex data flows.
- Wired mode constraints into the parser, validator and BuildJob.
- Explicitly rejected legacy configuration migration.
- Completed the Gravitino catalog provider:
  - REST calls;
  - bearer token and basic authentication;
  - vendor media type;
  - metalake/catalog resolution;
  - mapping to native Spark catalogs;
  - vendor-type validation.
- Added client target aliases and submission-plan validation.
- Added Iceberg batch tests and catalog-manager tests.
- Noted that these modes would be replaced by `batch`, `sync` and `streaming` on
  2026-11-01.

Key interfaces:

```scala
CatalogProvider
ConnectionProvider
ResolvedCatalog
ResolvedConnection
```

### 2026-10-29

Theme: incremental-state reliability

Completed:

- Adapt append sinks to the incremental transaction model:
  - record transaction ids;
  - decide replay safety from sink idempotence;
  - require an explicit `incremental.recovery` decision when append replay is unsafe.
- Added state backends:
  - filesystem / HDFS / S3A;
  - JDBC;
  - Redis;
  - etcd.
- Added locking:
  - owner, host and pid;
  - acquisition time;
  - lease TTL;
  - fencing token;
  - force unlock.
- Added state audit events:
  - lock acquired/released;
  - transaction begin/commit/abort;
  - replay/failure.
- Added HDFS/S3A state tests and process-crash recovery tests.
- Added migration and read compatibility for old filesystem state files.
- Added `IncrementalStateTool` tests:
  - show;
  - status;
  - reset;
  - resolve;
  - unlock.
- Added `ConfigRedactor` for log redaction and stable fingerprints.
- Evaluated Kafka → Hudi/Paimon/Iceberg streaming incremental ingestion.
- Cleaned unused code and updated docs.

State records contain:

```text
committedCursor
pending transaction
plan fingerprint
attempts
lastTransactionId
lastCommittedAt
lastFailureAt / lastFailureReason
fencingToken
lock info
```

State records never contain:

```text
credentials
connection URLs
source configuration
```

### 2026-10-30

Theme: Kafka streaming ingestion

Completed:

- Added `kafka-source` streaming mode:
  - raw/string;
  - JSON value schema;
  - DDL string or field map;
  - `parseMode: failfast / dropMalformed / permissive`;
  - `includeMetadata`;
  - `keepRawValue`;
  - `badRecordsPath` for batch;
  - `includeCorruptRecord` for streaming.
- Added CDC decoding:
  - generic `op-column`;
  - Debezium `before`/`after`;
  - `I/U/D` normalization;
  - Kafka topic/partition/offset/timestamp ordering metadata.
- Added Kafka → Hudi/Paimon/Iceberg:
  - streaming append;
  - streaming upsert;
  - streaming CDC;
  - one checkpoint per sink.
- Enforced one source and one sink in streaming jobs.
- Added the Kafka runtime.
- Added Kafka JSON/CDC and container tests.
- Documented limitations:
  - no Schema Registry at this point (added on 2026-11-05);
  - Kafka ordering is per partition;
  - cross-partition ordering for the same key requires event time or explicit order fields.

### 2026-11-01

Theme: batch / sync / streaming refactor

Background:

The initial `full` / `incremental` / `etl` modes mixed user intent with execution engine and
made transform rules and future extension unclear.

Completed:

- Replaced them with:
  - `batch`: general bounded processing;
  - `sync`: one-source/one-sink synchronization with `full` and `incremental`;
  - `streaming`: Structured Streaming.
- Unified rules:
  - every mode has exactly one sink;
  - sync and streaming have exactly one source;
  - scheduling is external;
  - old modes are not migrated.
- Defined transform constraints by mode:
  - batch accepts general operators;
  - sync incremental accepts only metadata-preserving operators;
  - streaming requires streaming capability declarations;
  - stateful streaming requires watermarks and a durable checkpoint.
- Removed old mode code and dead paths.
- Updated design docs, READMEs and examples.
- Kept the explicit no-legacy-migration boundary.

### 2026-11-02

Theme: checkpoints, object storage, streaming state and the maintenance control plane

Completed:

#### Capability manifest

- Generated `capabilities.yaml` in the distribution.
- Upgraded manifest schema to 2.1.
- Added:
  - `verify`;
  - `show`;
  - `diff`;
  - capability-set hash.
- Described:
  - sources, sinks and transforms;
  - connection providers;
  - catalog providers;
  - execution modes;
  - connection and catalog types;
  - jar and module.
- Added third-party extension through
  `META-INF/spark-pudding/component.yaml`.

#### Checkpoints

- Added `checkpointRetention`:
  - `keep`;
  - `delete-on-success`.
- Added `bin/pudding checkpoint`:
  - `show`;
  - `status --json`;
  - `delete`;
  - `cleanup --older-than --keep-last`.
- Added Hadoop-only checkpoint operations, later extended to object stores.
- Restricted `delete-on-success` to finite triggers.

#### Object-store state and checkpoints

- Added S3A checkpoint/state support.
- Added OSS, Azure ABFS/WASB, GCS and COS runtime packs.
- Added checkpoint CLI options:
  - `--conf-file`;
  - `--hadoop-conf`.
- Documented object-store constraints:
  - dedicated prefix;
  - single writer;
  - no lifecycle auto-expiry;
  - prefer HDFS, NFS or a persistent volume.

#### Streaming observability

- Added streaming metrics:
  - `streaming.query.started`;
  - `streaming.query.progress`;
  - `streaming.query.completed`;
  - `streaming.query.failed`;
  - `streaming.query.last.progress.timestamp.ms`;
  - `streaming.source.lag.*`;
  - `streaming.source.healthy`;
  - `streaming.state.*`;
  - watermark and lag metrics.
- Added a JMX MBean.
- Added structured logs.

#### Stateful streaming

- Added:
  - event-time window aggregation;
  - watermark-bounded deduplication;
  - inner stream-stream joins;
  - watermark-bounded state retention.

#### Maintenance API and CLI

- Added `maintenance-api`:
  - `TableMaintenanceService`;
  - `MaintenanceRequest` / `MaintenanceResult`;
  - action definitions and validation.
- Added `maintenance-spark`:
  - Iceberg status, rewrite, expire and orphan removal;
  - Paimon status, compact, expire and orphan removal;
  - Hudi status, clean, compact, cluster and archive.
- Added `maintenance-cli`:
  - `bin/pudding maintenance ...`;
  - `--dry-run`;
  - `--confirm`;
  - `--json`;
  - exit codes 0/1/2.
- Added the Chinese README.
- Added Testcontainers and MinIO/S3A regressions.

### 2026-11-03

Theme: configuration contract, maintenance HTTP, streaming state and public CLI

Completed:

#### Unified Spark configuration

- Added `SparkConfigResolver`:
  - unifies `job.config`, connection, catalog and runtime settings;
  - defines precedence;
  - fails on duplicate-key conflicts before startup;
  - replaces the old double application of client and driver settings.
- Documented the resolution chain:
  - job configuration;
  - client target;
  - catalog properties;
  - connection properties;
  - runtime defaults.

#### Object-store runtimes

- Added:
  - `hadoop-oss-runtime`;
  - `hadoop-azure-runtime`;
  - `gcs-runtime`;
  - `hadoop-cos-runtime`.
- Connected Iceberg's data layer to `S3FileIO`, separate from S3A checkpoints.
- Added MinIO/S3FileIO tests.

#### Maintenance HTTP control plane

- Added `maintenance-server`:
  - `GET /healthz`;
  - `GET /v1/capabilities`;
  - `POST /v1/maintenance`;
  - `GET /v1/maintenance`;
  - `GET /v1/maintenance/{id}`;
  - `POST /v1/maintenance/{id}/cancel`.
- Added:
  - bearer-token authentication;
  - `Idempotency-Key`;
  - durable task state;
  - JSONL audit logs;
  - single-worker concurrency limits.
- Running Spark actions are not forcibly interrupted to avoid partial table commits.

#### JDBC / MySQL streaming upsert

- Added JDBC streaming `outputMode=update`:
  - `recordKeyFields`;
  - target primary-key/unique-index validation;
  - staging table;
  - PostgreSQL/H2 `MERGE`;
  - MySQL/MariaDB keyed update plus insert;
  - one transaction;
  - null/duplicate key validation.
- JDBC streaming still rejects `cdcOpColumn`; delete propagation is limited to sync
  incremental CDC.

#### Streaming state and metrics

- Added `StreamingStateContract`.
- Added streaming `update` / `upsert` output.
- Added Kafka lag and health metrics.
- Added state metrics.

#### CLI and configuration

- Consolidated scripts under `bin/pudding`.
- Removed old shims and duplicate entry points.
- Removed `job.enableHiveSupport`.
- Hive support now flows through `job.config`.
- Added breaking-change documentation.
- Ran a code audit, dead-code cleanup and documentation update.

### 2026-11-04

Theme: Lakehouse semantics, native CDC and JDK 11/17

Completed:

#### Iceberg

- `iceberg-sink` upsert/CDC:
  - `recordKeyFields`;
  - `cdcOpColumn`;
  - `foreachBatch` plus `MERGE INTO`;
  - latest change per key;
  - append transactions keep the snapshot marker and fencing token;
  - merge paths do not write the fencing marker, documented as a limitation.
- `iceberg-source` streaming and upsert.
- Documented that Iceberg 1.5 has no row-level changelog stream; deletes require sync
  incremental CDC.

#### Paimon

- Added Paimon catalog streaming sink support.
- Paimon 1.3.2 and Spark 3.4 have no native catalog `toTable` streaming writer:
  - append uses the resolved location;
  - update/CDC use the direct primary-key writer;
  - `_row_kind_` carries row operations.
- Paimon full refresh remains non-atomic and requires `allowNonAtomicReplace`.
- The direct writer does not apply sink `tableOptions`.

#### Hudi

- Added Hudi catalog incremental reads.
- Added Hudi catalog streaming writes.
- Added atomic Hudi full refresh:
  - `insert_overwrite_table` replace commit;
  - COW and MOR;
  - failed-refresh rollback tests.

#### Lakehouse regressions

- Added local Spark tests for Hudi, Paimon and Iceberg.
- Added Hudi/Paimon MinIO/S3A tests:
  - table data;
  - timeline/snapshot;
  - incremental reads;
  - full refresh;
  - checkpoints.
- Added Iceberg S3FileIO tests.

#### Native CDC

- Added `database-cdc-source`:
  - `mysql-cdc-source`;
  - `postgres-cdc-source`;
  - `mongodb-cdc-source`.
- Built on Debezium 1.9.8 and MongoDB change streams:
  - initial snapshot;
  - binlog / WAL / resume token;
  - `I/U/D`;
  - `_cdc_timestamp`;
  - `_cdc_sequence`;
  - `_cdc_table`.
- Documented:
  - streaming only;
  - one table or collection per source instance;
  - explicit schema;
  - DDL is not emitted as rows;
  - interrupted MySQL snapshots are re-run;
  - PostgreSQL slots are not automatically dropped.
- Added CDC → Hudi/Paimon/Iceberg end-to-end container tests.
- Added MySQL/PostgreSQL/MongoDB CDC container tests.

#### JDK

- Dropped JDK 8.
- Added JDK 11 and JDK 17 support:
  - Java release 11;
  - Maven Enforcer rejects JDK 8 and JDK 18+;
  - JDK 17 profile;
  - child JVM `--add-opens`;
  - JDK 17 compilation and targeted tests in CI.
- Evaluated Spark upgrades and retained the Spark 3.4.0 baseline.

#### Engineering cleanup

- Removed unused imports and updated comments and READMEs.
- Added the compatibility matrix and object-store test documentation.

### 2026-11-05

Theme: audit, checkpoint safety, Schema Registry and JDBC incremental/CDC

Completed:

#### Code audit and reliability fixes

- Removed compiler-visible unused imports, local definitions, unreachable branches and dead
  members.
- CDC:
  - `Long` timestamps use epoch milliseconds;
  - numeric conversion uses exact `BigInt` with range checks;
  - heartbeat/transaction metadata records are skipped;
  - JSON null uses `NullNode`;
  - the Debezium sequence advances only when a row is appended;
  - checkpoint offsets read only the latest committed batch;
  - schema-history paths are handled inside try/finally;
  - MongoDB updates with a null `fullDocument` are skipped.
- Core:
  - redact component exceptions;
  - pass trigger information into the source context;
  - warn when incremental-lock release fails;
  - include attempts/failure in `IncrementalState.isEmpty`;
  - do not latch failed JMX registration;
  - recognize `cosn`/`cos` object-store state paths;
  - mark unreadable state locks as `<unreadable>`;
  - trim whitespace in `JobConfigureParser`;
  - refuse filesystem roots and non-checkpoint directories in checkpoint cleanup.
- JDBC:
  - `tableExists` treats only recognized missing-table states as absent;
  - connection and permission errors propagate;
  - added `JdbcTablesTest`.
- Maintenance:
  - escape backslashes before quotes in SQL literals;
  - fix the Paimon SQL `-Xlint` issue;
  - stop the Spark session when Hadoop setup fails;
  - reject unsupported retention flags;
  - map duration/size overflow to usage errors.
- Client/Kubernetes:
  - report FAILED when driver-pod deletion is unconfirmed;
  - treat deleting an absent pod as idempotent success;
  - wait for secret cleanup during `SubmissionManager.close`;
  - fix Secret namespace handling in `k8s-submit`;
  - handle empty `compgen` results in shell scripts.
- Lakehouse:
  - Iceberg snapshot-sequence ordering;
  - drop internal columns from upsert output;
  - reject duplicate keys when no order fields exist;
  - reject null merge keys;
  - set Paimon `supportsAtomicReplace=false`;
  - align direct-writer columns to the table schema;
  - validate empty Hudi/Paimon operations;
  - validate duplicate Hudi/Paimon keys.

#### Checkpoint behavior on external stop

Problem:

```text
Natural completion: exception=None, isActive=false
External query.stop(): exception=None, isActive=false
```

The old implementation could not distinguish them and could delete the checkpoint, causing
data replay.

Fix:

- `available-now`:
  - require a final zero-row progress;
  - require its batch id to be greater than the latest committed offset batch id;
  - zero-row cleanup batches commit offsets and are not accepted as completion evidence.
- `once`:
  - require the single batch to be committed and to have emitted progress.
- External stop before evidence:
  - keep the checkpoint.
- Failure, timeout and non-finite triggers:
  - keep the checkpoint.
- Added `StreamingCheckpointCompletionTest`.

#### Kafka Schema Registry / Avro / Protobuf

Added:

- `runtime/kafka-schema-registry-runtime`;
- `ConfluentValueDecoder`;
- `FixedSubjectNameStrategy`;
- `valueFormat: avro|protobuf` in `kafka-source`.

Supported:

- Confluent wire format;
- Avro and Protobuf;
- default subject derived from the topic;
- fixed subject;
- basic authentication;
- TLS and Schema Registry properties;
- Avro nested records, arrays, maps, unions, decimals, dates and timestamps;
- Protobuf scalars, enums, bytes, nested messages, repeated fields, maps and `Timestamp`;
- `cdcFormat: op-column`;
- `includeMetadata` and `keepRawValue`.

Example:

```yaml
source:
  - type: kafka-source
    id: orders
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
        subject: orders-value
        properties:
          basic.auth.credentials.source: USER_INFO
```

Implementation details:

- the connector loads the optional runtime by reflection;
- the runtime uses Confluent 7.4.0;
- Confluent/Avro/Protobuf/Jackson/Guava dependencies are shaded under
  `org.pudding.kafka.schema.shaded.*`;
- the client adds the runtime only when `valueFormat=avro|protobuf`;
- JSON-only Kafka jobs do not load the 18 MB runtime.

Limitations:

- `valueSchema` remains explicit;
- Spark schemas are not derived automatically from registry schemas;
- `badRecordsPath` and `includeCorruptRecord` remain JSON-only;
- Debezium `before`/`after` remains JSON-only;
- Protobuf well-known types currently focus on `Timestamp`.

#### JDBC incremental source

`jdbc-source` / `mysql-source` now support:

```yaml
source:
  - type: jdbc-source
    id: orders
    config:
      url: jdbc:postgresql://db:5432/analytics
      table: public.orders
      user: pudding_ro
      password: ${secret:env/ORDERS_DB_PASSWORD}
      cursorField: updated_at
      incremental:
        semantics: upsert
        keyFields: [id]
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
```

Capabilities:

- `append` and `upsert`;
- one cursor column;
- numeric, date, time, timestamp and string cursors;
- `initial: full|latest`;
- `incremental.start` and `incremental.end`;
- top-level `cursorField` participates in the plan fingerprint;
- table and cursor identifiers are quoted from JDBC metadata;
- cursor values are escaped by JDBC type;
- upsert keeps the latest row per key in the range;
- table snapshots do not expose deletes;
- no composite cursors;
- no resumable snapshots.

#### JDBC CDC sink

`jdbc-sink` / `mysql-sink` support sync incremental:

```yaml
sink:
  - type: jdbc-sink
    id: orders_target
    dependency: orders_cdc
    config:
      url: jdbc:postgresql://warehouse:5432/warehouse
      table: public.orders
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
      cdcOpColumn: __op
      cdcOrderFields: [_cdc_timestamp, _cdc_sequence]
```

Behavior:

- keep the last operation per key;
- `D` executes delete;
- `I/U` execute upsert;
- delete and upsert share one database transaction;
- staging tables hold upsert rows and delete keys;
- PostgreSQL/H2 use `MERGE` plus `DELETE ... WHERE EXISTS`;
- MySQL/MariaDB use keyed `UPDATE JOIN`, `INSERT ... WHERE NOT EXISTS`, and
  `DELETE ... JOIN`;
- the target must have a matching primary key or unique index;
- upsert/CDC replay is idempotent;
- append replay is not guaranteed to be idempotent;
- streaming JDBC still rejects `cdcOpColumn`.

#### Version

- Maven project version changed from `0.0.2-SNAPSHOT` to `1.0.0-SNAPSHOT`.
- Git branch renamed from `v-0.2` to `v-1.0`.
- Parent/dependency versions in every POM, README examples, jar names, Kubernetes image
  tags and capability-manifest examples were updated.
- `@deprecated` version markers now use `1.0.0`.

#### Documentation and examples

- Updated the English/Chinese READMEs, design docs, matrices and module READMEs.
- Added:
  - `build/examples/incremental_jdbc_to_jdbc.yaml`;
  - `build/examples/read_kafka_avro_example.yaml`.
- Added the English and Chinese CLI guides `build/doc/cli.md` and
  `build/doc/cli.zh-CN.md`, covering every `bin/pudding` subcommand with runnable examples.
- Added a `pudding submit` section to both READMEs, covering attached/background submission,
  target inference, exit codes and client-jar overrides.
- Updated runtime, client, compatibility, JDBC and Schema Registry docs.
- Added `RELEASE_NOTES.md` and this English version.

### 2026-11-06

Theme: local end-to-end smoke and CDC fixes

- **Local client failure propagation**:
  - `LocalTarget` now uses `startProcess(launcher)` and reads the `spark-submit`
    child process exit code;
  - `ProcessRunningApplication` returns `FINISHED` for non-Kubernetes targets
    when the exit code is zero, instead of reporting `LOST`;
  - before the fix `pudding submit` returned 0 when the driver failed; it now
    returns non-zero.
- **Elasticsearch Scala 2.12 compatibility**:
  - `elasticsearch-runtime` now depends on
    `elasticsearch-spark-30_2.12:8.9.1` instead of `elasticsearch-hadoop:8.9.1`;
  - the previous artifact was compiled against Spark 2.3 / Scala 2.11 and failed
    to load on Spark 3.4;
  - the ES source/sink local smoke cases pass again.
- **Two MongoDB CDC fixes**:
  - the first change event after the initial snapshot reused the last snapshot
    sequence and was rejected by `CdcRecordBuffer` as a non-increasing CDC
    sequence; the poller now allocates the next sequence before encoding;
  - the first change stream no longer uses `startAtOperationTime` from `hello`
    (whose cluster time can lag the last oplog write). It starts at the cursor
    creation point, so DDL that happened before the snapshot (for example the
    rename produced by an aggregation `$out`) is not replayed. Resuming an
    interrupted snapshot still replays from the recorded `startAt`.
- **JDBC incremental upsert keeps payload ordering columns**:
  - `IncrementalFrames.latestByKey` no longer drops real payload columns that
    are also ordering fields (for example a JDBC `updated_at` cursor). It drops
    only internal metadata columns whose names start with `_`;
  - this fixes sync-incremental JDBC writes failing with "missing target
    column" because the cursor column had been removed before the sink aligned
    the row to the target table.
- **CDC transient connection retry and reconnect**:
  - `DebeziumCdcPoller` now recognises retriable connection failures
    (`RetriableException`, SQLState `08xxx`, `ConnectException`,
    `SocketException`, `EOFException`, MySQL `CommunicationsException`,
    timeouts);
  - Debezium stops its internal task after a connection failure, so the poller
    closes and recreates the connector/task and resumes from the source offset
    of the last emitted record, retrying every two seconds;
  - a database restart or a short network partition no longer terminates the
    streaming query;
  - configuration, authentication and schema errors still fail immediately;
  - Debezium's binlog client may also reconnect on its own; in RG-05 it
    recovered about two minutes after the MySQL restart, with the poller-level
    recreate as the fallback for errors that do not self-heal.
- **JDBC/MySQL streaming CDC writes**:
  - `jdbc-sink` and `mysql-sink` streaming writes now accept `cdcOpColumn`;
  - each micro-batch applies deletes and upserts in one database transaction
    through private staging tables;
  - it requires `recordKeyFields`, a matching primary key or unique index on
    the target, `outputMode: append` and `checkpointLocation`;
  - without `cdcOpColumn`, `outputMode: update` still upserts complete rows and
    does not delete target rows.
- **JDBC incremental read parallelism**:
  - a JDBC read without `partitionColumn` produces one partition, so the
    dedup window and write could land in a single task;
  - `JdbcIncremental` now repartitions by the business keys to
    `spark.sql.shuffle.partitions` before the window;
  - the 10,000,000-row PostgreSQL to JDBC scale case no longer OOMs in a
    single-task write.
- **Paimon streaming append to an existing table**:
  - the streaming writer no longer receives table-creation properties
    (`primary-key`, `bucket`, `tableOptions`); only runtime options from
    `options` are passed;
  - this fixes pre-created Paimon targets failing with
    "Change 'primary-key' is not supported yet" for Kafka Avro/Protobuf jobs;
  - the streaming frame is aligned to the table schema by column name before
    the append, so a reordered `valueSchema` map cannot shift the primary-key
    column.
- `pudding-test` P0 component smoke: 30/30 finite batch cases passed and the
  E2E-01 transform chain passed. CDC smoke passed for SR-11 (MySQL), SR-12
  (PostgreSQL, 10,000-row initial snapshot) and SR-13 (MongoDB, 10,000-row
  snapshot plus I/U/D). The earlier MySQL 1236 error in SR-11 came from a
  timed-out Spark driver that kept the Debezium `server_id` alive; the local
  runner now terminates the whole submit process group. E2E-02
  (union/join/dedup), E2E-03 (validate/quarantine/schema contract), E2E-04
  (JDBC incremental upsert), E2E-05 (MySQL CDC to Paimon), E2E-06 (MySQL CDC to
  Hudi/Iceberg), E2E-07 (MongoDB CDC to Paimon), E2E-08 (PostgreSQL CDC to
  JDBC), E2E-09 (Kafka to Hudi with checkpoint restart), E2E-10 (Kafka
  Avro/Protobuf with Schema Registry to Paimon), E2E-11 (window aggregate and
  dedup), E2E-12 (Hudi to Paimon and Iceberg to Hudi incremental sync),
  E2E-13 (Hive/Kafka/MongoDB/Elasticsearch round trips), E2E-14 (full refresh across
  file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg), RG-01 (kill -9 recovery),
  RG-02 (concurrent lock), RG-03 (pending transaction abort/commit), RG-04
  (Kafka broker restart), RG-05 (CDC resume after a MySQL restart) and RG-07
  (state CLI status/reset/unlock/migrate/list) all passed. Negative cases
  NG-01/02/03/04/05/07/08 (multiple sinks, unknown dependency, unsupported
  streaming source, invalid outputMode, missing `url`, missing contract column,
  missing secret) fail before startup and name the offending key without
  exposing credentials.

### 2026-11-07

Theme: object-store checkpoints/state, Iceberg S3FileIO and checkpoint CLI fixes

- **Secret references in job.config conflicted with spark-submit forwarding**:
  - the client forwards `job.config` entries verbatim as `spark-submit --conf`
    arguments, so the driver's runtime SparkConf still contains the unresolved
    `${secret:...}` value while `BuildJob` has already resolved `job.config`;
    `SparkConfigResolver` treated the two as conflicting values and refused to
    start;
  - the driver now resolves secret references in the runtime SparkConf as well
    before comparing and merging;
  - this supports credentialed Spark/Hadoop settings such as
    `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` in
    `job.config`; logs still show `******`.
- **Checkpoint age on object stores**:
  - S3A synthesizes a directory modification time from the current clock.
    `CheckpointFiles.lastModified` included directory times, so
    `checkpoint status` reported `ageSeconds` near 0, `cleanup --older-than`
    never expired anything, and `--keep-last` could retain the wrong
    checkpoint;
  - age is now computed from file times only; an empty directory still falls
    back to its own time so it can age out;
  - a regression test covers a checkpoint whose directories are current while
    its files are old.
- **Object-store end-to-end verification** (pudding-test):
  - S3A checkpoint: 10 rows on the first run, still 10 after a restart, and 12
    after a new file was added;
  - S3A incremental state: rejected before startup without
    `allowBestEffortLock`; with it enabled, 100 then 102 rows with no
    duplicates, and `state status` showed a committed cursor, no pending
    transaction and no lock;
  - Iceberg JDBC catalog plus `S3FileIO`: wrote and read 1,000 rows with the
    warehouse at `s3://pudding/warehouse/os-02`;
  - two scoped MinIO users proved bidirectional credential isolation between
    S3A and S3FileIO (cross-reads returned 403), and a write with the wrong
    credentials failed with 403 in the S3FileIO data layer;
  - checkpoint CLI JSON fields, `delete --dry-run`, `cleanup --keep-last`,
    `--older-than`, invalid `--conf-file`/duration handling and the safety
    refusals all passed;
  - rendered job files and submit logs contain only `${secret:file:...}`
    references, never plaintext credentials.
- **Scale gate E2E-14 (1.3 GiB Iceberg full refresh)**:
  - an existing 200-row table was atomically replaced with 10,000,000 rows by
    `sync full`;
  - the Iceberg commit reported `removedDataFiles=1`, `removedRecords=200`,
    `addedDataFiles=11` and `addedRecords=10000000`, and the read-back count
    matched.
- **Scale gate E2E-06 (10,000,013-row MySQL CDC initial snapshot into Hudi)**:
  - the first run wrote 6,200,031 rows before the local Hudi HTTP marker
    server address became unreachable after the host network address changed;
  - the run resumed from the checkpoint with
    `hoodie.write.markers.type=DIRECT` and `upsert`; the Debezium snapshot
    replay updated existing rows by `order_id` idempotently;
  - the final Hudi table contained exactly 10,000,013 rows with no duplicates;
  - this also exercised the CDC queue-capacity fix (capacity at least
    `maxRecordsPerBatch + 1`) and the Hudi empty-microbatch fix.
- **Scale gate E2E-09 (10,000,000 Kafka JSON messages into Hudi)**:
  - a 4-partition `orders-scale` topic produced at about 301k messages/s,
    with every message delivered;
  - the streaming bulk insert with `maxOffsetsPerTrigger=500000` consumed
    10,000,000 messages; the Hudi table held 10,000,000 rows and 484 MB;
  - `hoodie.write.markers.type=DIRECT` keeps long runs independent of the
    local HTTP marker server.
- **E2E-11 self stream-stream join**:
  - one Kafka source produces PAID left/right streams, joined on `order_id`
    as an inner stream-stream join with one-minute watermarks on both sides;
  - the first run emitted 5 rows and a checkpoint restart emitted no
    duplicates.
- **Schema Registry subject and error path**:
  - a custom subject decoded 1,000 Avro rows;
  - the Confluent wire-format schema id is authoritative;
    `schemaRegistry.subject` is not required for id-based messages, and a
    missing subject does not block decoding;
  - an unknown schema id 999999 fails with the topic and id in the error.
- **RG-06 checkpoint retention semantics**:
  - `delete-on-success` with `available-now` removed the checkpoint after the
    finite input drained;
  - an external stop through `timeoutMs` kept the checkpoint and logged
    "did not prove that its input was drained";
  - the direct `query.stop()` path is covered by the core unit suite.
- **NG-06 streaming CDC validation**:
  - an invalid op value returns `unsupported cdc operation in op: X`;
  - a missing op column returns
    `cdc input is missing operation column: missing_op`;
  - a batch without `cdcOrderFields` or Kafka/Debezium ordering metadata is
    rejected.
- **Hudi streaming append with empty microbatches**:
  - Hudi updates its metadata table on every streaming commit, and a
    zero-record microbatch failed with
    `Positive number of partitions required`;
  - empty batches are normal while a CDC snapshot starts or a Kafka backlog
    drains;
  - the append path now uses `foreachBatch` and commits the offset without
    writing to Hudi when the batch has no rows;
  - covered by the 10,000,000-row MySQL CDC initial snapshot case.
- **CDC queue-capacity self-deadlock**:
  - the default `queueCapacity` equalled `maxRecordsPerBatch` when
    `maxRecordsPerBatch >= 10000`, so one batch could fill the queue exactly;
  - Spark commits the previous source offset only while constructing a batch
    with new data; with a full queue the poller blocked, no new data arrived,
    and the previous batch was never committed;
  - the effective capacity is now at least `maxRecordsPerBatch + 1`, leaving
    one record of headroom;
  - found and covered by the 10,000,000-row MySQL CDC initial snapshot case,
    with a capacity regression test in `CdcSourceOptionsTest`.
- **OS-03 to OS-08 operations cases** (pudding-test):
  - HDFS checkpoint/state recovered after NameNode/DataNode restarts;
  - JDBC/Redis/etcd state backends covered migrate, status, reset, unlock and
    fencing;
  - Iceberg/Paimon/Hudi maintenance covered status/compact/expire/orphan/
    archive, `--confirm`/`--dry-run` and the safety refusals;
  - maintenance-server covered token auth, idempotency, cancellation, audit
    and the confirmation guard;
  - capabilities covered verify/show/diff plus missing-jar and
    missing-manifest detection.

### 2026-11-08

Theme: code audit, documentation and configuration cleanup

- Compiled every module with `-Ywarn-unused`:
  - removed the unused `database` parameter from the MongoDB change stream;
  - cleaned unused pattern variables in CDC encoding and incremental state reads;
  - kept only the signature parameters required by SPI default methods (for example
    `DataSink.isIdempotentIncremental(semantics)`);
- confirmed that every main component class is referenced by `META-INF/services` or a CLI
  script, with no unregistered source/sink/transform/provider;
- updated the comments and docs for CDC queue capacity, Hudi empty microbatches, checkpoint
  age and the Kafka Schema Registry subject;
- added `JAVA_HOME`/`SPARK_HOME` examples and the JDK 11/17 note to `conf/env.properties`.

### 2026-11-11

Theme: code-review P0/P1 fixes

- Fixed the P0 data-correctness findings: Hudi CDC images now decode Avro DATE, TIMESTAMP,
  DECIMAL and BINARY values instead of using `from_json`; deduplication is rejected in sync
  incremental pipelines; Iceberg change ordering includes the insert/delete priority; Iceberg
  merge paths enforce and persist fencing; Kafka CDC ordering uses partition and offset before
  the event timestamp; MongoDB binary, ObjectId and Decimal128 keys are encoded consistently;
  Hudi append replay checks the archived timeline.
- Fixed the P1 failure and configuration findings: JDBC table probes fall back to
  case-insensitive metadata, YARN client submissions use the spark-submit exit status, JDBC CDC
  validates upsert keys, redaction replacement strings are quoted and fingerprint serialization
  fails closed, file sink checkpoint options are honored, and batch lakehouse CDC honors the
  sink's `cdcOrderFields`.
- Added regression tests for every fix. The Hudi CDC decoder is a serializable Catalyst UDF so
  the full test reactor does not depend on Spark's closure cleaner for the decoder.
- Follow-up audit fixes: the Hudi CDC decoder distinguishes Avro CDC-log images from
  InternalRow JSON, decodes base64 binary and nested values, and fails closed on lossy DECIMAL;
  DECIMAL text parsing is byte-exact; Iceberg MERGE fencing stores and checks the transaction id;
  unreadable archived Hudi commit metadata fails closed.
- Addressed the remaining audit Minors: the Hudi archived replay scan no longer bounds the target
  timeline by a source cursor; file checkpoint paths are normalized; JDBC metadata matching uses
  catalog/schema; JDBC upsert honors `cdcOrderFields`; Iceberg snapshot sequence lookup uses a
  broadcast UDF; `PipelineValidator` checks only transforms reachable from the incremental sink.
- Full `mvn clean verify` on JDK 11 passed after the audit follow-up (61 modules, 14m08s).

### 2026-11-12

Theme: YARN cluster lifecycle and current-code benchmark baseline

- YARN cluster submissions now read the application id from spark-submit output and track the
  application through the ResourceManager REST API. The endpoint comes from `yarn-site.xml`
  (`YARN_CONF_DIR`/`HADOOP_CONF_DIR`) or `PUDDING_YARN_RM_URL`; a missing web application address
  fails before launch.
- Product decision: Hudi CDC keeps the fail-closed DECIMAL behavior for InternalRow JSON images.
  Hudi does not preserve the value there, so the source reports an error instead of writing a
  wrong number. Base-file recovery is deferred until it is required.
- Local current-code scale baseline on the testbed: E2E-01 23s; Iceberg/Hudi/Paimon 1.3 GiB full
  refresh 25s/244s/27s; E2E-04 10M PostgreSQL incremental 163s; E2E-06 10M MySQL CDC to Hudi
  3037s and to Iceberg 1021s; E2E-09 10M Kafka to Hudi 195s. These are single-node local
  measurements, not production SLAs.

---

## Compatibility and upgrade notes

### Version

- The Maven project version changed from `0.0.2-SNAPSHOT` to `1.0.0-SNAPSHOT`.
- The Git branch was renamed from `v-0.2` to `v-1.0`.
- Distribution names, jar names and example paths use `1.0.0-SNAPSHOT`.
- The remote `origin/v-0.2` branch is not modified or deleted automatically.

### Job modes

Old modes:

```text
full / incremental / etl
```

Replaced by:

```text
batch / sync / streaming
```

Old configurations are not migrated and fail at startup.

### Sink constraint

Every mode accepts exactly one sink. Split multi-target jobs into separate jobs.

The framework does not include an internal scheduler. Use Airflow, cron, Kubernetes CronJob or
another external system.

### JDK

- JDK 8 is no longer supported.
- JDK 11 and JDK 17 are supported.
- Maven rejects JDK 8 and JDK 18+ during `validate`.

### Hive

- `job.enableHiveSupport` was removed.
- Pass Spark settings through `job.config`:

```yaml
job:
  config:
    spark.sql.catalogImplementation: hive
```

### Public CLI

All entry points are now:

```text
bin/pudding
```

Old shims and duplicate scripts were removed.

### Capability manifest

The manifest schema is now 2.1. Backends should store a manifest per distribution version and
use `diff` to compare added, removed and changed components.

### Checkpoints

`delete-on-success` now requires completion evidence:

- `available-now`: the final zero-row progress must have a batch id greater than the latest
  committed offset;
- `once`: the single batch must be committed and must have emitted progress.

External stops, failures and timeouts keep the checkpoint.

### Schema Registry

Avro/Protobuf require an explicit `valueSchema`. The Spark schema is not inferred from the
registry schema.

A direct `spark-submit` job must include:

```text
libs/kafka-runtime-<version>.jar
libs/kafka-schema-registry-runtime-<version>.jar
```

### JDBC incremental

`cursorField` belongs to the top-level source config, not the `incremental` block:

```yaml
config:
  table: public.orders
  cursorField: updated_at
  incremental:
    semantics: upsert
    keyFields: [id]
    initial: full
```

JDBC table snapshots cannot observe deletes. Use a CDC source when deletes must be propagated.

### Upgrade checklist

- Update dependencies, image tags and distribution paths to `1.0.0-SNAPSHOT`.
- Convert old job modes to `batch`, `sync` or `streaming`.
- Confirm every job has exactly one sink.
- Upgrade to JDK 11 or JDK 17.
- Move Hive settings into `job.config`.
- Switch scripts to `bin/pudding`.
- Regenerate or verify the capability manifest.
- Review `delete-on-success` checkpoint policies.
- Add the Schema Registry runtime for Kafka Avro/Protobuf jobs.
- Configure top-level `cursorField` for JDBC incremental jobs.

---

## Known limitations

- General streaming DLQ/quarantine is not integrated into the same job.
- No Prometheus `/metrics`, OpenTelemetry, or streaming `/healthz` / `/readyz` endpoints.
- CDC does not cover Oracle, SQL Server or multi-table include lists.
- CDC schema changes still require configuration changes and a new checkpoint.
- Streaming joins are inner-only; outer/semi/anti joins and advanced state timers are not
  covered.
- Secrets are still limited to environment variables and files; Vault, AWS/GCP/Azure Secret
  Manager are not integrated.
- Iceberg REST/Glue/Nessie/Unity catalogs are not covered.
- Lakehouse table creation, automatic catalog registration and schema-evolution policy are not
  yet an end-to-end flow.
- Paimon catalog full refresh remains non-atomic and opt-in.
- YARN cluster lifecycle tracking uses the ResourceManager REST endpoint without Kerberos
  authentication; protected clusters need an authenticated gateway.
- Hudi CDC fails closed for DECIMAL values in base-file/replace-commit images (including
  `bulk_insert` and insert-only commits), because Hudi's InternalRow JSON stores only a lossy
  zero/non-zero marker. Binary CDC on tables with `hoodie.populate.meta.fields=false` is
  rejected because the Avro and InternalRow encodings can not be distinguished.
- File/Hive full refresh is not reader-atomic.
- Live OSS/Azure/GCS/COS endpoints and MySQL/PostgreSQL CDC sink container regressions still
  need a complete CI environment.
- JDBC incremental supports only one cursor column; composite cursors, delete detection and
  resumable snapshots are not supported.
- Schema Registry Protobuf well-known type support is limited; `Timestamp` is the main supported
  type.

---

## Quality verification

Completed during the release:

- Full `mvn clean verify` on JDK 11: passed (2026-11-08, 61 modules, zero
  failures; 7h44m wall clock including host sleep);
- Full `mvn clean verify` after the code-review P0/P1 fixes and the follow-up audit:
  passed (2026-11-11, 61 modules, zero failures, 14m08s);
- Full `mvn clean verify` after YARN cluster tracking and the final v1.0 review:
  passed (2026-11-12, 61 modules, zero failures, 13m50s);
- Local object-storage, HDFS, table-maintenance, maintenance-server and
  capability cases passed (OS-01 to OS-08, E2E-15);
- Scale gates: 1.3 GiB transform, 10,000,000-row JDBC incremental,
  1.3 GiB Iceberg full refresh, a 10,000,013-row MySQL CDC initial
  snapshot into Hudi and 10,000,000 Kafka JSON messages into Hudi;
- JDK 17 targeted tests:
  - Schema Registry runtime;
  - Kafka source;
  - JDBC incremental source;
  - JDBC CDC/streaming;
  - child-JVM crash recovery;
- core, CDC, JDBC, maintenance, client and lakehouse regression suites;
- H2 JDBC incremental/CDC tests;
- Mock Schema Registry Avro/Protobuf tests;
- MinIO/S3A, HDFS, Redis, etcd and Testcontainers suites are enabled by environment.

Container tests are skipped when Docker is unavailable, following the project convention.
`PUDDING_TESTCONTAINERS_REQUIRED=true` makes CI fail instead of skipping.

---

## Next steps

Recommended production hardening order:

1. Prometheus/OTel plus streaming health endpoints.
2. General streaming DLQ/quarantine.
3. Secret Manager and maintenance-server TLS/RBAC.
4. Oracle/SQL Server CDC and DDL/schema evolution.
5. Automatic lakehouse table creation, registration and schema evolution.

Deliberately not recommended:

- an internal scheduler;
- multi-sink global transactions;
- automatic migration of old configurations;
- binding Gravitino as a required SDK;
- putting all connector SDKs into core.
