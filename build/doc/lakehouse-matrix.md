# Lakehouse capability matrix

This page summarizes the supported source, sink, catalog and full-refresh behavior for Apache
Hudi 0.14.0, Apache Iceberg 1.5.0 and Apache Paimon 1.3.2 on Spark 3.4. It is a companion to
[incremental-sync.md](incremental-sync.md) and [streaming-state.md](streaming-state.md).

## Sources

| Source | Batch | Streaming snapshots | Append incremental | Upsert incremental | CDC incremental |
| --- | --- | --- | --- | --- | --- |
| `hudi-source` | yes | yes (path or catalog) | no | yes | yes |
| `iceberg-source` | yes | yes (path or catalog) | yes | yes | yes |
| `paimon-source` | yes | yes (path or catalog) | yes | no | yes |

- Hudi incremental reads accept `tablePath` or resolve the base path of a Hudi catalog table.
- Iceberg `upsert` and `cdc` incremental reads use the `.changes` metadata table and therefore
  require a catalog table. Upsert returns the latest complete row per key and fails if the last
  change for a key is a delete; use `cdc` when deletes must propagate.
- Iceberg streaming snapshots are append-file streams, not a row-level changelog. Batch
  `snapshotId`, `asOfTimestamp` and `tag` selectors are rejected in streaming mode.
- Paimon `append` incremental semantics require an append-only table; `cdc` requires a
  primary-key table and keys matching the table primary keys.

## Sinks

| Sink | Streaming append | Streaming update/upsert | Streaming CDC | Sync incremental |
| --- | --- | --- | --- | --- |
| `hudi-sink` | yes | yes (`recordKeyFields`) | yes (`cdcOpColumn`) | append, upsert, cdc |
| `iceberg-sink` | yes | yes (`recordKeyFields`) | yes (`cdcOpColumn`) | append, upsert, cdc |
| `paimon-sink` | yes | yes (primary-key table) | yes (`_row_kind_`) | append, upsert, cdc |

All three sinks require a checkpoint and support `checkpointRetention`. Streaming `update` is
intended for changed rows from a stateful transform; streaming CDC uses append output because the
operation column carries deletes.

Iceberg incremental upsert/CDC merges complete rows by key. A missing target is created from the
first batch. Append transactions keep the snapshot summary marker and fencing token; the merge
paths are idempotent by key and do not write a snapshot marker.

## Catalog and path mode

| Operation | Hudi | Iceberg | Paimon |
| --- | --- | --- | --- |
| Batch source/sink | path or catalog | path or catalog | path or catalog |
| Streaming source | path or catalog snapshot stream | path or catalog snapshot stream | path or catalog |
| Streaming sink | path or catalog location | catalog required by the sink | path or catalog location |
| Incremental source | path or catalog location | append: path or catalog; upsert/cdc: catalog | path or catalog |
| Sync incremental sink | path or catalog | path or catalog | path or catalog |

Hudi and Paimon use a catalog-to-location fallback because their native Spark streaming writers
are path-based. Hudi resolves the table location from the session catalog. Paimon uses
`FileStoreTable.location()`; `update`/CDC do not need the path, and catalog `append` uses the
resolved path with the native Paimon streaming writer. This is the Spark 3.4 / Paimon 1.3.2
compatibility path, not native catalog `toTable` streaming.

## Full refresh

| Sink | `sync` full | Atomic | Mechanism |
| --- | --- | --- | --- |
| `hudi-sink` | yes | yes | `insert_overwrite_table` replace commit; catalog mode resolves the base path |
| `iceberg-sink` | yes | yes | Iceberg overwrite preserving table schema and partition spec |
| `paimon-sink` | yes | no (opt-in) | Paimon catalog overwrite replaces the table without a staging commit; requires `allowNonAtomicReplace` and may not preserve primary-key/table properties |
| `jdbc-sink`, `mysql-sink` | yes | yes | staging table plus one transaction |
| `file-sink`, `hive-sink` | opt-in | no | `allowNonAtomicReplace: true` required |

Hudi COW and MOR replacement are covered by regression tests. A failed Hudi full refresh leaves
the previous snapshot visible; the replace commit is the publication point.

## Regression suites

| Suite | Coverage |
| --- | --- |
| `HudiSourceTest` | path/catalog batch reads, catalog incremental reads, upsert/CDC semantics. |
| `HudiSinkTest` | path/catalog writes, catalog streaming append/update, empty-microbatch handling, COW/MOR atomic full refresh, failed-refresh rollback. |
| `IcebergSourceIncrementalTest` | append/upsert/CDC changelog reads, delete rejection for upsert, full snapshot. |
| `IcebergSourceStreamingTest` | catalog/path streaming DataFrames, selector rejection, historical `stream-from-timestamp`. |
| `IcebergSinkTest` | streaming append/update/CDC, sync incremental append/upsert/CDC, atomic full refresh. |
| `IcebergS3FileIOTest` | MinIO S3FileIO data-layer write/read through an AWS SDK-backed JdbcCatalog. |
| `PaimonSourceTest` | append/CDC incremental reads and catalog reads. |
| `PaimonSinkTest` | path/catalog streaming append/update, sync incremental append/upsert/CDC, full-refresh contract. |
| `HudiObjectStoreTest` | Hudi data files, timeline commits, incremental read, full refresh, failure rollback and S3A checkpoints on MinIO/S3. |
| `PaimonObjectStoreTest` | Paimon catalog-created table data, snapshots, CDC, full refresh and S3A checkpoints on MinIO/S3. |

The container-backed object-store suites cover exactly-once state, checkpoint paths, the Iceberg
S3FileIO data path, and Hudi/Paimon table data and checkpoints. Real OSS, Azure, GCS and COS
endpoints remain opt-in environment-driven coverage; the new regression module also accepts an
existing S3-compatible endpoint through `PUDDING_TEST_S3_ENDPOINT`.

## Known boundaries

- Iceberg 1.5 has no row-level changelog Structured Streaming source. Snapshot streaming is for
  append-oriented consumers; use `sync` incremental CDC for deletes.
- Iceberg path-mode incremental CDC/upsert is unavailable because `.changes` is catalog-only.
- Paimon release 1.3.2 has no native catalog streaming sink; the sink uses a resolved location.
- Hudi `HudiSource` does not declare append incremental semantics for a primary-key table; use
  upsert or CDC.
- Paimon sink `tableOptions` apply to table creation, not to the direct CDC writer's existing
  table. Put table properties on the target table.
