# Table maintenance

The distribution's `bin/pudding maintenance` subcommand is a one-shot CLI for routine lakehouse
table maintenance. It wraps the native Spark SQL procedures of Iceberg 1.5.0, Paimon 1.3.2 and
Hudi 0.14.0; it does not start a scheduler and does not change the job mode model.

Maintenance commands are intended for an external scheduler (Airflow, cron, a Kubernetes
CronJob) and a maintenance window. They are separate from streaming checkpoints and incremental
state; the CLI never deletes checkpoints or cursor state.

## Quick start

Create a Spark properties file containing only `spark.*` keys. For an Iceberg Hadoop catalog:

```properties
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=hadoop
spark.sql.catalog.lake.warehouse=file:///tmp/pudding/iceberg
```

For Paimon:

```properties
spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse=file:///tmp/pudding/paimon
```

Run an action:

```shell
# Read-only health check
SPARK_MASTER=local[*] bin/pudding maintenance iceberg status \
  --catalog lake --table db.orders \
  --conf-file /etc/pudding/iceberg-maintenance.properties --json

# Compact small files
SPARK_MASTER=local[*] bin/pudding maintenance iceberg rewrite-data-files \
  --catalog lake --table db.orders \
  --target-file-size 128MB --min-input-files 5 \
  --conf-file /etc/pudding/iceberg-maintenance.properties

# Expire old snapshots (destructive)
SPARK_MASTER=local[*] bin/pudding maintenance iceberg expire-snapshots \
  --catalog lake --table db.orders \
  --older-than 7d --retain-last 5 --confirm \
  --conf-file /etc/pudding/iceberg-maintenance.properties

# Hudi path mode
SPARK_MASTER=local[*] bin/pudding maintenance hudi compact \
  --path hdfs:///warehouse/orders --option op=scheduleAndExecute

# Hudi table registered in a catalog
SPARK_MASTER=local[*] bin/pudding maintenance hudi clean \
  --table default.hudi_orders --retain-last 10 --confirm
```

`SPARK_MASTER` is passed to `spark-submit`; omit it when the Spark installation or
`spark-defaults.conf` already supplies a master. `SPARK_HOME` selects the Spark installation.

## HTTP control plane

The optional `maintenance-server` module exposes the same `TableMaintenanceService` over HTTP
for a platform or scheduler:

```shell
export PUDDING_MAINTENANCE_TOKEN="$(cat /etc/pudding/maintenance-token)"
SPARK_MASTER=local[*] bin/pudding maintenance-server \
  --host 127.0.0.1 --port 8080 \
  --state-file /var/lib/pudding/maintenance-tasks.json \
  --audit-file /var/lib/pudding/maintenance-audit.jsonl \
  --conf-file /etc/pudding/iceberg-maintenance.properties
```

The token is read from `PUDDING_MAINTENANCE_TOKEN` or `--auth-token-file`; it is never accepted
on the command line. `--allow-anonymous` is only for local development.

Endpoints:

| Method | Path | Meaning |
| --- | --- | --- |
| `GET` | `/healthz` | process and task counts; no auth |
| `GET` | `/v1/capabilities` | supported formats and actions |
| `POST` | `/v1/maintenance` | submit a task; returns `202` with a task id |
| `GET` | `/v1/maintenance?status=&limit=` | list recent tasks |
| `GET` | `/v1/maintenance/{id}` | task status and result |
| `POST` | `/v1/maintenance/{id}/cancel` | cancel a queued task |

Submission is asynchronous and accepts an optional `Idempotency-Key` header:

```shell
curl -sS -X POST http://127.0.0.1:8080/v1/maintenance \
  -H "Authorization: Bearer $PUDDING_MAINTENANCE_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: nightly-iceberg-expire-2026-11-04" \
  -d '{
    "format": "iceberg",
    "action": "expire-snapshots",
    "catalog": "lake",
    "table": "db.orders",
    "retention": {"olderThan": "7d", "retainLast": 5},
    "confirmed": true
  }'
```

Tasks, results and audit events are written to the configured state and audit files. The state
file contains requests and results but never Spark configuration or credentials. Only queued
tasks can be cancelled; a running Spark action is not forcibly interrupted because aborting a
table commit in the middle is not safe. Use the task status and the external scheduler to decide
whether to retry.

## Actions

### Iceberg

| Action | Native procedure | Notes |
| --- | --- | --- |
| `status` | `snapshots`, `files`, `manifests` metadata tables | snapshot/file counts, size, small-file count |
| `rewrite-data-files` | `rewrite_data_files` | `--target-file-size`, `--min-input-files`, `--where`, `--strategy` |
| `rewrite-manifests` | `rewrite_manifests` | manifest compaction |
| `expire-snapshots` | `expire_snapshots` | `--older-than`, `--retain-last` |
| `remove-orphan-files` | `remove_orphan_files` | `--older-than` required, minimum 24h, native dry-run |

### Paimon

| Action | Native procedure | Notes |
| --- | --- | --- |
| `status` | `$snapshots`, `$files`, `$partitions` system tables | snapshot/file/partition counts |
| `compact` | `sys.compact` | `--where`, `--option partitions=...`, `--option compact-strategy=full` |
| `compact-manifests` | `sys.compact_manifests` | manifest compaction |
| `expire-snapshots` | `sys.expire_snapshots` | `--older-than`, `--retain-max`, `--retain-min` |
| `remove-orphan-files` | `sys.remove_orphan_files` | `--older-than` required, minimum 24h, native dry-run |

Paimon procedures require the table to be reachable through a Paimon Spark catalog. A table
created by path must be exposed through a catalog with the matching warehouse before it can be
maintained. Paimon's default `retain_min` is 10; specify `--retain-min` when `--retain-max` is
below 10. Paimon `options` are a comma-separated `key=value` string, so an option value containing
a comma is rejected rather than silently misparsed.

### Hudi

| Action | Native procedure | Notes |
| --- | --- | --- |
| `status` | `show_commits`, `show_compaction`, `show_clustering` | COW tables report compaction as not applicable; a path summary adds file count and size |
| `clean` | `run_clean` | requires a registered `--table`; accepts one retention mode from `--retain-last`, `--older-than` or an explicit `retain-commits`/`hours-retained`/`file-versions-retained` option |
| `compact` | `run_compaction` | supports `--path`; `--option op=schedule|execute|scheduleAndExecute` |
| `cluster` | `run_clustering` | supports `--path`; `--where` becomes the clustering predicate |
| `archive` | `archive_commits` | supports `--path`; timeline retention |

A path-only Hudi table cannot use `run_clean` because the Hudi SQL procedure resolves a catalog
table. Register the table in Spark/Hive or use Hudi's own cleaner for that path. Compaction,
clustering and archive accept a path directly. `archive_commits` also requires a consistent
Hudi metadata-table state; a filesystem-backed metadata table that has not been initialized or
compacted can make Hudi's archiver fail. Keep the metadata table healthy or disable it for tables
that do not need metadata acceleration.

## Safety

- `expire-snapshots`, `remove-orphan-files` and `clean` require `--confirm` unless `--dry-run`.
- `remove-orphan-files` requires `--older-than` of at least 24h. Never run it while another
  writer may be uploading files into the table location.
- `--dry-run` executes native dry-run procedures where available; other actions print the
  statements without executing them.
- `clean` and `expire` are not automatically retried. Run them during a maintenance window or
  rely on the format's native table locking/commit protocol.
- Keep the `--conf-file` at mode `0600`; it may contain object-store or catalog credentials.
  Only `spark.*` keys are accepted, and values are never printed.
- The CLI does not delete checkpoints, cursor state or table data outside the selected action.

## Output and exit codes

`--json` returns one JSON object with `format`, `action`, `target`, `dryRun`, `executed`,
`statements`, `summary`, `rows` and `message`. Exit codes are:

- `0`: request completed; a native dry-run is `executed=false`;
- `1`: procedure or Spark execution failed;
- `2`: usage, configuration or validation error.

## Programmatic API

The CLI is an adapter over `org.pudding.maintenance.api.TableMaintenanceService`:

```scala
val service: TableMaintenanceService = new SparkTableMaintenanceService(spark)
val result = service.execute(MaintenanceRequest(
  format = "iceberg",
  action = "status",
  catalog = Some("lake"),
  table = Some("db.orders")))
```

`MaintenanceRequest`, `MaintenanceResult`, `MaintenanceActionSpec` and validation are independent
of Spark. The optional `maintenance-server` module reuses the same requests over HTTP and persists
task state and audit events without duplicating action logic; see the HTTP control plane below.

## Not included

- Scheduling and retry policy; use an external orchestrator.
- Hudi rollback/repair/savepoint and Iceberg/Paimon rollback procedures; these are expert
  recovery operations and are intentionally not exposed by this CLI.
- Schema evolution, data rewrites with business logic and cross-table transactions.
- Multi-worker or high-availability task execution, TLS termination and role-based authorization;
  the optional HTTP server runs one task at a time and expects a trusted network or a reverse
  proxy for transport security.
