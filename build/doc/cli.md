# pudding CLI guide

`bin/pudding` is the supported command line for a spark-pudding distribution. It is a single
entry point over several internal scripts: arguments are passed through unchanged, and the
scripts under `bin/lib/` are internal implementation details.

```shell
"$PUDDING_HOME/bin/pudding" help
"$PUDDING_HOME/bin/pudding" <command> --help
```

This guide covers every command with runnable examples. For the submission SDK, target options
and lifecycle API, see [../../client/README.md](../../client/README.md). For Kubernetes cluster
setup, see [../k8s/README.md](../k8s/README.md).

## Distribution layout

The archive unpacks into one directory:

```text
spark-pudding-1.0.0-SNAPSHOT/
  bin/
    pudding                  unified entry point
    pudding-client.jar       submission client used by `pudding submit`
    lib/                     internal command implementations
  conf/
    env.properties           environment exported to the submission process
  libs/                      engine, connector and provider jars
  examples/                  runnable job configurations
  doc/                       this guide and the other reference documents
  log/                       default output directory for background jobs
  k8s/                       Kubernetes example job and image files
```

Set the environment before running any command:

```shell
export PUDDING_HOME=/opt/spark-pudding-1.0.0-SNAPSHOT
export JAVA_HOME=/path/to/jdk-11          # JDK 17 is also supported
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_HOME=/path/to/spark-3.4.0-bin-hadoop3
```

`SPARK_HOME` is required by commands that need Spark or Hadoop classes (`checkpoint`, `state`,
`maintenance`, `maintenance-server`, `k8s build-image`, and the `submit` targets that launch
`spark-submit`). If `SPARK_HOME` is not set, `checkpoint` and `state` fall back to
`hadoop classpath` when `hadoop` is on `PATH`.

## Command overview

| Command | Purpose |
| --- | --- |
| `pudding submit` | Submit a job through the bundled client |
| `pudding checkpoint` | Inspect or remove streaming checkpoints |
| `pudding state` | Inspect or repair incremental sync state |
| `pudding capabilities` | Verify or compare a capability manifest |
| `pudding maintenance` | Run one lakehouse table-maintenance action |
| `pudding maintenance-server` | Start the HTTP maintenance control plane |
| `pudding k8s submit` | Submit a job to Kubernetes |
| `pudding k8s build-image` | Build a Spark + spark-pudding container image |
| `pudding k8s kind-setup` | Create a local kind cluster and driver RBAC |
| `pudding verify` | Verify an assembled distribution from the source tree |

## Submit a job

```shell
"$PUDDING_HOME/bin/pudding" submit <env.properties> <job-file> [log-file]
```

- `env.properties` is required and may be empty. Each entry is exported to the submission
  process, for example `HADOOP_CONF_DIR`, `YARN_CONF_DIR`, `KUBECONFIG` or `JAVA_HOME`. Keep the
  file at mode `0600` when it contains credentials.
- `job-file` is a JSON, YAML or HOCON job configuration. It declares `job.mode` and exactly one
  sink. The file is parsed by the client and delivered to the driver without appearing on the
  `spark-submit` command line.
- `log-file` is optional. With it, the client starts the job with `nohup` and returns
  immediately; the job PID and log path are printed. Without it, the client stays attached and
  returns the application's exit status.

The execution target is inferred from `job.master`:

| `job.master` | Target | Deploy mode |
| --- | --- | --- |
| `local`, `local[*]`, `local[N]` | `local` | `client` only |
| `yarn`, `yarn-client`, `yarn-cluster` | `yarn` | `cluster` by default; `client` also accepted |
| `k8s://...` | `kubernetes` | `cluster` only |

The target can also be forced with the client option `--target`, and `job.master` can be
overridden with `--master`. The unified `submit` command does not expose those options; use the
client jar directly for them (see [Client overrides](#client-overrides)).

`pudding submit` exits with `0` only when the application reaches `FINISHED`. `FAILED`,
`KILLED`, `LOST`, a submission error or an interrupted client all return a non-zero exit code.
A background submission always returns `0` after start; its real outcome is only in the log
file.

YARN cluster submissions are tracked through the ResourceManager REST API after spark-submit
exits. The client discovers `yarn.resourcemanager.webapp.address` or the HTTPS variant from
`yarn-site.xml` in `YARN_CONF_DIR`/`HADOOP_CONF_DIR`; `PUDDING_YARN_RM_URL` overrides it.
Without a web application address the submission fails before launch. Kerberos-protected
ResourceManagers require an authenticated gateway. YARN client mode uses the spark-submit exit
status.

### Local example

`examples/read_csv_example.yaml` is a complete local batch job. Set `job.sparkHome`,
`job.sparkPuddingHome`, `job.master: local[*]`, `job.deployMode: client` and the CSV path, then
run it attached:

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml"
```

The console sink prints the rows and the client returns when the query finishes. To run it in the
background and follow the log:

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml" \
  "$PUDDING_HOME/log/my-job.log"

tail -f "$PUDDING_HOME/log/my-job.log"
```

For a scheduler, use the attached form: the process exits only when the job finishes, so the
scheduler sees the real result. Use the background form for an operator shell or a long-running
streaming job that is supervised elsewhere.

### YARN example

Set `job.master: yarn` and `job.deployMode: cluster`, and make the jars visible to the cluster.
The distribution must exist on the submission host for the client to discover the library
layout; `job.sparkPuddingHome` points at that local copy:

```yaml
job:
  mode: sync
  strategy: incremental
  appName: orders-incremental-sync
  sparkHome: /opt/spark-3.4.0-bin-hadoop3
  sparkPuddingHome: /opt/spark-pudding-1.0.0-SNAPSHOT
  master: yarn
  deployMode: cluster
  config:
    spark.yarn.queue: root.data
    spark.executor.instances: '4'
source:
  - type: jdbc-source
    id: orders_source
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_ro
      password: ${secret:env/ORDERS_DB_PASSWORD}
      cursorField: updated_at
      incremental:
        semantics: upsert
        keyFields: [id]
        initial: full
        statePath: hdfs:///pudding/state/orders
sink:
  - type: jdbc-sink
    id: orders_target
    dependency: orders_source
    config:
      url: jdbc:postgresql://warehouse.internal:5432/warehouse
      table: public.orders
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
```

Set the cluster configuration directories in `conf/env.properties`:

```properties
HADOOP_CONF_DIR=/etc/hadoop/conf
YARN_CONF_DIR=/etc/hadoop/conf
```

Then submit:

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  /etc/pudding/orders-sync.yaml
```

The client stages the job configuration as a private temporary file and, in cluster mode, ships
it to the driver through `spark.yarn.dist.files` as `pudding-job.conf`. The file is removed when
the application ends. It is never placed in the `spark-submit` arguments or environment.

If the library jars are not reachable from cluster nodes, copy the distribution to HDFS and pass
`--resourceBase` to the client jar:

```shell
hdfs dfs -put "$PUDDING_HOME" /apps/spark-pudding/1.0.0-SNAPSHOT

"$PUDDING_HOME/bin/pudding-client.jar" \
  -c "$PUDDING_HOME/conf/env.properties" \
  -j /etc/pudding/orders-sync.yaml \
  --target yarn \
  --resourceBase hdfs:///apps/spark-pudding/1.0.0-SNAPSHOT
```

### Kubernetes example

`pudding k8s submit` wraps the client, creates a Secret for the job configuration and passes only
the Secret reference to the driver:

```shell
"$PUDDING_HOME/bin/pudding" k8s submit \
  -c "$PUDDING_HOME/conf/env.k8s.properties" \
  -j "$PUDDING_HOME/k8s/job-example.yaml" \
  --master "k8s://https://kubernetes.example:443" \
  --image registry.example/spark-pudding:1.0.0-SNAPSHOT \
  --namespace data-platform \
  --service-account spark \
  --resource-base local:///opt/spark-pudding
```

`--master` is how the submit host reaches the API server. When the driver must use a different
URL (for example a kind cluster port mapping), add `--driver-master`. See
[../k8s/README.md](../k8s/README.md) for image building, kind setup and the Secret workflow.

Direct client submissions require `--k8s-job-secret`; the wrapper is the recommended path
because it creates and deletes that Secret automatically.

### Client overrides

The unified `submit` command intentionally exposes only the common path. Use the bundled client
jar when you need to override the target or a deployment option:

```shell
"$PUDDING_HOME/bin/pudding-client.jar" \
  -c "$PUDDING_HOME/conf/env.properties" \
  -j "$PUDDING_HOME/examples/read_csv_example.yaml" \
  --target local \
  --sparkHome /opt/spark-3.4.0-bin-hadoop3 \
  --sparkPuddingHome "$PUDDING_HOME" \
  --conf spark.driver.memory=1g \
  --verbose
```

Frequently used client options:

| Option | Description |
| --- | --- |
| `-c, --confFile` | Environment properties file (required) |
| `-j, --jobFile` | JSON, YAML or HOCON job file (required) |
| `-t, --target` | `local`, `yarn` or `kubernetes`; inferred from the master by default |
| `--master` | Override `job.master` |
| `--deployMode` | Override `job.deployMode` where the target supports it |
| `--resourceBase` | Base location containing `libs`, such as an HDFS URI |
| `--sparkHome` | Override `job.sparkHome` |
| `--sparkPuddingHome` | Override `job.sparkPuddingHome` |
| `--conf key=value` | Add Spark configuration; repeatable |
| `--verbose` | Print the generated `spark-submit` command |

`--verbose` may print `--conf` values, so avoid it in shared logs when credentials are involved.
The job configuration itself is delivered by file or Secret and is not printed.

## Checkpoints

`pudding checkpoint` inspects or deletes Structured Streaming checkpoints without submitting a
Spark job. Set `HADOOP_CONF_DIR` for HDFS, or pass object-store settings with `--conf-file`.

```shell
# Human-readable summary: existence, shape, size, age, commit/offset counts
"$PUDDING_HOME/bin/pudding" checkpoint show hdfs:///pudding/checkpoints/orders

# Machine-readable status for monitoring
"$PUDDING_HOME/bin/pudding" checkpoint status hdfs:///pudding/checkpoints/orders --json

# Preview, then delete one checkpoint
"$PUDDING_HOME/bin/pudding" checkpoint delete hdfs:///pudding/checkpoints/orders --dry-run
"$PUDDING_HOME/bin/pudding" checkpoint delete hdfs:///pudding/checkpoints/orders

# Delete checkpoints older than seven days under a parent directory
"$PUDDING_HOME/bin/pudding" checkpoint cleanup hdfs:///pudding/checkpoints \
  --older-than 7d --dry-run
"$PUDDING_HOME/bin/pudding" checkpoint cleanup hdfs:///pudding/checkpoints --older-than 7d

# Keep only the two newest checkpoints
"$PUDDING_HOME/bin/pudding" checkpoint cleanup hdfs:///pudding/checkpoints --keep-last 2
```

Object-store example:

```shell
"$PUDDING_HOME/bin/pudding" checkpoint show s3a://bucket/pudding/checkpoints/orders \
  --conf-file /etc/pudding/checkpoint-s3a.properties
```

`--conf-file` accepts only `fs.*` keys. Use an IAM role, instance profile, IRSA or a `0600`
credential file; do not put access keys on the command line. `--hadoop-conf <dir>` adds the XML
files from a Hadoop configuration directory.

Stop every writer before deleting or cleaning checkpoints. The tool cannot detect an active
writer. `cleanup` only scans direct children of the given root, and the root must be the parent
of checkpoint directories, not a checkpoint. When both `--older-than` and `--keep-last` are
present, a checkpoint must satisfy both conditions. See
[streaming-checkpoints.md](streaming-checkpoints.md) for the retention semantics and the
completion evidence used by `delete-on-success`.

## Incremental state

`pudding state` inspects or repairs the incremental cursor journal. A state target is either a
filesystem path or `@/path/to/state.properties`; the properties file selects the backend
(`filesystem`, `jdbc`, `redis` or `etcd`) and its options.

```shell
# Show all sources, or one source
"$PUDDING_HOME/bin/pudding" state show hdfs:///pudding/state
"$PUDDING_HOME/bin/pudding" state show hdfs:///pudding/state orders

# JSON for monitoring
"$PUDDING_HOME/bin/pudding" state status hdfs:///pudding/state orders --json

# List source ids
"$PUDDING_HOME/bin/pudding" state list hdfs:///pudding/state

# Resolve an interrupted transaction after checking the sink
"$PUDDING_HOME/bin/pudding" state resolve hdfs:///pudding/state orders --abort
"$PUDDING_HOME/bin/pudding" state resolve hdfs:///pudding/state orders --commit

# Set a cursor explicitly, or clear state for a re-bootstrap
"$PUDDING_HOME/bin/pudding" state reset hdfs:///pudding/state orders --cursor 20260101000000
"$PUDDING_HOME/bin/pudding" state reset hdfs:///pudding/state orders --clear

# Remove a lock left by a failed process after verifying no writer is alive
"$PUDDING_HOME/bin/pudding" state unlock hdfs:///pudding/state orders

# Create or upgrade a JDBC state table
"$PUDDING_HOME/bin/pudding" state migrate @/etc/pudding/state-jdbc.properties
```

A properties target looks like this:

```properties
backend=jdbc
url=jdbc:postgresql://state-db:5432/pudding
user=pudding
password=${secret:env/STATE_DB_PASSWORD}
table=pudding_incremental_state
lockTtlMs=600000
```

`${secret:...}` references are resolved by the tool. Set `PUDDING_STATE_CLASSPATH` to a JDBC
driver jar when the driver is not on the Spark classpath. Redis and etcd provider jars are
loaded from `libs` automatically. Keep a properties file containing a plaintext credential at
mode `0600`. See [incremental-sync.md](incremental-sync.md) for the commit/abort protocol,
fencing and recovery rules.

## Capability manifest

`pudding capabilities` reads `capabilities.yaml` and jar metadata without loading Spark or
connector classes.

```shell
# Verify that the manifest matches the jars in a distribution
"$PUDDING_HOME/bin/pudding" capabilities verify "$PUDDING_HOME"

# Print the merged manifest
"$PUDDING_HOME/bin/pudding" capabilities show "$PUDDING_HOME"

# Compare two releases before an upgrade
"$PUDDING_HOME/bin/pudding" capabilities diff \
  /opt/spark-pudding-previous /opt/spark-pudding-1.0.0-SNAPSHOT
```

`verify` returns non-zero when a declared component or jar is missing or when the manifest is
malformed. `diff` reports added, removed and changed components. See
[capability-manifest.md](capability-manifest.md) for the schema and fragment rules.

## Table maintenance

`pudding maintenance` runs one native Spark SQL maintenance action for Iceberg, Paimon or Hudi.
It uses an external scheduler for cadence and never touches checkpoints or incremental state.

```shell
# Read-only health check
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg status \
  --catalog lake --table db.orders \
  --conf-file /etc/pudding/iceberg-catalog.properties --json

# Compact small files
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg rewrite-data-files \
  --catalog lake --table db.orders \
  --target-file-size 128MB --min-input-files 5 \
  --conf-file /etc/pudding/iceberg-catalog.properties

# Destructive actions require --confirm; preview with --dry-run
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg expire-snapshots \
  --catalog lake --table db.orders --older-than 7d --retain-last 5 --confirm \
  --conf-file /etc/pudding/iceberg-catalog.properties

# Hudi path mode
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance hudi compact \
  --path hdfs:///warehouse/orders --option op=scheduleAndExecute

# Hudi table registered in a catalog
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance hudi clean \
  --table default.hudi_orders --retain-last 10 --confirm
```

Supported actions:

| Format | Actions |
| --- | --- |
| Iceberg | `status`, `rewrite-data-files`, `rewrite-manifests`, `expire-snapshots`, `remove-orphan-files` |
| Paimon | `status`, `compact`, `compact-manifests`, `expire-snapshots`, `remove-orphan-files` |
| Hudi | `status`, `clean`, `compact`, `cluster`, `archive` |

`SPARK_MASTER` is passed to `spark-submit`; omit it when Spark already has a default master. The
`--conf-file` contains only `spark.*` keys and should be mode `0600`. Run
`pudding maintenance <format> <action> --help` for the action-specific options. See
[table-maintenance.md](table-maintenance.md) for safety rules and per-format behavior.

## Maintenance HTTP server

`pudding maintenance-server` exposes the same maintenance service over HTTP for a platform or
scheduler.

```shell
SPARK_MASTER=yarn "$PUDDING_HOME/bin/pudding" maintenance-server \
  --host 127.0.0.1 --port 8080 \
  --state-file /var/lib/pudding/maintenance-tasks.json \
  --audit-file /var/lib/pudding/maintenance-audit.jsonl \
  --auth-token-file /etc/pudding/maintenance-token \
  --conf-file /etc/pudding/iceberg-catalog.properties
```

The bearer token is read from `--auth-token-file` or `PUDDING_MAINTENANCE_TOKEN`; it is never
accepted as a command-line value. `--allow-anonymous` is only for local development. The state
file contains requests and results but no Spark configuration or credentials. See
[table-maintenance.md](table-maintenance.md#http-control-plane) for the endpoint list and
request schema.

## Kubernetes

The three Kubernetes commands are also documented in [../k8s/README.md](../k8s/README.md).

```shell
# 1. Create a kind cluster, namespace, service account and driver RBAC
"$PUDDING_HOME/bin/pudding" k8s kind-setup \
  --cluster pudding --namespace pudding-k8s-test --service-account spark

# 2. Build an image and load it into kind
"$PUDDING_HOME/bin/pudding" k8s build-image \
  --spark-home "$SPARK_HOME" \
  --image spark-pudding:1.0.0-SNAPSHOT \
  --kind-cluster pudding

# 3. Submit a job
"$PUDDING_HOME/bin/pudding" k8s submit \
  -c "$PUDDING_HOME/conf/env.k8s.properties" \
  -j "$PUDDING_HOME/k8s/job-example.yaml" \
  --master "k8s://https://127.0.0.1:6443" \
  --image spark-pudding:1.0.0-SNAPSHOT \
  --namespace pudding-k8s-test \
  --service-account spark \
  --resource-base local:///opt/spark-pudding
```

`k8s build-image` copies only Spark, `libs` and the demo CSV into a temporary build context. The
host Spark `conf` directory and other configuration directories are not included. Keep
`env.k8s.properties` at mode `0600` when it contains credentials.

## Verify a distribution

In the source tree, `pudding verify` checks an assembled distribution:

```shell
"$PUDDING_HOME/bin/pudding" verify build/target/spark-pudding-1.0.0-SNAPSHOT
```

It validates the expected layout, jars, example files and the capability manifest. This command
is available from the source checkout only; it is not part of the packaged distribution.

## Security notes

- Never pass passwords or access keys as command-line arguments. Use `--conf-file`,
  `env.properties`, environment variables or `${secret:...}` references.
- Job files and state properties containing credentials must be mode `0600`. The client stages
  the job file as an owner-only temporary file and removes it when the application ends.
- `pudding submit` and `spark-submit` do not log the job text; only the file or Secret reference
  is passed. `--verbose` prints the generated command, including `--conf` values.
- Checkpoint and incremental state contain cursor values and offsets, never credentials or
  source configuration.
- The maintenance server refuses to start with neither a token nor `--allow-anonymous`; expose
  it only on a trusted network.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `environment file is not readable` | `pudding submit` requires an existing `env.properties`; use an empty file for a local job |
| `cannot infer execution target` | Set `job.master` to a supported value or pass `--target` to the client jar |
| `yarn target requires master 'yarn'` | `--master` cannot change a YARN target to another master; remove it or use the right target |
| `spark-pudding libs directory does not exist` | Set `job.sparkPuddingHome` or `--sparkPuddingHome` to the unpacked distribution |
| Job starts in background but fails immediately | Read the supplied log file; a background submission cannot return the job result |
| `pudding checkpoint` cannot find Hadoop classes | Set `SPARK_HOME` or put `hadoop` on `PATH` |
| JDBC state command fails to load the driver | Add the driver jar to `PUDDING_STATE_CLASSPATH` |
| Kubernetes submission cannot read the job | Use `pudding k8s submit`, or create the Secret required by `--k8s-job-secret` |
