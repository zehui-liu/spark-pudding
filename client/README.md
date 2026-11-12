# spark-pudding submission SDK

The client module contains a reusable submission SDK and the command line entry point.

- `PuddingSubmitter` prepares and submits a job through an `ExecutionTarget`.
- `ExecutionTarget` implementations select deployment details and produce an immutable
  `SubmissionPlan`.
- `RunningApplication` exposes the application id, state, recent logs, error and asynchronous
  waiting.
- `SubmissionManager` adds durable `SubmissionRecord`s, automatic state persistence, Kubernetes
  reattachment after a client restart, reconciliation, log access and cancellation.
- `LocalTarget`, `YarnTarget` and `KubernetesTarget` are the built-in targets.
- `ExecutionTarget.named(alias, target)` exposes a built-in target under a stable alias, so one
  manager can hold several YARN or Kubernetes clusters. Plans and durable records keep the
  canonical `local`/`yarn`/`kubernetes` type.

The REST service and frontend are expected to call this SDK rather than launching Spark
processes themselves.

## CLI

The client jar remains directly executable for SDK users and for target options that the unified
launcher does not expose. The supported distribution entry point is `bin/pudding submit`, which
wraps this CLI and adds the shared environment handling. The complete CLI guide, including every
`pudding` subcommand, is in [`../build/doc/cli.md`](../build/doc/cli.md) (English) and
[`../build/doc/cli.zh-CN.md`](../build/doc/cli.zh-CN.md) (Chinese):

```shell
"$PUDDING_HOME/bin/pudding" submit conf/env.properties examples/read_csv_example.yaml
```

The equivalent direct client invocation is:

```shell
"$PUDDING_HOME/bin/pudding-client.jar" -c conf/env.properties -j examples/read_csv_example.yaml
```

The target is inferred from the Spark master in the job configuration. It can also be selected
explicitly. The CLI exposes only the built-in target types; named multi-cluster targets are
available through the SDK's `SubmissionManager`:

```shell
# Local development. The target falls back to local[*] if the job master is not local.
"$PUDDING_HOME/bin/pudding-client.jar" -c conf/env.properties -j examples/read_csv_example.yaml --target local

# YARN. Cluster deploy mode is the default.
"$PUDDING_HOME/bin/pudding-client.jar" -c conf/env.properties -j examples/read_csv_example.yaml \
  --target yarn \
  --resourceBase hdfs:///apps/spark-pudding/1.0.0-SNAPSHOT

# Kubernetes. Cluster deploy mode is required.
"$PUDDING_HOME/bin/pudding-client.jar" -c conf/env.properties -j examples/read_csv_example.yaml \
  --target kubernetes \
  --master k8s://https://kubernetes.example:443 \
  --resourceBase local:///opt/spark-pudding \
  --k8s-image registry.example/spark-pudding:1.0.0-SNAPSHOT \
  --k8s-namespace data-platform \
  --k8s-service-account pudding-spark \
  --k8s-job-secret pudding-job-config \
  --kubeconfig /etc/pudding/kubeconfig
```

The CLI requires `--k8s-job-secret`, an existing Secret in the submission namespace. The Secret
key defaults to `job.conf` and can be changed with `--k8s-job-secret-key`. Create it beforehand,
for example:

```shell
kubectl -n data-platform create secret generic pudding-job-config \
  --from-file=job.conf=examples/read_csv_example.yaml
```

`bin/pudding k8s submit` automates Secret creation and deletion.

Useful options:

| Option | Description |
| --- | --- |
| `-t, --target` | `local`, `yarn` or `kubernetes`; inferred from the master by default |
| `--master` | Override the job master |
| `--deployMode` | Override the deploy mode where the target supports it |
| `--resourceBase` | Base location containing `libs`; a URI for remote storage |
| `--sparkHome` | Override `job.sparkHome` |
| `--sparkPuddingHome` | Override `job.sparkPuddingHome` |
| `--conf key=value` | Add Spark configuration; repeatable |
| `--k8s-image` | Kubernetes Spark container image |
| `--k8s-namespace` | Kubernetes namespace |
| `--k8s-service-account` | Driver service account |
| `--k8s-image-pull-secrets` | Comma separated image pull secret names |
| `--k8s-job-secret` | Existing Secret containing the job configuration; required for Kubernetes |
| `--k8s-job-secret-key` | Key inside the job Secret; defaults to `job.conf` |
| `--kubeconfig` | Exported as `KUBECONFIG` for the submission process |
| `--verbose` | Print the generated spark-submit command |

The CLI exits with `0` only when the application reaches `FINISHED`. `FAILED`, `KILLED`, `LOST`
and submission errors return a non-zero exit code.

YARN cluster submissions exit spark-submit after handing the application to the ResourceManager.
The client reads the application id from the spark-submit output and tracks the application
through the ResourceManager REST API. It resolves
`yarn.resourcemanager.webapp.address` (or the HTTPS variant) from `yarn-site.xml` under
`YARN_CONF_DIR`/`HADOOP_CONF_DIR`; `PUDDING_YARN_RM_URL` overrides the discovered URL. A missing
web application address fails before launch. The client does not authenticate to a
Kerberos-protected ResourceManager; use an authenticated gateway for such clusters. YARN client
mode keeps using the spark-submit exit status.

Kubernetes cluster submissions do not report launcher protocol states, and spark-submit can exit
with code `0` even when the driver pod fails. The SDK labels each driver pod with a unique
submission id and tracks it through the Kubernetes API: the pod phase determines `FINISHED` or
`FAILED`, and the application id comes from the pod's `spark-app-selector` label. spark-submit
output is logged and used for the final state only when the API is unavailable. A missing final
phase is reported as `LOST` rather than as success. Cancelling a Kubernetes application deletes
the driver pod; Kubernetes then removes the executors it owns.

The credentials used for submission need `get` and `list` on `pods` in the target namespace,
`get` on `pods/log` to read driver logs, and `delete` on `pods` for cancellation. The driver
service account role is separate; see `../build/k8s/README.md`.

## SDK

```java
JobConfiguration job = JobConfiguration.fromFile(Paths.get("job.yaml"));

ExecutionTarget target = YarnTarget.builder()
        .resourceBase("hdfs:///apps/spark-pudding/1.0.0-SNAPSHOT")
        .queue("root.data")
        .deployMode("cluster")
        .sparkConf("spark.executor.instances", "4")
        .build();

SubmissionRequest request = SubmissionRequest.builder(job)
        .environment(env)   // HADOOP_CONF_DIR, YARN_CONF_DIR, ...
        .build();

PuddingSubmitter submitter = new PuddingSubmitter();
RunningApplication running = submitter.submit(target, request);

// Persist the external application id immediately; do not block a REST request thread here.
String applicationId = running.getApplicationId().orElse(null);

// Batch callers may wait for the final result.
SubmissionResult result = running.await();
```

`prepare` is separated from `submit`, so a caller can inspect a plan before starting a process:

```java
SubmissionPlan plan = submitter.prepare(target, request);
```

`SubmissionPlan.toString()` and `JobConfiguration.toString()` do not include configuration values
or application arguments.

### Named targets and multiple clusters

Wrap a built-in target to expose it under a stable name. The name is the key used by
`SubmissionManager`; the type and preparation/submission behavior still come from the delegate:

```java
ExecutionTarget yarnAnalytics = ExecutionTarget.named("yarn-analytics",
        YarnTarget.builder()
                .resourceBase("hdfs:///apps/spark-pudding")
                .queue("analytics")
                .build());

ExecutionTarget yarnBatch = ExecutionTarget.named("yarn-batch",
        YarnTarget.builder()
                .resourceBase("hdfs:///apps/spark-pudding")
                .queue("batch")
                .build());

ExecutionTarget k8sPrimary = ExecutionTarget.named("k8s-primary",
        KubernetesTarget.builder()
                .master("k8s://https://kubernetes.example:443")
                .namespace("data-platform")
                .image("registry.example/spark-pudding:1.0.0-SNAPSHOT")
                .resourceBase("local:///opt/spark-pudding")
                .kubeconfig("/etc/pudding/kubeconfig")
                .build());
```

Aliases must match `[a-z0-9][a-z0-9-]*` and are at most 64 characters. They are independent of
the built-in canonical names, so `yarn` itself can also be used as an alias, but registering the
same alias twice is rejected.

Lookup is exact-alias first. If no alias matches, the input is treated as a canonical type
(`local`, `yarn`, `kubernetes`, and the existing `yarn-client`/`yarn-cluster`/`k8s` aliases);
that form succeeds only when exactly one configured target has the type. With two YARN targets,
`submit("yarn", ...)` throws an ambiguity error and the caller must use `yarn-analytics` or
`yarn-batch`. An empty target name is accepted only when the manager holds exactly one target.
The `prepare`/`submit` plan validates that its canonical target type matches the selected named
target, so a YARN plan cannot be launched through a Kubernetes alias.

The durable `SubmissionRecord` always stores the canonical type (`local`/`yarn`/`kubernetes`),
not the alias, and keeps the master, deploy mode and kubeconfig path needed for reattachment.
Consequently records remain readable and recoverable if the target is later renamed or removed
from the manager.

### Updating targets at runtime

`SubmissionManager.updateTargets` atomically replaces the target set without rebuilding or
closing the manager:

```java
manager.updateTargets(Arrays.asList(yarnAnalytics, yarnBatch, k8sPrimary));
```

The collection is a complete replacement, not a merge. Duplicate aliases are rejected, and an
empty collection is allowed (later target lookups fail). The call validates the new set before
taking the lifecycle write lock, waits for in-flight submissions to finish launching, and rejects
updates after `close()`. Existing `ManagedSubmission` instances, watchers, logs, Kubernetes
Secrets and active runs are untouched; only new submissions use the new set. Removing a target
does not cancel work already submitted through it.

### Kubernetes example

```java
ExecutionTarget target = KubernetesTarget.builder()
        .master("k8s://https://kubernetes.example:443")
        .resourceBase("local:///opt/spark-pudding")
        .image("registry.example/spark-pudding:1.0.0-SNAPSHOT")
        .namespace("data-platform")
        .serviceAccount("pudding-spark")
        .imagePullSecrets(Arrays.asList("regcred"))
        .jobSecret("pudding-job-config")
        .jobSecretKey("job.conf")
        .kubeconfig("/etc/pudding/kubeconfig")
        .build();
```

For a target-level or request-level Secret reference, the Secret must exist in the submission
namespace. KubernetesTarget adds
`spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG=<secret>:<key>` and passes only
`--confType <type> --jobEnv PUDDING_JOB_CONFIG` to the driver. The job text is not placed in
the spark-submit command or the driver Pod specification.

#### SDK-managed Secrets

A long-lived service should not share one Secret between concurrent submissions. With
`SubmissionManager`, ask the SDK to create a dedicated Secret for each submission:

```java
SubmissionRequest request = SubmissionRequest.builder(job)
        .managedJobSecret()
        .jobSecretKey("job.conf")   // optional; inherits the target key
        .build();

ManagedSubmission managed = manager.submit("kubernetes", request);
```

The manager generates `pudding-job-<submissionId>`, creates it after the `PENDING` record is
durable and before the launcher starts, and records only its name, key and ownership flag. When
the submission reaches a final state, or the launch fails, the manager deletes the Secret on a
background thread. Failures are retried during `reconcile()` and by the startup sweep, so a
restart cleans up final submissions. Only Secrets created by the manager are deleted; a Secret
selected with `jobSecret(...)` on the request is never deleted.

The manager must be the component that submits Kubernetes jobs: calling
`KubernetesTarget.prepare`/`submit` directly with `managedJobSecret()` does not provision the
Secret. The submitter identity needs `get`, `create` and `delete` on `secrets` in addition to
the pod permissions above.

For `local://` resources, the distribution must be baked into the image at the same path. For a
remote resource base, upload the versioned `libs` directory before submitting.

## Recent logs

`RunningApplication.getRecentLogs(maxLines)` returns an immutable `ApplicationLog` with the most
recent lines, a truncation flag and `getText()`. The in-memory buffer is bounded; content is never
included in `toString()`:

```java
ApplicationLog log = running.getRecentLogs(200);
String text = log.getText();
boolean olderLinesWereDropped = log.isTruncated();
```

For attached applications, local and YARN launcher output and Kubernetes driver output are
captured through the Spark launcher. A Kubernetes application reads its driver Pod logs through
the API first and falls back to spark-submit output when the API is unavailable. After a client
restart, `SubmissionManager` can read Kubernetes driver logs from the persisted submission id
while the driver Pod still exists. A YARN cluster driver after the client has exited has no
in-process log source; use YARN log aggregation for that case.

Log text may contain credentials printed by user code. Before persisting or exposing it outside
the current process, pass it through `LogRedaction.redactText` (or `redactLine` per line). The
engine redacts its own configuration logs, but it can not guarantee that arbitrary application
output contains no credential.

## Durable submissions

`SubmissionManager` is the long-lived SDK entry point for a REST service. Create one manager for
the service lifetime and share it between request threads:

```java
SubmissionStore store = new FileSubmissionStore(
        Paths.get("/var/lib/pudding/submissions"));

SubmissionManager manager = SubmissionManager.builder()
        .target(localTarget)
        .target(yarnTarget)
        .target(kubernetesTarget)
        .store(store)
        .pollInterval(Duration.ofSeconds(2))
        .reattachGrace(Duration.ofSeconds(30))
        .build();

ManagedSubmission managed = manager.submit("kubernetes", request);
String submissionId = managed.getSubmissionId();

// Later request handlers do not keep the ManagedSubmission object alive.
SubmissionRecord record = manager.find(submissionId)
        .orElseThrow(() -> new IllegalStateException("unknown submission"))
        .refresh();
ApplicationLog log = manager.find(submissionId).get().getRecentLogs(200);
manager.find(submissionId).get().cancel();
List<SubmissionRecord> all = manager.list();
```

The manager:

- writes a `PENDING` `SubmissionRecord` before launching and updates it from the attached
  `RunningApplication`;
- automatically starts a daemon watcher that refreshes every managed submission; `reconcile()`
  performs an immediate pass and `ManagedSubmission.refresh()` refreshes one submission;
- reloads records when constructed, so a restarted service can list and inspect old submissions;
- reattaches non-final Kubernetes submissions by the `pudding-submission-id` driver Pod label,
  using the persisted master, namespace and kubeconfig path;
- marks a restored Kubernetes submission `LOST` when no driver Pod appears within the
  reattachment grace period, or when a previously seen driver Pod disappears before a final
  phase;
- marks restored local and YARN submissions `LOST`, because their launcher lived in the previous
  client process and can not be reattached;
- cancels an attached application through its launcher, and cancels a reattached Kubernetes
  submission by deleting the driver Pod before persisting `KILLED`;
- creates and deletes per-submission Kubernetes Secrets for `managedJobSecret()` requests, and
  retries pending cleanup during `reconcile()` and on startup; only Secrets it created are
  deleted;
- does not cancel running applications in `close()`; shutdown waits for submissions that are
  already being prepared or launched, then stops the watcher and releases Kubernetes API clients.

A record contains only submission id, target, application name, master, deploy mode, application
id, Kubernetes namespace, kubeconfig **path**, Kubernetes job Secret name/key/ownership, state,
bounded error text and timestamps. `targetName` is the canonical type, even when submission used
a named alias. It never contains job configuration, Spark configuration
values, environment variable values or kubeconfig contents. Final states are immutable. Use a
database-backed `SubmissionStore` when several service hosts share submission state;
`FileSubmissionStore` is intended for one host.

`FileSubmissionStore` restricts its directory to the owner (`0700`), writes each record through
an owner-only temporary file (`0600`), forces it to disk and renames it into place, using an
atomic move when the filesystem supports it. Symbolic links inside the store are rejected. Choose
a private directory that is not world readable.

## Target behavior

| Target | Master | Deploy mode | Resource base |
| --- | --- | --- | --- |
| `local` | local master, otherwise `local[*]` | `client` | local distribution or override |
| `yarn` | `yarn` | `cluster` by default | local or HDFS/remote URI |
| `kubernetes` | `k8s://...` | `cluster` | required cluster-visible URI |

Shared rules:

- `ExecutionTarget.named` only changes selection and identity; prepare/submit behavior, deploy
  mode, resource base rules and record fields are exactly those of the wrapped target type.
- `spark.master`, `spark.submit.deployMode` and `spark.app.name` from the job configuration are
  ignored because the target owns deployment.
- The distribution is discovered from `job.sparkPuddingHome` unless a target builder supplies
  `sparkPuddingHome`. That path must be a local directory on the submission host, even when the
  resource base is remote.
- The expected layout is `$PUDDING_HOME/libs/pudding-core-<version>.jar`,
  `connection-providers-<version>.jar`, `catalog-providers-<version>.jar`, connector jars and the
  runtime jars selected by the pipeline. Connector jars are added only when referenced; jobs
  using JDBC or MySQL source/sink connectors, or a JDBC catalog, also receive the shared
  `jdbc-common-<version>.jar`; Kafka sources with `valueFormat: avro` or `protobuf` also receive
  `kafka-schema-registry-runtime-<version>.jar`. See
  [runtime/README.md](../runtime/README.md) for the format runtime selection rules.
- `KubernetesTarget` rejects plain local resource paths, because the driver and executors run in
  containers.
- `KubernetesTarget` rejects a missing or invalid job Secret name/key. With
  `managedJobSecret()` it generates `pudding-job-<submissionId>` instead; this mode requires
  `SubmissionManager`.
- Local job configuration is never passed with `--jobText`. The SDK writes a `0600` temporary
  file and passes `--jobFile`. In YARN cluster mode it also adds the file to
  `spark.yarn.dist.files` under the alias `pudding-job.conf`, so the driver on a NodeManager can
  read it. The temporary directory is removed when the application reaches a final state or the
  JVM exits.

## Security note

`LocalTarget` and `YarnTarget` write the job configuration to a temporary file readable only by
the submitting user and pass `--jobFile`; the configuration does not appear in the process list
or verbose command logs. In YARN cluster mode the file is shipped as a distributed file under a
fixed alias, and the temporary local copy is deleted when the application finishes. Protect the
submission host's temporary directory and keep job configuration itself free of long-lived
credentials where possible.

`KubernetesTarget` delivers the configuration through a Secret key reference and passes only the
environment variable name to the driver. Use `bin/pudding k8s submit` to create and clean up the
Secret, manage a pre-existing Secret yourself, or let `SubmissionManager` own a generated Secret
per submission with `managedJobSecret()`. The engine's `--jobEnv` option can also be used
directly with `spark-submit`; `--jobFile` is available for file-based delivery.
