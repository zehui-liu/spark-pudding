# Kubernetes deployment

This directory contains the assets for running spark-pudding on Kubernetes:

| File | Purpose |
| --- | --- |
| `../bin/pudding k8s build-image` | Build a Spark + spark-pudding image, optionally load it into kind |
| `../bin/pudding k8s kind-setup` | Create a local kind cluster, namespace, service account and driver RBAC |
| `../bin/pudding k8s submit` | Submit a job through the Kubernetes execution target |
| `Dockerfile` | Image definition used by `pudding k8s build-image` |
| `entrypoint.sh` | Driver and executor container entrypoint |
| `job-example.yaml` | Smoke-test job using the demo CSV baked into the image |
| `../conf/env.k8s.properties.example` | Environment file template for the submit host |

The image contains a Spark distribution at `/opt/spark` and the spark-pudding
`libs` directory at `/opt/spark-pudding/libs` (plus the demo CSV used by the smoke test).
The default resource base is therefore `local:///opt/spark-pudding`.

## Prerequisites

- The unpacked spark-pudding distribution (`mvn -B verify` output)
- Spark 3.4.0, matching the project baseline; `pudding k8s build-image` compares the Spark `RELEASE`
  version with the client manifest when both are available and rejects a mismatch, otherwise it
  warns and continues
- Docker, kubectl, kind for the local test; a registry and cluster for production
- Java 11 or 17 on the submission host

Build the distribution first:

```shell
mvn -B verify
export PUDDING_HOME="$PWD/build/target/spark-pudding-1.0.0"
export SPARK_HOME=/path/to/spark-3.4.0-bin-hadoop3
```

All scripts below live in `$PUDDING_HOME/bin`.

## Local kind smoke test

1. Create the cluster and driver resources:

```shell
"$PUDDING_HOME/bin/pudding" k8s kind-setup \
  --cluster pudding \
  --namespace pudding-k8s-test \
  --service-account spark
```

The script creates a namespace-scoped Role allowing the driver to manage
pods, services, configmaps, logs and persistent volume claims. It prints the
kind API server URL.

2. Build the image and load it into kind. `kind` must be on `PATH`.

```shell
"$PUDDING_HOME/bin/pudding" k8s build-image \
  --spark-home "$SPARK_HOME" \
  --image spark-pudding:1.0.0 \
  --kind-cluster pudding
```

The build context is temporary. Only Spark, `libs` and the demo CSV are copied;
the host Spark `conf` directory and other configuration directories are not
added to the image. The demo CSV is included when it exists in the
distribution, which is what `job-example.yaml` reads.

3. Create an environment file. Keep it at mode `0600` if you add credentials.

```shell
cp "$PUDDING_HOME/conf/env.k8s.properties.example" \
  "$PUDDING_HOME/conf/env.k8s.properties"
chmod 600 "$PUDDING_HOME/conf/env.k8s.properties"
```

Set `JAVA_HOME`, `KUBECONFIG` and, if needed, a clean `SPARK_CONF_DIR` in that
file. See the security notes below before pointing it at an existing
configuration directory.

4. Submit the example job:

```shell
API_SERVER="$(kubectl --context kind-pudding config view --minify \
  -o jsonpath='{.clusters[0].cluster.server}')"

"$PUDDING_HOME/bin/pudding" k8s submit \
  -c "$PUDDING_HOME/conf/env.k8s.properties" \
  -j "$PUDDING_HOME/k8s/job-example.yaml" \
  --master "k8s://$API_SERVER" \
  --image spark-pudding:1.0.0 \
  --namespace pudding-k8s-test \
  --service-account spark \
  --resource-base local:///opt/spark-pudding \
  --driver-master k8s://https://kubernetes.default.svc
```

`--master` is used by the submit host to reach the API. The kind port mapping is
usually `127.0.0.1:<random port>`, which is not reachable from a pod, so
`--driver-master` tells the driver to use the in-cluster service. The image
entrypoint defaults to `k8s://https://kubernetes.default.svc` as well.

The wrapper packages the job configuration into a uniquely named Secret and
passes only its name to the client. The driver reads the configuration from the
`PUDDING_JOB_CONFIG` environment variable injected through a Secret key
reference, so the job text appears neither in the spark-submit command line nor
in the driver Pod specification. `kubectl` must be available, and its current
context must point at the target cluster. The resolved namespace is used when
`--namespace` is omitted. The auto-created Secret is deleted when the wrapper
exits. To use a Secret managed outside the wrapper instead:

```shell
kubectl --context kind-pudding -n pudding-k8s-test create secret generic \
  pudding-job-config \
  --from-file=job.conf="$PUDDING_HOME/k8s/job-example.yaml"

"$PUDDING_HOME/bin/pudding" k8s submit \
  -c "$PUDDING_HOME/conf/env.k8s.properties" \
  -j "$PUDDING_HOME/k8s/job-example.yaml" \
  --master "k8s://$API_SERVER" \
  --image spark-pudding:1.0.0 \
  --namespace pudding-k8s-test \
  --service-account spark \
  --job-secret pudding-job-config
```

The file passed with `-j` and the Secret content must be identical: the client parses `-j` for
jar selection and planning, while the driver reads the Secret.

The command blocks until the driver pod reaches a final phase. It exits `0` for
`FINISHED`, and non-zero for `FAILED`, `KILLED`, `LOST` or submission errors.
The SDK adds a unique label to the driver pod and tracks its phase through the
Kubernetes API, so the result does not depend on spark-submit's exit code or
output. spark-submit output is used only as a fallback when the API is
unavailable. Cancellation through the SDK deletes the driver pod; the
Kubernetes garbage collector then removes its executors.

The credentials on the submission host, normally the current `kubectl` context
or `KUBECONFIG`, need `get` and `list` on `pods` in the job namespace, `get`
on `pods/log` to read driver logs, and `delete` on `pods` to cancel a job. When using SDK-managed job Secrets
(`managedJobSecret()` through `SubmissionManager`), or when the wrapper creates
its temporary Secret, the same identity also needs `get`, `create` and `delete`
on `secrets` in that namespace. This is separate from the driver Role created by
`pudding k8s kind-setup`, which lets the driver create executors. For a minimal
production identity:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-pudding-submitter
  namespace: data-platform
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "delete"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "create", "delete"]
```

SDK-managed Secrets are named `pudding-job-<submissionId>`. The SDK records
only the name, key and ownership flag, and deletes a Secret only when it created
it. Cleanup is retried during reconciliation and on the next startup, so final
submissions do not leave owned Secrets behind.

If the API is unreachable, submission still proceeds and the SDK falls back to
the final phase in spark-submit output. Use the API path for reliable status and
cancellation.

5. Inspect the run:

```shell
kubectl --context kind-pudding -n pudding-k8s-test get pods
kubectl --context kind-pudding -n pudding-k8s-test logs <driver-pod> | tail -100
```

A successful smoke test prints the demo CSV schema and rows, followed by the
client message `finished successfully via kubernetes target`.

Clean up the local test:

```shell
kubectl --context kind-pudding delete namespace pudding-k8s-test
kind delete cluster --name pudding
```

## Production notes

- Push the image to a registry and pass its full name with `--image`. Use
  `--image-pull-secrets` when the namespace requires pull credentials.
- Use a real namespace and service account. The Role created by
  `pudding k8s kind-setup` is a minimal starting point for development; review it
  against your cluster policy before production use.
- The submission host must be able to reach `--master`. The driver pod normally
  uses `--driver-master k8s://https://kubernetes.default.svc` and the service
  account token. Override the entrypoint's `PUDDING_DRIVER_MASTER` only when
  the cluster uses a different in-cluster API endpoint.
- To distribute jars outside the image, upload the versioned `libs` directory
  and use a cluster-visible `--resource-base` such as `hdfs:///apps/spark-pudding`.
  The image must still provide the Spark runtime and a compatible entrypoint.
- Keep the Spark version in the image, the submission host's Spark installation
  and the client dependencies aligned. The image build prints the packaged
  Spark version.
- A remote resource base can serve multiple Pudding versions, while a
  self-contained image pins Spark and Pudding together. Choose the model that
  fits the release process.

## Security notes

- The job configuration is delivered through a Secret and is not part of the
  spark-submit command or Pod specification. Do not enable `--verbose` in
  shared logs when `--conf`/`--confFile` values contain credentials; the
  generated command still shows Spark configuration, though not the job text.
- Spark copies driver configuration into a Kubernetes ConfigMap in the target
  namespace. Do not use a `SPARK_CONF_DIR` or `HADOOP_CONF_DIR` that contains
  access keys, passwords or other credentials. Deliver credentials through
  Kubernetes Secrets and explicit Spark configuration instead.
- Protect job YAML, environment files and kubeconfigs with restrictive
  permissions. The scripts print Secret names, never Secret data, but the
  source files themselves may contain secrets. Auto-created Secrets are
  deleted on exit; pre-existing Secrets are left untouched. The same rule
  applies to SDK-managed Secrets: only Secrets created for the submission are
  deleted after it reaches a final state.
- The wrapper refuses to auto-create a Secret larger than 700 KiB. Kubernetes
  restricts a Secret object to 1 MiB, and base64 encoding expands the data.
- Temporary image build contexts are created with mode `0700` and removed on
  exit. Use `--keep-context` only for local debugging.
