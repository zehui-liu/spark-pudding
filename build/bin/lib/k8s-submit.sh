#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Submit a spark-pudding job through the reusable Kubernetes target. This is a
# convenience wrapper around pudding-client.jar. The job configuration is
# delivered through a Kubernetes Secret, never through command-line arguments.
# An auto-created Secret is deleted when this script exits.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
SPARK_PUDDING_HOME_VALUE="${SPARK_PUDDING_HOME:-$PUDDING_HOME}"
CONF_FILE=""
JOB_FILE=""
MASTER="${K8S_MASTER:-}"
RESOURCE_BASE="${K8S_RESOURCE_BASE:-local:///opt/spark-pudding}"
IMAGE="${K8S_IMAGE:-}"
NAMESPACE="${K8S_NAMESPACE:-}"
SERVICE_ACCOUNT="${K8S_SERVICE_ACCOUNT:-}"
PULL_SECRETS="${K8S_IMAGE_PULL_SECRETS:-}"
KUBECONFIG_PATH="${KUBECONFIG:-}"
SPARK_HOME_ARG="${SPARK_HOME:-}"
DRIVER_MASTER="${K8S_DRIVER_MASTER:-}"
JOB_SECRET="${K8S_JOB_SECRET:-}"
JOB_SECRET_KEY="${K8S_JOB_SECRET_KEY:-job.conf}"
VERBOSE="false"
AUTO_CREATED_SECRET="false"
EXTRA_CONF=()
EXTRA_ARGS=()

cleanup() {
  local status=$?
  trap - EXIT
  if [[ "$AUTO_CREATED_SECRET" == "true" ]]; then
    if ! kubectl --namespace "$NAMESPACE" delete secret "$JOB_SECRET" \
        --ignore-not-found >/dev/null 2>&1; then
      echo "pudding k8s submit: warning: failed to delete job configuration Secret: $JOB_SECRET" >&2
    fi
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

usage() {
  cat <<'EOF'
Submit a spark-pudding job to Kubernetes.

Usage:
  pudding k8s submit -c <env.properties> -j <job.yaml> --image <name:tag> \
    --master <k8s://...> [options] [-- <extra client options>]

Required:
  -c, --confFile <file>            Environment properties file
  -j, --jobFile <file>             Job configuration file
  --image <name:tag>               Spark container image
  --master <k8s://...>             Kubernetes API server URL

Options:
  --resource-base <base>           Cluster-visible resource base
                                   (default: local:///opt/spark-pudding)
  --namespace <name>               Kubernetes namespace
  --service-account <name>         Driver service account
  --image-pull-secrets <names>     Comma separated image pull secret names
  --job-secret <name>              Use an existing Secret containing the job
                                   configuration instead of creating one
  --job-secret-key <key>           Key inside the job Secret (default: job.conf)
  --kubeconfig <file>              kubeconfig exported as KUBECONFIG
  --spark-home <dir>               Spark installation used by the submit host
  --spark-pudding-home <dir>       Local libs directory owner
                                   (default: PUDDING_HOME)
  --driver-master <master>         Master used inside the driver pod
                                   (default from the image entrypoint:
                                   k8s://https://kubernetes.default.svc)
  --conf <key=value>               Additional Spark configuration; repeatable
  --verbose                        Print the generated spark-submit command
  -h, --help                       Show this help

Environment:
  PUDDING_HOME, SPARK_HOME, SPARK_PUDDING_HOME, KUBECONFIG, K8S_MASTER, K8S_IMAGE,
  K8S_NAMESPACE, K8S_SERVICE_ACCOUNT, K8S_RESOURCE_BASE,
  K8S_IMAGE_PULL_SECRETS, K8S_JOB_SECRET, K8S_JOB_SECRET_KEY,
  K8S_DRIVER_MASTER

When --job-secret is omitted, kubectl must be available and its context must
point at the target cluster. The wrapper creates a uniquely named Secret in
the selected namespace and deletes it on exit. Use --kubeconfig or KUBECONFIG
when the context is not the current one.

The --verbose option prints the generated spark-submit command. The job
configuration is not part of it, but --conf values may still contain
credentials. Avoid --verbose in shared logs.
EOF
}

die() {
  echo "pudding k8s submit: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--confFile|--conf-file)
      [[ $# -ge 2 ]] || die "missing value for $1"
      CONF_FILE="$2"
      shift 2
      ;;
    -j|--jobFile|--job-file)
      [[ $# -ge 2 ]] || die "missing value for $1"
      JOB_FILE="$2"
      shift 2
      ;;
    --master)
      [[ $# -ge 2 ]] || die "missing value for --master"
      MASTER="$2"
      shift 2
      ;;
    --resource-base|--resourceBase)
      [[ $# -ge 2 ]] || die "missing value for $1"
      RESOURCE_BASE="$2"
      shift 2
      ;;
    --image|--k8s-image)
      [[ $# -ge 2 ]] || die "missing value for $1"
      IMAGE="$2"
      shift 2
      ;;
    --namespace|--k8s-namespace)
      [[ $# -ge 2 ]] || die "missing value for $1"
      NAMESPACE="$2"
      shift 2
      ;;
    --service-account|--k8s-service-account)
      [[ $# -ge 2 ]] || die "missing value for $1"
      SERVICE_ACCOUNT="$2"
      shift 2
      ;;
    --image-pull-secrets|--k8s-image-pull-secrets)
      [[ $# -ge 2 ]] || die "missing value for $1"
      PULL_SECRETS="$2"
      shift 2
      ;;
    --job-secret|--k8s-job-secret)
      [[ $# -ge 2 ]] || die "missing value for $1"
      JOB_SECRET="$2"
      shift 2
      ;;
    --job-secret-key|--k8s-job-secret-key)
      [[ $# -ge 2 ]] || die "missing value for $1"
      JOB_SECRET_KEY="$2"
      shift 2
      ;;
    --kubeconfig)
      [[ $# -ge 2 ]] || die "missing value for --kubeconfig"
      KUBECONFIG_PATH="$2"
      shift 2
      ;;
    --spark-home)
      [[ $# -ge 2 ]] || die "missing value for --spark-home"
      SPARK_HOME_ARG="$2"
      shift 2
      ;;
    --spark-pudding-home)
      [[ $# -ge 2 ]] || die "missing value for --spark-pudding-home"
      SPARK_PUDDING_HOME_VALUE="$2"
      shift 2
      ;;
    --driver-master)
      [[ $# -ge 2 ]] || die "missing value for --driver-master"
      DRIVER_MASTER="$2"
      shift 2
      ;;
    --conf)
      [[ $# -ge 2 ]] || die "missing value for --conf"
      EXTRA_CONF+=("$2")
      shift 2
      ;;
    --verbose)
      VERBOSE="true"
      shift
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1 (use --help)"
      ;;
  esac
done

[[ -n "$CONF_FILE" ]] || die "-c/--confFile is required"
[[ -n "$JOB_FILE" ]] || die "-j/--jobFile is required"
[[ -n "$IMAGE" ]] || die "--image is required"
[[ -n "$MASTER" ]] || die "--master is required"
[[ -n "$RESOURCE_BASE" ]] || die "--resource-base can not be empty"
[[ "$MASTER" == k8s://* ]] || die "--master must start with k8s://, got: $MASTER"

[[ -d "$PUDDING_HOME" ]] || die "spark-pudding distribution not found: $PUDDING_HOME"
PUDDING_HOME="$(cd "$PUDDING_HOME" && pwd)"
CLIENT_JAR="$PUDDING_HOME/bin/pudding-client.jar"
[[ -f "$CLIENT_JAR" ]] || die "client jar not found: $CLIENT_JAR"

[[ -d "$SPARK_PUDDING_HOME_VALUE" ]] || die \
  "spark-pudding home not found: $SPARK_PUDDING_HOME_VALUE"
SPARK_PUDDING_HOME_VALUE="$(cd "$SPARK_PUDDING_HOME_VALUE" && pwd)"
if ! compgen -G "$SPARK_PUDDING_HOME_VALUE/libs/pudding-core-*.jar" >/dev/null; then
  die "pudding-core jar not found in $SPARK_PUDDING_HOME_VALUE/libs"
fi

[[ -f "$CONF_FILE" ]] || die "environment properties file not found: $CONF_FILE"
[[ -f "$JOB_FILE" ]] || die "job configuration file not found: $JOB_FILE"
[[ -s "$JOB_FILE" ]] || die "job configuration file is empty: $JOB_FILE"
JOB_FILE="$(cd "$(dirname "$JOB_FILE")" && pwd)/$(basename "$JOB_FILE")"

[[ "$JOB_SECRET_KEY" =~ ^[-._a-zA-Z0-9]+$ ]] || die \
  "invalid --job-secret-key (use alphanumeric, '-', '_', or '.')"
(( ${#JOB_SECRET_KEY} <= 253 )) || die "--job-secret-key is too long"
if [[ -n "$JOB_SECRET" ]]; then
  [[ "$JOB_SECRET" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]] || die \
    "invalid --job-secret name: $JOB_SECRET"
  (( ${#JOB_SECRET} <= 253 )) || die "--job-secret name is too long"
fi
(( $(wc -c < "$JOB_FILE") <= 716800 )) || die \
  "job configuration is larger than 700 KiB; Kubernetes Secrets can not hold it"

if [[ -n "$KUBECONFIG_PATH" ]]; then
  export KUBECONFIG="$KUBECONFIG_PATH"
fi

# Resolve the context namespace for both the auto-created and existing Secret paths. Without
# this, an existing Secret in a non-default context namespace is looked up in "default".
if [[ -z "$NAMESPACE" ]] && command -v kubectl >/dev/null 2>&1; then
  NAMESPACE="$(kubectl config view --minify -o jsonpath='{..namespace}' 2>/dev/null || true)"
  NAMESPACE="${NAMESPACE:-default}"
fi

if [[ -z "$JOB_SECRET" ]]; then
  command -v kubectl >/dev/null 2>&1 || die \
    "kubectl is required to create the job configuration Secret; install kubectl, \
set KUBECONFIG, or pass --job-secret"
  JOB_SECRET="pudding-job-$(date -u +%Y%m%d%H%M%S)-$$-$RANDOM"
  AUTO_CREATED_SECRET="true"
  kubectl --namespace "$NAMESPACE" create secret generic "$JOB_SECRET" \
    --from-file="$JOB_SECRET_KEY=$JOB_FILE" >/dev/null || die \
    "failed to create job configuration Secret in namespace: $NAMESPACE"
  echo "job configuration Secret: $JOB_SECRET (auto-created, namespace: $NAMESPACE)" >&2
else
  echo "job configuration Secret: $JOB_SECRET (existing)" >&2
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  [[ -x "$JAVA_HOME/bin/java" ]] || die "java not found at $JAVA_HOME/bin/java"
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA="$(command -v java || true)"
  [[ -n "$JAVA" ]] || die "java not found; set JAVA_HOME or add java to PATH"
fi

ARGS=(
  -c "$CONF_FILE"
  -j "$JOB_FILE"
  --target kubernetes
  --master "$MASTER"
  --resourceBase "$RESOURCE_BASE"
  --k8s-image "$IMAGE"
  --k8s-job-secret "$JOB_SECRET"
  --k8s-job-secret-key "$JOB_SECRET_KEY"
  --sparkPuddingHome "$SPARK_PUDDING_HOME_VALUE"
)

if [[ -n "$SPARK_HOME_ARG" ]]; then
  ARGS+=(--sparkHome "$SPARK_HOME_ARG")
fi
if [[ -n "$NAMESPACE" ]]; then
  ARGS+=(--k8s-namespace "$NAMESPACE")
fi
if [[ -n "$SERVICE_ACCOUNT" ]]; then
  ARGS+=(--k8s-service-account "$SERVICE_ACCOUNT")
fi
if [[ -n "$PULL_SECRETS" ]]; then
  ARGS+=(--k8s-image-pull-secrets "$PULL_SECRETS")
fi
if [[ -n "$KUBECONFIG_PATH" ]]; then
  ARGS+=(--kubeconfig "$KUBECONFIG_PATH")
fi
if [[ "$VERBOSE" == "true" ]]; then
  echo "warning: --verbose prints the spark-submit command; --conf values may contain credentials" >&2
  ARGS+=(--verbose)
fi
if [[ ${#EXTRA_CONF[@]} -gt 0 ]]; then
  for entry in "${EXTRA_CONF[@]}"; do
    ARGS+=(--conf "$entry")
  done
fi
if [[ -n "$DRIVER_MASTER" ]]; then
  ARGS+=(--conf "spark.kubernetes.driverEnv.PUDDING_DRIVER_MASTER=$DRIVER_MASTER")
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  ARGS+=("${EXTRA_ARGS[@]}")
fi

echo "target: kubernetes" >&2
echo "master: $MASTER" >&2
echo "image: $IMAGE" >&2
echo "namespace: ${NAMESPACE:-<cluster default>}" >&2
echo "service account: ${SERVICE_ACCOUNT:-<cluster default>}" >&2
echo "resource base: $RESOURCE_BASE" >&2

STATUS=0
"$JAVA" -jar "$CLIENT_JAR" "${ARGS[@]}" || STATUS=$?
exit "$STATUS"
