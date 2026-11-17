#!/usr/bin/env bash
#
# Copyright 2024-2026 the spark-pudding authors
#
# Licensed under the Apache License, Version 2.0
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

# Create a local kind cluster and the namespace-scoped resources needed by a
# Spark driver. This is a development and smoke-test helper, not a production
# RBAC installer.

set -euo pipefail

CLUSTER="pudding"
NAMESPACE="pudding-k8s-test"
SERVICE_ACCOUNT="spark"
IMAGE=""
KUBECONFIG_FILE="${KUBECONFIG:-}"
CONTEXT=""

usage() {
  cat <<'EOF'
Create a kind cluster and Spark driver RBAC for local Kubernetes tests.

Usage:
  pudding k8s kind-setup [options]

Options:
  --cluster <name>           kind cluster name (default: pudding)
  --namespace <name>         Kubernetes namespace (default: pudding-k8s-test)
  --service-account <name>   Driver service account (default: spark)
  --image <name:tag>         Load this image into the kind cluster
  --kubeconfig <file>        kubeconfig used by kind and kubectl
  -h, --help                 Show this help

Environment:
  KUBECONFIG

The script is idempotent: it reuses an existing cluster, namespace and service
account, and reapplies the namespaced driver Role and RoleBinding.
EOF
}

die() {
  echo "pudding k8s kind-setup: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster)
      [[ $# -ge 2 ]] || die "missing value for --cluster"
      CLUSTER="$2"
      shift 2
      ;;
    --namespace)
      [[ $# -ge 2 ]] || die "missing value for --namespace"
      NAMESPACE="$2"
      shift 2
      ;;
    --service-account)
      [[ $# -ge 2 ]] || die "missing value for --service-account"
      SERVICE_ACCOUNT="$2"
      shift 2
      ;;
    --image)
      [[ $# -ge 2 ]] || die "missing value for --image"
      IMAGE="$2"
      shift 2
      ;;
    --kubeconfig)
      [[ $# -ge 2 ]] || die "missing value for --kubeconfig"
      KUBECONFIG_FILE="$2"
      shift 2
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

command -v kind >/dev/null 2>&1 || die "kind command not found"
command -v kubectl >/dev/null 2>&1 || die "kubectl command not found"

if [[ -n "$KUBECONFIG_FILE" ]]; then
  export KUBECONFIG="$KUBECONFIG_FILE"
fi

clusters="$(kind get clusters 2>/dev/null || true)"
if ! printf '%s\n' "$clusters" | grep -Fqx "$CLUSTER"; then
  echo "creating kind cluster $CLUSTER" >&2
  kind create cluster --name "$CLUSTER"
fi

CONTEXT="kind-$CLUSTER"
KUBECTL=(kubectl)
if [[ -n "$KUBECONFIG_FILE" ]]; then
  KUBECTL+=(--kubeconfig "$KUBECONFIG_FILE")
fi

kubectl_ctx() {
  "${KUBECTL[@]}" --context "$CONTEXT" "$@"
}

kubectl_ctx cluster-info >/dev/null \
  || die "kubectl context $CONTEXT is not available"

if ! kubectl_ctx get namespace "$NAMESPACE" >/dev/null 2>&1; then
  kubectl_ctx create namespace "$NAMESPACE"
fi

kubectl_ctx apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT}
  namespace: ${NAMESPACE}
EOF

kubectl_ctx apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-driver
  namespace: ${NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log", "services", "configmaps", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-driver
  namespace: ${NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: ${SERVICE_ACCOUNT}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-driver
EOF

if [[ -n "$IMAGE" ]]; then
  echo "loading $IMAGE into kind cluster $CLUSTER" >&2
  kind load docker-image "$IMAGE" --name "$CLUSTER"
fi

API_SERVER="$(kubectl_ctx config view --minify \
  -o jsonpath='{.clusters[0].cluster.server}')"
[[ -n "$API_SERVER" ]] || die "could not determine the API server for $CONTEXT"

echo
echo "kind cluster:      $CLUSTER"
echo "kubectl context:   $CONTEXT"
echo "namespace:         $NAMESPACE"
echo "service account:   $SERVICE_ACCOUNT"
echo "kubernetes master: k8s://$API_SERVER"
echo
echo "Submit a job with:"
echo "  export PUDDING_HOME=<unpacked spark-pudding distribution>"
echo "  \"\$PUDDING_HOME/bin/pudding\" k8s submit \\"
echo "    -c \"\$PUDDING_HOME/conf/env.k8s.properties\" \\"
echo "    -j \"\$PUDDING_HOME/k8s/job-example.yaml\" \\"
echo "    --master 'k8s://$API_SERVER' \\"
echo "    --image '${IMAGE:-<image>}' \\"
echo "    --namespace '$NAMESPACE' \\"
echo "    --service-account '$SERVICE_ACCOUNT' \\"
echo "    --resource-base local:///opt/spark-pudding \\"
echo "    --driver-master k8s://https://kubernetes.default.svc"
