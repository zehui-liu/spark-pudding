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

# Build a Spark-on-Kubernetes image containing a Spark distribution and the
# unpacked spark-pudding distribution. The image is self-contained, so jobs can
# use a local:///opt/spark-pudding resource base.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
SPARK_HOME="${SPARK_HOME:-}"
IMAGE=""
BASE_IMAGE="eclipse-temurin:11-jdk-jammy"
KIND_CLUSTER=""
DOCKER="${DOCKER:-docker}"
PLATFORM=""
NO_CACHE="false"
KEEP_CONTEXT="false"
CONTEXT_DIR=""

usage() {
  cat <<'EOF'
Build a self-contained Spark + spark-pudding Kubernetes image.

Usage:
  pudding k8s build-image --spark-home <spark-distribution> [options]

Options:
  --spark-home <dir>      Spark distribution directory (or set SPARK_HOME)
  --pudding-home <dir>    Unpacked spark-pudding distribution
                          (default: parent of this script)
  --image <name:tag>      Output image (default: spark-pudding:<version>)
  --base-image <image>    Debian/Ubuntu-style base with bash and useradd
                          (default: eclipse-temurin:11-jdk-jammy)
  --kind-cluster <name>   Load the image into a kind cluster after building
  --docker <command>      Container build command (default: docker)
  --platform <platform>   Optional image platform, for example linux/amd64
  --no-cache              Disable the Docker build cache
  --keep-context          Keep the temporary build context for inspection
  -h, --help              Show this help

Environment:
  PUDDING_HOME, SPARK_HOME, DOCKER

The build context is created in a temporary directory. Spark and the libs
directory are copied there. The Spark conf directory and all other host
configuration directories are excluded from the image.
EOF
}

die() {
  echo "pudding k8s build-image: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$CONTEXT_DIR" && -d "$CONTEXT_DIR" ]]; then
    if [[ "$KEEP_CONTEXT" == "true" ]]; then
      echo "build context kept: $CONTEXT_DIR" >&2
    else
      rm -rf "$CONTEXT_DIR"
    fi
  fi
}

trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spark-home)
      [[ $# -ge 2 ]] || die "missing value for --spark-home"
      SPARK_HOME="$2"
      shift 2
      ;;
    --pudding-home)
      [[ $# -ge 2 ]] || die "missing value for --pudding-home"
      PUDDING_HOME="$2"
      shift 2
      ;;
    --image)
      [[ $# -ge 2 ]] || die "missing value for --image"
      IMAGE="$2"
      shift 2
      ;;
    --base-image)
      [[ $# -ge 2 ]] || die "missing value for --base-image"
      BASE_IMAGE="$2"
      shift 2
      ;;
    --kind-cluster)
      [[ $# -ge 2 ]] || die "missing value for --kind-cluster"
      KIND_CLUSTER="$2"
      shift 2
      ;;
    --docker)
      [[ $# -ge 2 ]] || die "missing value for --docker"
      DOCKER="$2"
      shift 2
      ;;
    --platform)
      [[ $# -ge 2 ]] || die "missing value for --platform"
      PLATFORM="$2"
      shift 2
      ;;
    --no-cache)
      NO_CACHE="true"
      shift
      ;;
    --keep-context)
      KEEP_CONTEXT="true"
      shift
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

[[ -n "$SPARK_HOME" ]] || die "--spark-home or SPARK_HOME is required"
[[ -x "$SPARK_HOME/bin/spark-submit" ]] || die \
  "Spark distribution not found at $SPARK_HOME (bin/spark-submit is missing)"
[[ -d "$SPARK_HOME/jars" ]] || die "Spark jars directory not found: $SPARK_HOME/jars"

[[ -d "$PUDDING_HOME" ]] || die "spark-pudding distribution not found: $PUDDING_HOME"
PUDDING_HOME="$(cd "$PUDDING_HOME" && pwd)"
[[ -d "$PUDDING_HOME/libs" ]] || die \
  "libs directory not found in $PUDDING_HOME; pass an unpacked spark-pudding distribution"
CORE_JAR="$(compgen -G "$PUDDING_HOME/libs/pudding-core-*.jar" | sort | head -n 1 || true)"
[[ -n "$CORE_JAR" ]] || die "pudding-core jar not found in $PUDDING_HOME/libs"
CLIENT_JAR="$PUDDING_HOME/bin/pudding-client.jar"
[[ -f "$CLIENT_JAR" ]] || die "client jar not found: $CLIENT_JAR"
[[ -f "$PUDDING_HOME/k8s/Dockerfile" ]] || die "missing $PUDDING_HOME/k8s/Dockerfile"
[[ -f "$PUDDING_HOME/k8s/entrypoint.sh" ]] || die "missing $PUDDING_HOME/k8s/entrypoint.sh"

command -v "$DOCKER" >/dev/null 2>&1 || die "container build command not found: $DOCKER"

PUDDING_VERSION="$(basename "$CORE_JAR")"
PUDDING_VERSION="${PUDDING_VERSION#pudding-core-}"
PUDDING_VERSION="${PUDDING_VERSION%.jar}"
[[ -n "$PUDDING_VERSION" ]] || PUDDING_VERSION="dev"
if [[ -z "$IMAGE" ]]; then
  IMAGE="spark-pudding:${PUDDING_VERSION}"
fi

SPARK_VERSION="unknown"
if [[ -f "$SPARK_HOME/RELEASE" ]]; then
  SPARK_VERSION="$(awk '/^Spark / {print $2; exit}' "$SPARK_HOME/RELEASE")"
  [[ -n "$SPARK_VERSION" ]] || SPARK_VERSION="unknown"
fi

EXPECTED_SPARK_VERSION=""
if command -v unzip >/dev/null 2>&1; then
  EXPECTED_SPARK_VERSION="$(unzip -p "$CLIENT_JAR" META-INF/MANIFEST.MF 2>/dev/null \
    | tr -d '\r' \
    | awk -F': ' '/^Implementation-Spark-Version:/ {print $2; exit}')"
fi
if [[ -z "$EXPECTED_SPARK_VERSION" ]]; then
  echo "warning: client manifest has no Spark version; skipping the compatibility check" >&2
elif [[ "$SPARK_VERSION" == "unknown" ]]; then
  echo "warning: could not detect the version of Spark at $SPARK_HOME" >&2
elif [[ "$SPARK_VERSION" != "$EXPECTED_SPARK_VERSION" ]]; then
  die "Spark $SPARK_VERSION at $SPARK_HOME does not match the project baseline" \
    "$EXPECTED_SPARK_VERSION; use a matching Spark distribution"
fi

echo "staging build context for $IMAGE" >&2
CONTEXT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/spark-pudding-image.XXXXXX")"
chmod 0700 "$CONTEXT_DIR"

mkdir -p "$CONTEXT_DIR/spark" "$CONTEXT_DIR/pudding"
cp -R "$SPARK_HOME"/. "$CONTEXT_DIR/spark/"
# Never bake host Spark configuration into the image. The cluster receives
# driver configuration from the submission; a host conf directory may contain
# credentials or site-specific overrides.
rm -rf "$CONTEXT_DIR/spark/conf"
mkdir -p "$CONTEXT_DIR/spark/conf"
cp -R "$PUDDING_HOME/libs" "$CONTEXT_DIR/pudding/libs"
if [[ -f "$PUDDING_HOME/examples/data/demo.csv" ]]; then
  mkdir -p "$CONTEXT_DIR/pudding/examples/data"
  cp "$PUDDING_HOME/examples/data/demo.csv" \
    "$CONTEXT_DIR/pudding/examples/data/demo.csv"
fi
cp "$PUDDING_HOME/k8s/Dockerfile" "$CONTEXT_DIR/Dockerfile"
cp "$PUDDING_HOME/k8s/entrypoint.sh" "$CONTEXT_DIR/entrypoint.sh"
chmod 0755 "$CONTEXT_DIR/entrypoint.sh"

build_args=(
  build
  --file "$CONTEXT_DIR/Dockerfile"
  --tag "$IMAGE"
  --build-arg "PUDDING_VERSION=$PUDDING_VERSION"
  --build-arg "BASE_IMAGE=$BASE_IMAGE"
)
if [[ -n "$PLATFORM" ]]; then
  build_args+=(--platform "$PLATFORM")
fi
if [[ "$NO_CACHE" == "true" ]]; then
  build_args+=(--no-cache)
fi
build_args+=("$CONTEXT_DIR")

echo "building $IMAGE with Spark $SPARK_VERSION" >&2
"$DOCKER" "${build_args[@]}"

if [[ -n "$KIND_CLUSTER" ]]; then
  command -v kind >/dev/null 2>&1 || die "kind command not found"
  echo "loading $IMAGE into kind cluster $KIND_CLUSTER" >&2
  kind load docker-image "$IMAGE" --name "$KIND_CLUSTER"
fi

cat <<EOF
image: $IMAGE
spark version: $SPARK_VERSION
pudding version: $PUDDING_VERSION
resource base: local:///opt/spark-pudding
EOF
