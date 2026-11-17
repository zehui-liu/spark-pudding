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

# Verify that the assembled distribution contains every component and shared support module.
# This is a build-time check; it is intentionally not shipped in the tar.

set -euo pipefail

usage() {
  cat <<'EOF'
Verify that an assembled spark-pudding distribution contains all module jars.

Usage:
  verify-distribution.sh <distribution-dir> [repository-root]

Arguments:
  distribution-dir  Unpacked spark-pudding distribution
  repository-root   Source tree root; defaults to two levels above this script

Exit status is non-zero when a connector, support module, core jar or the client jar is missing.
EOF
}

die() {
  echo "verify-distribution: $*" >&2
  exit 1
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

[[ $# -ge 1 && $# -le 2 ]] || {
  usage >&2
  exit 2
}

DISTRIBUTION="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="${2:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

[[ -d "$DISTRIBUTION/libs" ]] || die "libs directory not found: $DISTRIBUTION/libs"
[[ -d "$REPOSITORY_ROOT" ]] || die "repository root not found: $REPOSITORY_ROOT"
[[ -f "$DISTRIBUTION/bin/pudding-client.jar" ]] || \
  die "client jar not found: $DISTRIBUTION/bin/pudding-client.jar"
[[ -x "$DISTRIBUTION/bin/pudding" ]] || \
  die "unified CLI not found: $DISTRIBUTION/bin/pudding"
for tool in submit checkpoint state capabilities maintenance maintenance-server \
  k8s-submit k8s-build-image k8s-kind-setup; do
  [[ -x "$DISTRIBUTION/bin/lib/$tool.sh" ]] || \
    die "internal tool not found: $DISTRIBUTION/bin/lib/$tool.sh"
done
[[ -f "$DISTRIBUTION/libs/table-maintenance.jar" ]] || \
  die "table-maintenance jar not found: $DISTRIBUTION/libs/table-maintenance.jar"
[[ -f "$DISTRIBUTION/libs/maintenance-server.jar" ]] || \
  die "maintenance-server jar not found: $DISTRIBUTION/libs/maintenance-server.jar"
[[ -s "$DISTRIBUTION/capabilities.yaml" ]] || \
  die "capability manifest not found or empty: $DISTRIBUTION/capabilities.yaml"
for kind in source sink transform connection catalog; do
  grep -qE "^[[:space:]]*(-[[:space:]]+)?kind: ['\"]?$kind['\"]?$" \
    "$DISTRIBUTION/capabilities.yaml" || \
    die "capability manifest has no $kind components"
done

LIBS="$(cd "$DISTRIBUTION/libs" && pwd)"
REPOSITORY_ROOT="$(cd "$REPOSITORY_ROOT" && pwd)"
MISSING=0
CHECKED=0

check_artifact() {
  local artifact="$1"
  CHECKED=$((CHECKED + 1))
  if ! compgen -G "$LIBS/${artifact}-*.jar" >/dev/null; then
    echo "missing component jar for $artifact in $LIBS" >&2
    MISSING=$((MISSING + 1))
  fi
}

check_artifact "pudding-core"

for group in connector provider runtime source sink transform; do
  [[ -d "$REPOSITORY_ROOT/$group" ]] || continue
  for pom in "$REPOSITORY_ROOT/$group"/*/pom.xml; do
    [[ -f "$pom" ]] || continue
    artifact="$(basename "$(dirname "$pom")")"
    check_artifact "$artifact"
  done
done

if [[ "$MISSING" -ne 0 ]]; then
  die "$MISSING of $CHECKED module jars are missing from $LIBS"
fi

S3_RUNTIME="$(compgen -G "$LIBS/hadoop-aws-runtime-*.jar" | head -1 || true)"
[[ -n "$S3_RUNTIME" ]] || die "hadoop-aws-runtime jar not found in $LIBS"
for entry in \
  org/apache/hadoop/fs/s3a/S3AFileSystem.class \
  com/amazonaws/auth/DefaultAWSCredentialsProviderChain.class; do
  unzip -l "$S3_RUNTIME" "$entry" >/dev/null 2>&1 || \
    die "hadoop-aws-runtime is missing $entry"
done

ICEBERG_RUNTIME="$(compgen -G "$LIBS/iceberg-runtime-*.jar" | head -1 || true)"
[[ -n "$ICEBERG_RUNTIME" ]] || die "iceberg-runtime jar not found in $LIBS"
for entry in \
  org/apache/iceberg/aws/s3/S3FileIO.class \
  software/amazon/awssdk/services/s3/S3Client.class; do
  unzip -l "$ICEBERG_RUNTIME" "$entry" >/dev/null 2>&1 || \
    die "iceberg-runtime is missing $entry"
done

CDC_SOURCE="$(compgen -G "$LIBS/database-cdc-source-*.jar" | head -1 || true)"
[[ -n "$CDC_SOURCE" ]] || die "database-cdc-source jar not found in $LIBS"
for entry in \
  META-INF/services/org.pudding.core.definition.DataSource \
  org/pudding/source/cdc/CdcTableProvider.class \
  org/pudding/source/cdc/MysqlCdcSource.class \
  org/pudding/source/cdc/PostgresCdcSource.class \
  org/pudding/source/cdc/MongodbCdcSource.class; do
  unzip -l "$CDC_SOURCE" "$entry" >/dev/null 2>&1 || \
    die "database-cdc-source is missing $entry"
done

CDC_RUNTIME="$(compgen -G "$LIBS/cdc-runtime-*.jar" | head -1 || true)"
[[ -n "$CDC_RUNTIME" ]] || die "cdc-runtime jar not found in $LIBS"
for entry in \
  io/debezium/connector/mysql/MySqlConnector.class \
  io/debezium/connector/postgresql/PostgresConnector.class \
  com/mongodb/client/MongoClients.class \
  com/mysql/cj/jdbc/Driver.class \
  org/postgresql/Driver.class \
  org/pudding/cdc/shaded/jackson/databind/ObjectMapper.class; do
  unzip -l "$CDC_RUNTIME" "$entry" >/dev/null 2>&1 || \
    die "cdc-runtime is missing $entry"
done
if unzip -l "$CDC_RUNTIME" com/fasterxml/jackson/databind/ObjectMapper.class \
    >/dev/null 2>&1; then
  die "cdc-runtime contains unrelocated Jackson classes"
fi
if unzip -l "$CDC_RUNTIME" org/antlr/v4/runtime/Parser.class >/dev/null 2>&1; then
  die "cdc-runtime must use Spark's ANTLR runtime, not bundle its own"
fi

echo "distribution packaging verified: $CHECKED module jars present in $LIBS"
