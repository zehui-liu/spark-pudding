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

# Run one table-maintenance action through spark-submit.
#
# The wrapper selects the format runtime and passes all arguments to the
# maintenance CLI. The CLI owns action parsing, validation and JSON output.

set -euo pipefail

usage() {
  cat <<'EOF'
Run a Hudi, Paimon or Iceberg table-maintenance action.

Usage:
  pudding maintenance <format> <action> [options]
  pudding maintenance --help

Formats:
  iceberg  status | rewrite-data-files | rewrite-manifests | expire-snapshots |
           remove-orphan-files
  paimon   status | compact | compact-manifests | expire-snapshots |
           remove-orphan-files
  hudi     status | clean | compact | cluster | archive

Run `pudding maintenance <format> <action> --help` for the CLI options.
Destructive actions require --confirm; use --dry-run to preview.

Environment:
  SPARK_HOME              Spark installation; spark-submit is taken from here
  SPARK_MASTER            spark-submit master, for example local[*] or yarn
  JAVA_HOME               Java installation used by spark-submit
  PUDDING_MAINTENANCE_JARS Additional comma-separated jars
  PUDDING_OBJECT_STORE_DIR Optional object-store Hadoop FS jars

The format runtime is selected from libs/iceberg-runtime-*.jar,
libs/paimon-runtime-*.jar or libs/hudi-runtime-*.jar. Catalog and credential
settings belong in a --conf-file containing only spark.* keys; keep that file
at mode 0600.
EOF
}

die() {
  echo "pudding maintenance: $*" >&2
  exit 2
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

[[ $# -ge 2 ]] || {
  usage >&2
  exit 2
}

FORMAT="$1"
case "$FORMAT" in
  iceberg|paimon|hudi) ;;
  *)
    die "unsupported format: $FORMAT (expected iceberg, paimon or hudi)"
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUDDING_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"
MAINTENANCE_JAR="$PUDDING_HOME/libs/table-maintenance.jar"
[[ -f "$MAINTENANCE_JAR" ]] || \
  die "table-maintenance.jar not found: $MAINTENANCE_JAR"

RUNTIME_PATTERN="$PUDDING_HOME/libs/${FORMAT}-runtime-*.jar"
RUNTIME_JAR="$(compgen -G "$RUNTIME_PATTERN" | head -1 || true)"
[[ -n "$RUNTIME_JAR" ]] || \
  die "runtime jar not found for format $FORMAT: $RUNTIME_PATTERN"

JARS="$RUNTIME_JAR"
for extra in \
  "$PUDDING_HOME/libs/"hadoop-aws-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-oss-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-azure-runtime-*.jar \
  "$PUDDING_HOME/libs/"gcs-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-cos-runtime-*.jar \
  "${PUDDING_OBJECT_STORE_DIR:-/nonexistent}/"*.jar; do
  [[ -f "$extra" ]] || continue
  JARS="$JARS,$extra"
done
if [[ -n "${PUDDING_MAINTENANCE_JARS:-}" ]]; then
  JARS="$JARS,$PUDDING_MAINTENANCE_JARS"
fi

if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  die "set SPARK_HOME to a Spark 3.4 installation or put spark-submit on PATH"
fi

# Pull --master out of the CLI arguments so spark-submit sees it before the
# application jar. The same value is harmless if the CLI also receives it.
MASTER_VALUE="${SPARK_MASTER:-}"
CLI_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --master)
      [[ $# -ge 2 ]] || die "--master requires a value"
      MASTER_VALUE="$2"
      shift 2
      ;;
    --master=*)
      MASTER_VALUE="${1#--master=}"
      shift
      ;;
    *)
      CLI_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -n "$MASTER_VALUE" ]]; then
  exec "$SPARK_SUBMIT" \
    --master "$MASTER_VALUE" \
    --class org.pudding.maintenance.cli.MaintenanceCli \
    --jars "$JARS" \
    "$MAINTENANCE_JAR" "${CLI_ARGS[@]}"
else
  exec "$SPARK_SUBMIT" \
    --class org.pudding.maintenance.cli.MaintenanceCli \
    --jars "$JARS" \
    "$MAINTENANCE_JAR" "${CLI_ARGS[@]}"
fi
