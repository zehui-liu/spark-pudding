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

# Start the HTTP maintenance control plane through spark-submit.
#
# The server owns one Spark maintenance session per task. All format runtimes are
# placed on the Spark classpath so a single server can handle Iceberg, Paimon and Hudi requests.

set -euo pipefail

usage() {
  cat <<'EOF'
Start the spark-pudding HTTP maintenance server.

Usage:
  pudding maintenance-server [options]

Options:
  --host <host>                Bind host (default 127.0.0.1)
  --port <port>                Bind port (default 8080; 0 picks a free port)
  --state-file <file>          Durable task state JSON
  --audit-file <file>          Append-only audit JSONL
  --auth-token-file <file>     Bearer token file; or set PUDDING_MAINTENANCE_TOKEN
  --allow-anonymous            Allow requests without a token (local use only)
  --conf-file <file>           Spark properties containing only spark.* keys
  --master <url>               Spark master for maintenance sessions
  --hadoop-conf <dir>          XML Hadoop configuration directory
  --max-request-bytes <n>      Request body limit (default 1048576)
  --max-tasks <n>              Retained task limit (default 1000)
  -h, --help

Environment:
  SPARK_HOME                   Spark installation containing bin/spark-submit
  SPARK_MASTER                 Optional default master passed to spark-submit
  PUDDING_HOME                 Unpacked spark-pudding distribution
  PUDDING_OBJECT_STORE_DIR     Optional object-store Hadoop FS jars
  PUDDING_MAINTENANCE_TOKEN    Bearer token read by the server
EOF
}

die() {
  echo "pudding maintenance-server: $*" >&2
  exit 2
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUDDING_HOME="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVER_JAR="$PUDDING_HOME/libs/maintenance-server.jar"
[[ -f "$SERVER_JAR" ]] || die "maintenance-server.jar not found: $SERVER_JAR"

if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  die "set SPARK_HOME to a Spark 3.4 installation or put spark-submit on PATH"
fi

JARS=""
for runtime_jar in \
  "$PUDDING_HOME/libs/"iceberg-runtime-*.jar \
  "$PUDDING_HOME/libs/"paimon-runtime-*.jar \
  "$PUDDING_HOME/libs/"hudi-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-aws-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-oss-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-azure-runtime-*.jar \
  "$PUDDING_HOME/libs/"gcs-runtime-*.jar \
  "$PUDDING_HOME/libs/"hadoop-cos-runtime-*.jar \
  "${PUDDING_OBJECT_STORE_DIR:-/nonexistent}/"*.jar; do
  [[ -f "$runtime_jar" ]] || continue
  if [[ -n "$JARS" ]]; then
    JARS="$JARS,$runtime_jar"
  else
    JARS="$runtime_jar"
  fi
done

MASTER_VALUE="${SPARK_MASTER:-}"
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --master)
      [[ $# -ge 2 ]] || die "--master requires a value"
      MASTER_VALUE="$2"
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --master=*)
      MASTER_VALUE="${1#--master=}"
      ARGS+=("$1")
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

SPARK_ARGS=(--class org.pudding.maintenance.server.MaintenanceServer)
if [[ -n "$MASTER_VALUE" ]]; then
  SPARK_ARGS+=(--master "$MASTER_VALUE")
fi
if [[ -n "$JARS" ]]; then
  SPARK_ARGS+=(--jars "$JARS")
fi

exec "$SPARK_SUBMIT" "${SPARK_ARGS[@]}" "$SERVER_JAR" "${ARGS[@]}"
