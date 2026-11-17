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

# Submit a spark-pudding job with the packaged client.
#
# With a log file the job runs in the background; otherwise the client stays
# attached to the terminal and waits for the application to finish.

set -euo pipefail

usage() {
  cat <<'EOF'
Submit a spark-pudding job with the client bundled in this distribution.

Usage:
  pudding submit <env.properties> <job-file> [log-file]

Arguments:
  env.properties  Environment file passed to the client with -c
  job-file        JSON, YAML or HOCON job configuration passed with -j
  log-file        Write background job output to this file and return

Environment:
  JAVA_HOME       Java installation to use; defaults to java on PATH

The execution target is derived from the job master. Use pudding k8s submit for
Kubernetes jobs that need image, namespace or service account options.
EOF
}

die() {
  echo "pudding submit: $*" >&2
  exit 1
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage >&2
  exit 2
fi

ENV_FILE="$1"
JOB_FILE="$2"
LOG_FILE="${3:-}"

[[ -r "$ENV_FILE" ]] || die "environment file is not readable: $ENV_FILE"
[[ -r "$JOB_FILE" ]] || die "job file is not readable: $JOB_FILE"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_JAR="$SCRIPT_DIR/../pudding-client.jar"
[[ -r "$CLIENT_JAR" ]] || die "client jar not found: $CLIENT_JAR"

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA="$JAVA_HOME/bin/java"
  [[ -x "$JAVA" ]] || die "java not found under JAVA_HOME: $JAVA_HOME"
else
  JAVA="$(command -v java || true)"
  [[ -n "$JAVA" ]] || die "java not found; set JAVA_HOME or add java to PATH"
fi

CLIENT_ARGS=(-jar "$CLIENT_JAR" -c "$ENV_FILE" -j "$JOB_FILE")

if [[ -n "$LOG_FILE" ]]; then
  LOG_DIR="$(dirname "$LOG_FILE")"
  mkdir -p "$LOG_DIR"
  nohup "$JAVA" "${CLIENT_ARGS[@]}" </dev/null >"$LOG_FILE" 2>&1 &
  echo "started spark-pudding submission in background (pid $!, log $LOG_FILE)"
else
  exec "$JAVA" "${CLIENT_ARGS[@]}"
fi
