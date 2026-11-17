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

# Inspect or verify a spark-pudding capability manifest.
#
# Usage:
#   pudding capabilities verify <distribution-dir>
#   pudding capabilities show <distribution-dir>
#   pudding capabilities diff <previous-distribution> <current-distribution>

set -euo pipefail

usage() {
  cat <<'EOF'
Inspect or verify a spark-pudding capability manifest.

Usage:
  pudding capabilities verify <distribution-dir>
  pudding capabilities show <distribution-dir>
  pudding capabilities diff <previous-distribution> <current-distribution>

The command reads capabilities.yaml and jar metadata only; it does not need
Spark or connector classes on its classpath.
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_JAR="$SCRIPT_DIR/../pudding-client.jar"

if [[ ! -f "$CLIENT_JAR" ]]; then
  echo "pudding capabilities: pudding-client.jar not found: $CLIENT_JAR" >&2
  exit 2
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA_BIN="$JAVA_HOME/bin/java"
else
  JAVA_BIN="java"
fi

exec "$JAVA_BIN" ${JAVA_OPTS:-} -cp "$CLIENT_JAR" \
  org.pudding.core.capability.CapabilityTool "$@"
