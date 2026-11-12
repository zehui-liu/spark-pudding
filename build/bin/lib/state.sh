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

# Inspect or repair incremental state without submitting a Spark job.
#
# Usage:
#   pudding state show <state-target> [source-id]
#   pudding state status <state-target> [source-id] [--json]
#   pudding state list <state-target>
#   pudding state reset <state-target> <source-id> --cursor <cursor> [--force]
#   pudding state reset <state-target> <source-id> --clear [--force]
#   pudding state resolve <state-target> <source-id> --commit|--abort [--force]
#   pudding state unlock <state-target> <source-id>
#   pudding state migrate <state-target>
#
# A state target is a filesystem path or @/path/to/state.properties.
# Redis and etcd provider jars and the S3A runtime are loaded from the distribution libs directory automatically.
# SPARK_HOME is used for the Hadoop filesystem classes. When it is not set,
# the script falls back to `hadoop classpath`.

set -euo pipefail

usage() {
  cat <<'EOF'
Inspect or repair spark-pudding incremental state.

Usage:
  pudding state show <state-target> [source-id]
  pudding state status <state-target> [source-id] [--json]
  pudding state list <state-target>
  pudding state reset <state-target> <source-id> --cursor <cursor> [--force]
  pudding state reset <state-target> <source-id> --clear [--force]
  pudding state resolve <state-target> <source-id> --commit|--abort [--force]
  pudding state unlock <state-target> <source-id>
  pudding state migrate <state-target>

A state target is a filesystem path or @/path/to/state.properties. A properties
target selects the backend (`backend=filesystem|jdbc|redis|etcd`) and supplies its
options. `migrate` creates or upgrades a JDBC state table. Redis and etcd provider
jars and the S3A runtime are loaded from the distribution libs directory automatically.

Environment:
  JAVA_HOME               Java installation to use; defaults to java on PATH
  SPARK_HOME              Spark installation containing jars/; required unless hadoop is on PATH
  HADOOP_CONF_DIR         Hadoop configuration directory for HDFS or object-store state paths
  PUDDING_STATE_CLASSPATH Extra classpath entries, for example a JDBC driver jar
EOF
}

die() {
  echo "pudding state: $*" >&2
  exit 2
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_JAR="$SCRIPT_DIR/../pudding-client.jar"
[[ -f "$CLIENT_JAR" ]] || die "pudding-client.jar not found: $CLIENT_JAR"

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA_BIN="$JAVA_HOME/bin/java"
else
  JAVA_BIN="java"
fi

EXTRA_CONF=""
if [[ -n "${HADOOP_CONF_DIR:-}" ]]; then
  EXTRA_CONF=":$HADOOP_CONF_DIR"
fi
if [[ -n "${YARN_CONF_DIR:-}" ]]; then
  EXTRA_CONF="$EXTRA_CONF:$YARN_CONF_DIR"
fi

STATE_PROVIDERS=""
for provider_jar in \
  "$SCRIPT_DIR/../../libs/"*state-provider-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-aws-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-oss-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-azure-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"gcs-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-cos-runtime-*.jar; do
  [[ -f "$provider_jar" ]] || continue
  STATE_PROVIDERS="$STATE_PROVIDERS:$provider_jar"
done

if [[ -n "${SPARK_HOME:-}" && -d "$SPARK_HOME/jars" ]]; then
  CLASSPATH="$CLIENT_JAR:$SPARK_HOME/jars/*$EXTRA_CONF$STATE_PROVIDERS"
elif command -v hadoop >/dev/null 2>&1; then
  CLASSPATH="$CLIENT_JAR:$(hadoop classpath)$EXTRA_CONF$STATE_PROVIDERS"
else
  die "set SPARK_HOME to a Spark 3.4 installation or put hadoop on PATH"
fi

if [[ -n "${PUDDING_STATE_CLASSPATH:-}" ]]; then
  CLASSPATH="$CLASSPATH:$PUDDING_STATE_CLASSPATH"
fi

exec "$JAVA_BIN" ${JAVA_OPTS:-} -cp "$CLASSPATH" \
  org.pudding.core.incremental.IncrementalStateTool "$@"
