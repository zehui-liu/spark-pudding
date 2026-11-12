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

# Inspect or remove Structured Streaming checkpoints without submitting a Spark job.
#
# Usage:
#   pudding checkpoint show <location> [--json]
#   pudding checkpoint status <location> [--json]
#   pudding checkpoint delete <location> [--dry-run]
#   pudding checkpoint cleanup <root> --older-than <duration> [--keep-last N] [--dry-run]
#
# SPARK_HOME is used for the Hadoop filesystem classes. When it is not set,
# the script falls back to `hadoop classpath`.

set -euo pipefail

usage() {
  cat <<'EOF'
Inspect or remove spark-pudding streaming checkpoints.

Usage:
  pudding checkpoint show <location> [--json] [--conf-file <file>] [--hadoop-conf <dir>]
  pudding checkpoint status <location> [--json] [--conf-file <file>] [--hadoop-conf <dir>]
  pudding checkpoint delete <location> [--dry-run] [--conf-file <file>] [--hadoop-conf <dir>]
  pudding checkpoint cleanup <root> --older-than <duration> [--keep-last N] [--dry-run]
    [--conf-file <file>] [--hadoop-conf <dir>]

<duration> is a positive number with a unit: ms, s, m, h or d (for example 7d).
`cleanup` scans only direct directory children of <root>; <root> must be the
parent of the checkpoint directories, not a checkpoint itself. Stop every
writer before deleting a checkpoint. --conf-file accepts fs.* keys, for example
fs.s3a.endpoint; --hadoop-conf adds XML files from a Hadoop configuration dir.

Environment:
  JAVA_HOME                    Java installation to use; defaults to java on PATH
  SPARK_HOME                   Spark installation containing jars/; required unless hadoop is on PATH
  HADOOP_CONF_DIR              Hadoop configuration directory for HDFS or object-store paths
  PUDDING_CHECKPOINT_CLASSPATH Extra classpath entries
  PUDDING_OBJECT_STORE_DIR     Optional directory with object-store Hadoop FS jars
EOF
}

die() {
  echo "pudding checkpoint: $*" >&2
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

OBJECT_STORE_JARS=""
for runtime_jar in \
  "$SCRIPT_DIR/../../libs/"hadoop-aws-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-oss-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-azure-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"gcs-runtime-*.jar \
  "$SCRIPT_DIR/../../libs/"hadoop-cos-runtime-*.jar \
  "${PUDDING_OBJECT_STORE_DIR:-/nonexistent}/"*.jar; do
  [[ -f "$runtime_jar" ]] || continue
  OBJECT_STORE_JARS="$OBJECT_STORE_JARS:$runtime_jar"
done

if [[ -n "${SPARK_HOME:-}" && -d "$SPARK_HOME/jars" ]]; then
  CLASSPATH="$CLIENT_JAR:$SPARK_HOME/jars/*$EXTRA_CONF$OBJECT_STORE_JARS"
elif command -v hadoop >/dev/null 2>&1; then
  CLASSPATH="$CLIENT_JAR:$(hadoop classpath)$EXTRA_CONF$OBJECT_STORE_JARS"
else
  die "set SPARK_HOME to a Spark 3.4 installation or put hadoop on PATH"
fi

if [[ -n "${PUDDING_CHECKPOINT_CLASSPATH:-}" ]]; then
  CLASSPATH="$CLASSPATH:$PUDDING_CHECKPOINT_CLASSPATH"
fi

exec "$JAVA_BIN" ${JAVA_OPTS:-} -cp "$CLASSPATH" \
  org.pudding.core.util.CheckpointTool "$@"
