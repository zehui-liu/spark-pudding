#!/bin/bash
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

# Spark Kubernetes container entrypoint. The Spark driver starts a local
# spark-submit (client mode) and executors start KubernetesExecutorBackend.
set -e

# Support arbitrary UIDs, as used by OpenShift and similar platforms.
if [[ -z "$(getent passwd "$(id -u)" 2>/dev/null || true)" && -w /etc/passwd ]]; then
  echo "$(id -u):x:$(id -u):$(id -g):${SPARK_USER_NAME:-anonymous uid}:$SPARK_HOME:/bin/false" \
    >> /etc/passwd
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 >/dev/null \
    | grep 'java.home' | awk '{print $3}')
fi

SPARK_CLASSPATH="${SPARK_CLASSPATH:+$SPARK_CLASSPATH:}$SPARK_HOME/jars/*"
if [[ -n "${SPARK_EXTRA_CLASSPATH:-}" ]]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_EXTRA_CLASSPATH"
fi
if [[ -n "${HADOOP_CONF_DIR:-}" ]]; then
  SPARK_CLASSPATH="$HADOOP_CONF_DIR:$SPARK_CLASSPATH"
fi
if [[ -n "${SPARK_CONF_DIR:-}" ]]; then
  SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH"
else
  SPARK_CLASSPATH="$SPARK_HOME/conf:$SPARK_CLASSPATH"
fi

case "${1:-}" in
  driver)
    shift
    # The submit host may reach the API server through a local port forward,
    # which is not reachable from the driver pod. Use the in-cluster endpoint
    # by default; override it with PUDDING_DRIVER_MASTER when necessary.
    driver_master="${PUDDING_DRIVER_MASTER:-k8s://https://kubernetes.default.svc}"
    master_args=()
    if [[ -n "$driver_master" ]]; then
      master_args=(--master "$driver_master")
    fi
    exec "$SPARK_HOME/bin/spark-submit" \
      --conf "spark.driver.bindAddress=${SPARK_DRIVER_BIND_ADDRESS:-}" \
      --deploy-mode client \
      "${master_args[@]}" \
      "$@"
    ;;
  executor)
    shift
    # Java options are supplied by the driver as SPARK_JAVA_OPT_<n> environment
    # variables. Keep them in memory so no option value is written to disk.
    mapfile -t SPARK_EXECUTOR_JAVA_OPTS < <(
      env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n \
        | sed 's/[^=]*=\(.*\)/\1/g' || true
    )
    exec "${JAVA_HOME}/bin/java" \
      -Xms"${SPARK_EXECUTOR_MEMORY}" \
      -Xmx"${SPARK_EXECUTOR_MEMORY}" \
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}" \
      -cp "$SPARK_CLASSPATH${SPARK_DIST_CLASSPATH:+:$SPARK_DIST_CLASSPATH}" \
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend \
      --driver-url "$SPARK_DRIVER_URL" \
      --executor-id "$SPARK_EXECUTOR_ID" \
      --cores "$SPARK_EXECUTOR_CORES" \
      --app-id "$SPARK_APPLICATION_ID" \
      --hostname "$SPARK_EXECUTOR_POD_IP" \
      --resourceProfileId "$SPARK_RESOURCE_PROFILE_ID" \
      --podName "$SPARK_EXECUTOR_POD_NAME"
    ;;
  *)
    exec "$@"
    ;;
esac
