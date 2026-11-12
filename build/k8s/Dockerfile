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

# The Spark distribution is staged into spark/ and the unpacked spark-pudding
# distribution into pudding/ by bin/pudding k8s build-image. A Spark distribution
# must be baked in because both the driver and executors run in containers.
ARG BASE_IMAGE=eclipse-temurin:11-jdk-jammy
FROM ${BASE_IMAGE}

ARG PUDDING_VERSION=dev

LABEL org.opencontainers.image.title="spark-pudding" \
      org.opencontainers.image.description="Apache Spark runtime with the spark-pudding distribution" \
      org.opencontainers.image.version="${PUDDING_VERSION}"

ENV SPARK_HOME=/opt/spark \
    SPARK_USER=spark \
    SPARK_LOCAL_DIRS=/tmp/spark-local \
    PATH=/opt/spark/bin:${PATH}

RUN if ! id -u 185 >/dev/null 2>&1; then \
      useradd --uid 185 --gid 0 --create-home --home-dir /home/spark \
        --shell /bin/bash spark; \
    fi \
 && mkdir -p /opt/spark /opt/spark-pudding /opt/spark/work-dir /tmp/spark-local \
      /home/spark

COPY spark/ /opt/spark/
COPY pudding/ /opt/spark-pudding/
COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod 0755 /opt/entrypoint.sh \
 && chown -R 185:0 /opt/spark /opt/spark-pudding /opt/spark/work-dir \
      /tmp/spark-local /home/spark

USER 185
WORKDIR /opt/spark/work-dir
ENTRYPOINT ["/opt/entrypoint.sh"]
