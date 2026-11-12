/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pudding.source.cdc

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSource, SourceContext}

/**
 * Reads PostgreSQL logical replication changes natively through Debezium's `pgoutput` plugin.
 *
 * The database must use `wal_level=logical`; the connector creates a filtered publication and
 * a stable replication slot unless those names are supplied in `options`.
 */
class PostgresCdcSource extends DataSource {

  override def identifier: String = "postgres-cdc-source"

  override def supportedConnectionTypes: Set[String] = Set("jdbc")

  override def supportedExecutionModes: Set[ExecutionMode] = Set(ExecutionMode.Streaming)

  override def initData(
      sparkSession: SparkSession,
      config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    CdcSourceSupport.read(sparkSession, context, "postgres")
  }
}
