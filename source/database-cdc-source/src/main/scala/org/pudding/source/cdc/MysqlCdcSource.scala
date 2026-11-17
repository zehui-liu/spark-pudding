/*
 * Copyright 2024-2026 the spark-pudding authors
 *
 * Licensed under the Apache License, Version 2.0
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
 * Reads MySQL binlog changes natively through Debezium on the Spark driver.
 *
 * The source is streaming-only and requires a single table with an explicit `schema`. The
 * resulting rows contain the declared columns plus `__op` (`I`, `U`, `D`), `_cdc_timestamp` and
 * `_cdc_table`. Checkpoint offsets contain only binlog positions.
 */
class MysqlCdcSource extends DataSource {

  override def identifier: String = "mysql-cdc-source"

  override def supportedConnectionTypes: Set[String] = Set("mysql", "jdbc")

  override def supportedExecutionModes: Set[ExecutionMode] = Set(ExecutionMode.Streaming)

  override def initData(
      sparkSession: SparkSession,
      config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    CdcSourceSupport.read(sparkSession, context, "mysql")
  }
}
