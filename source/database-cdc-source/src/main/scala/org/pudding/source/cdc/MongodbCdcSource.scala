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
 * Reads MongoDB change streams natively through the MongoDB Java driver.
 *
 * The deployment must be a replica set or sharded cluster. The initial snapshot is ordered by
 * `_id`, and the source schema must contain `_id` because MongoDB delete events only carry the
 * document key. Successful checkpoint restarts resume from the saved resume token; a checkpoint
 * taken during the snapshot restarts after the last emitted `_id`.
 */
class MongodbCdcSource extends DataSource {

  override def identifier: String = "mongodb-cdc-source"

  override def supportedConnectionTypes: Set[String] = Set("mongodb")

  override def supportedExecutionModes: Set[ExecutionMode] = Set(ExecutionMode.Streaming)

  override def initData(
      sparkSession: SparkSession,
      config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    CdcSourceSupport.read(sparkSession, context, "mongodb")
  }
}
