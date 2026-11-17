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

package org.pudding.core.spark.mock

import org.apache.spark.sql.SparkSession

import org.pudding.core.definition.{
  DataSource,
  IncrementalRead,
  IncrementalRequest,
  IncrementalSemantics,
  IncrementalSource,
  SourceContext
}

/**
 * Incremental source used by validator and execution planning tests.
 */
class MockIncrementalSource extends DataSource with IncrementalSource {

  override def identifier: String = "mock_incremental_source"

  override def supportsLakeIncremental: Boolean = true

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    IncrementalSemantics.values

  override def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead = {
    import sparkSession.implicits._
    IncrementalRead(
      dataFrame = Seq((1, "incremental")).toDF("id", "name"),
      semantics = request.semantics,
      keyFields = request.keyFields,
      opColumn = request.opColumn,
      fromCursor = request.start,
      toCursor = "10",
      cursorType = "test")
  }

  override def initData(
      sparkSession: SparkSession,
      config: Option[Map[String, Any]]): org.apache.spark.sql.DataFrame = {
    import sparkSession.implicits._
    Seq((1, "incremental")).toDF("id", "name")
  }
}
