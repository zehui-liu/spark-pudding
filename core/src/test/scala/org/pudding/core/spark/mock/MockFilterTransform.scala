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

import org.apache.spark.sql.DataFrame
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

class MockFilterTransform extends DataTransform {

  override def identifier: String = "mock_filter"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def preservesIncrementalMetadata: Boolean = true

  override def handle(dataFrames: Map[String, DataFrame], config: Option[Map[String, Any]]): DataFrame = {
    assert(dataFrames.size == 1)
    val df = dataFrames.head._2
    df.filter("age > 30").toDF()
  }
}
