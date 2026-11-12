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

package org.pudding.transform

import org.apache.spark.sql.DataFrame

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Filters one dependency with a Spark SQL condition.
 *
 * {{{
 * config:
 *   condition: "amount >= 100 and city = 'beijing'"
 *   numPartitions: 10
 * }}}
 */
class FilterTransform extends DataTransform {

  private val ConditionKey = "condition"

  override def identifier: String = "filter-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def preservesIncrementalMetadata: Boolean = true

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val condition = TransformSupport.requiredString(cfg, ConditionKey)
    val filtered = TransformSupport.single(dataFrames, identifier).filter(condition)
    TransformSupport.applyNumPartitions(filtered, cfg)
  }
}
