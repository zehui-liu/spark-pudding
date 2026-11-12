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

import org.pudding.core.definition.DataTransform

/**
 * Limits the number of rows returned by one dependency. Spark applies the limit to the whole
 * result, not to every partition.
 *
 * {{{
 * config:
 *   limit: 100
 *   numPartitions: 1
 * }}}
 */
class LimitTransform extends DataTransform {

  private val LimitKey = "limit"

  override def identifier: String = "limit-transform"

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val limit = TransformSupport.requiredInt(cfg, LimitKey, 0)
    val limited = TransformSupport.single(dataFrames, identifier).limit(limit)
    TransformSupport.applyNumPartitions(limited, cfg)
  }
}
