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

package org.pudding.core.spark.mock

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.definition.{DataTransform, TransformContext}

/**
 * Test transform that requires incremental metadata from the runtime context.
 */
class MockIncrementalAwareTransform extends DataTransform {

  override def identifier: String = "mock_incremental_aware"

  override def preservesIncrementalMetadata: Boolean = true

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    throw new PuddingException("this transform requires the transform context")
  }

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]],
      context: TransformContext): DataFrame = {
    if (context.incremental.isEmpty) {
      throw new PuddingException("incremental metadata was not propagated")
    }
    dataFrames.head._2
  }
}
