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

package org.pudding.transform

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Merges two or more dependencies with union. `byName` is the default and aligns columns by
 * name; `byPosition` requires `inputs` because the dependency map has no stable order.
 *
 * {{{
 * config:
 *   inputs: [source-1, source-2]
 *   mode: byName
 *   allowMissingColumns: true
 *   distinct: true
 * }}}
 */
class UnionTransform extends DataTransform {

  private val InputsKey = "inputs"

  private val ModeKey = "mode"

  private val AllowMissingColumnsKey = "allowMissingColumns"

  private val DistinctKey = "distinct"

  override def identifier: String = "union-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def preservesIncrementalMetadata: Boolean = true

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    TransformSupport.requireAtLeast(dataFrames, 2, identifier)

    val byName = this.resolveMode(TransformSupport.optionalString(cfg, ModeKey))
    val allowMissingColumns = TransformSupport.optionalBoolean(cfg, AllowMissingColumnsKey)
      .getOrElse(false)
    if (!byName && allowMissingColumns) {
      throw new PuddingException(s"$AllowMissingColumnsKey is only supported in byName mode!")
    }
    val distinct = TransformSupport.optionalBoolean(cfg, DistinctKey).getOrElse(false)

    val inputs = TransformSupport.stringSeq(cfg, InputsKey)
    val orderedIds = inputs.getOrElse {
      if (byName) {
        dataFrames.keys.toSeq.sorted
      } else {
        throw new PuddingException(
          s"$identifier in byPosition mode requires $InputsKey to define the union order!")
      }
    }
    this.validateInputs(orderedIds, dataFrames)

    val frames = orderedIds.map(id => dataFrames(id))
    val unioned = frames.tail.foldLeft(frames.head) { (left, right) =>
      if (byName) {
        left.unionByName(right, allowMissingColumns)
      } else {
        left.union(right)
      }
    }
    val result = if (distinct) unioned.distinct() else unioned
    TransformSupport.applyNumPartitions(result, cfg)
  }

  private def resolveMode(value: Option[String]): Boolean = {
    TransformSupport.normalize(value.getOrElse("byName")) match {
      case "byname" | "name" => true
      case "byposition" | "position" => false
      case other =>
        throw new PuddingException(s"$ModeKey must be byName or byPosition, but got: $other")
    }
  }

  private def validateInputs(
      inputs: Seq[String],
      dataFrames: Map[String, DataFrame]): Unit = {
    val unknown = inputs.toSet.diff(dataFrames.keySet)
    if (unknown.nonEmpty) {
      throw new PuddingException(
        s"$InputsKey contains unknown dependencies: ${unknown.toSeq.sorted.mkString(", ")}")
    }
    val missing = dataFrames.keySet.diff(inputs.toSet)
    if (missing.nonEmpty) {
      throw new PuddingException(
        s"$InputsKey must include every dependency, missing: ${missing.toSeq.sorted.mkString(", ")}")
    }
  }
}
