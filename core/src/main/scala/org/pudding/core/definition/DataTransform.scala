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

package org.pudding.core.definition

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import org.pudding.core.configure.{ExecutionMode, TransformConf}
import org.pudding.core.{EasyHandle, PuddingException}

/**
 * Perform data processing operations such as aggregation, join, filtering, etc.,
 * supporting the input of multiple dataframes and the output of one dataframe, many to one.
 * with parameters passed in the form of a map.
 * The interface implementer needs to perform parameter validation themselves
 */
trait DataTransform extends EasyHandle with Logging {

  /**
   * Unique identifier used as the transform `type` in job configuration.
   */
  def identifier: String

  /**
   * Transforms one or more dependency DataFrames into one DataFrame. Implementations validate
   * their own configuration.
   */
  def handle(dataFrames: Map[String, DataFrame], config: Option[Map[String, Any]]): DataFrame

  /**
   * Transform with execution and incremental metadata. The default delegates to the basic
   * overload so existing transforms keep working.
   */
  def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]],
      context: TransformContext): DataFrame = {
    this.handle(dataFrames, config)
  }

  /**
   * Execution modes this transform supports. The safe default is batch.
   */
  def supportedExecutionModes: Set[ExecutionMode] = Set(ExecutionMode.Batch)

  /**
   * Whether streaming execution maintains operator state that is persisted in the checkpoint.
   * A stateful transform requires a sink with a stable checkpoint and participates in the
   * streaming state contract guard.
   */
  def streamingStateful: Boolean = false

  /**
   * Validate configuration before Spark resources are allocated. A stateful streaming transform
   * should require its watermark, window or state-retention settings here so a job fails before
   * a Spark application is submitted. The default implementation performs no validation.
   */
  def validateConfig(
      config: Option[Map[String, Any]],
      executionMode: ExecutionMode): Unit = ()

  /**
   * Whether this transform keeps incremental key, operation and ordering metadata intact. A
   * transform used in an explicit incremental pipeline must return true.
   */
  def preservesIncrementalMetadata: Boolean = false
}

object DataTransform {

  /**
   * identifiers of all available DataTransform implementations, used to validate a pipeline before
   * Spark is started.
   */
  def identifiers: Set[String] = this.loadServices().keySet

  /**
   * all available transform implementations, keyed by identifier.
   */
  def services(): Map[String, DataTransform] = this.loadServices()

  /**
   * factory method
   * @param transformCfs Seq[TransformDef]
   * @return map[id, (Transform, config map)]
   */
  def apply(transformCfs: Seq[TransformConf]): Map[String, (DataTransform, Option[Map[String, Any]])] = {
    if (transformCfs == null || transformCfs.isEmpty) {
      throw new PuddingException("transformCfs can not be empty!")
    }

    val identifierAndServices = this.loadServices()
    transformCfs.map(t => (t.id,
      identifierAndServices.getOrElse(t.`type`,
        throw new PuddingException(s"${t.`type`} can not get Transform implement!")) -> t.config))
      .toMap
  }

  private def loadServices(): Map[String, DataTransform] = {
    val serviceLoader = ServiceLoader.load(classOf[DataTransform])
    serviceLoader.iterator().asScala.map(s => s.identifier -> s).toMap
  }
}
