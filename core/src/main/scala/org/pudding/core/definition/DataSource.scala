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

package org.pudding.core.definition

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.configure.{ExecutionMode, SourceConf}
import org.pudding.core.{EasyHandle, PuddingException}

/**
 * Support creating dataframes from different data sources and configuring them to be passed in the form of maps.
 * Interface implementers need to perform parameter validation.
 */
trait DataSource extends EasyHandle with Logging with Serializable {

  /**
   * Unique identifier used as the source `type` in job configuration.
   */
  def identifier: String

  /**
   * Reads the source and returns its DataFrame. Implementations validate their own configuration.
   */
  def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame

  /**
   * Reads the source with resolved connection and catalog references. The default implementation
   * delegates to the config-only overload so existing connectors keep working.
   */
  def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    this.initData(sparkSession, context.config)
  }

  /**
   * Execution modes this source supports. Batch is the safe default for existing connectors.
   */
  def supportedExecutionModes: Set[ExecutionMode] = Set(ExecutionMode.Batch)

  /**
   * Whether this source participates in the constrained sync incremental mode.
   *
   * The older name referred to the initial lakehouse-only implementation. New sources should
   * override this method; the default keeps existing lakehouse implementations compatible.
   */
  def supportsIncrementalSync: Boolean = supportsLakeIncremental

  /**
   * Legacy capability name retained for source compatibility.
   */
  @deprecated("override supportsIncrementalSync", "1.0.0")
  def supportsLakeIncremental: Boolean = false

  /**
   * Whether this source can consume a top-level connection definition.
   */
  def supportsConnection: Boolean = this.supportedConnectionTypes.nonEmpty

  /**
   * Whether this source can consume a top-level catalog definition.
   */
  def supportsCatalog: Boolean = this.supportedCatalogTypes.nonEmpty

  /**
   * Connection types accepted by this source. An external source that accepts any connection type
   * must override `supportsConnection` as well; the default `supportsConnection` is false when
   * this set is empty.
   */
  def supportedConnectionTypes: Set[String] = Set.empty

  /**
   * Catalog types accepted by this source.
   */
  def supportedCatalogTypes: Set[String] = Set.empty
}

object DataSource {

  /**
   * identifiers of all available DataSource implementations, used to validate a pipeline before
   * Spark is started.
   */
  def identifiers: Set[String] = this.loadServices().keySet

  /**
   * all available source implementations, keyed by identifier.
   */
  def services(): Map[String, DataSource] = this.loadServices()

  /**
   * capabilities of every source implementation, used to reject unsupported references before
   * a Spark session is created.
   */
  def capabilities(): Map[String, ComponentCapabilities] = {
    this.loadServices().map { case (identifier, source) =>
      identifier -> ComponentCapabilities(
        source.supportsConnection,
        source.supportsCatalog,
        source.supportedConnectionTypes,
        source.supportedCatalogTypes)
    }
  }

  /**
   * factory method
   *
   * @param sourceCfs Seq[SourceDef]
   * @return map[id, (DataSource, config map)]
   */
  def apply(sourceCfs: Seq[SourceConf]): Map[String, (DataSource, Option[Map[String, Any]])] = {
    if (sourceCfs == null || sourceCfs.isEmpty) {
      throw new PuddingException("sourceCfs can not be empty!")
    }

    val identifierAndServices = this.loadServices()
    sourceCfs.map(s => (s.id, identifierAndServices.getOrElse(s.`type`,
      throw new PuddingException(s"${s.`type`} can not get DataSource implement!")) -> s.config))
      .toMap
  }

  private def loadServices(): Map[String, DataSource] = {
    val serviceLoader = ServiceLoader.load(classOf[DataSource])
    serviceLoader.iterator().asScala.map(s => s.identifier -> s).toMap
  }
}
