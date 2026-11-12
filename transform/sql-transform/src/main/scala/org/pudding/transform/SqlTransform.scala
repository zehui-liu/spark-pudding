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

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Execute SQL over one or more dependency dataframes.
 *
 * Every dependency is registered as a temporary view before the SQL is executed:
 *  - `tempViews` maps a dependency id to a view name and is the preferred way for multiple inputs;
 *  - `tempView` keeps the original single-input configuration;
 *  - if neither is configured, view names are derived from dependency ids
 *    (for example, dependency `source-1` is registered as `source_1`).
 */
class SqlTransform extends DataTransform {

  private val TEMP_VIEW_KEY = "tempView"

  private val TEMP_VIEWS_KEY = "tempViews"

  private val SQL_TEXT_KEY = "sql"

  private val NUM_PARTITIONS_KEY = "numPartitions"

  private val VIEW_NAME_PATTERN = "[A-Za-z_][A-Za-z0-9_]*".r

  override def identifier: String = "sql-transform"

  /**
   * SQL is accepted in streaming mode for stateless projections, filters and joins that Spark
   * can plan as streaming. Use the dedicated aggregate, dedup and join transforms for stateful
   * streaming so watermark settings and the state contract are validated by the engine.
   */
  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def handle(dataFrames: Map[String, DataFrame], config: Option[Map[String, Any]]): DataFrame = {
    val cfg = config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    if (dataFrames.isEmpty) {
      throw new PuddingException("dataFrames can not be empty!")
    }

    val sql = this.getMapValueThrow[String](cfg, SQL_TEXT_KEY)
    if (sql == null || sql.trim.isEmpty) {
      throw new PuddingException("sql can not be empty!")
    }

    val views = this.resolveViews(cfg, dataFrames)
    logInfo(s"register temp views: ${views.mkString(", ")}")

    views.foreach { case (id, viewName) =>
      dataFrames(id).createOrReplaceTempView(viewName)
    }

    val dataFrame = dataFrames.values.head.sparkSession.sql(sql)
    this.resolveNumPartitions(cfg) match {
      case Some(numPartitions) => dataFrame.repartition(numPartitions)
      case None => dataFrame
    }
  }

  /**
   * Resolve the temporary view name for every dependency dataframe.
   */
  private def resolveViews(cfg: Map[String, Any], dataFrames: Map[String, DataFrame]): Map[String, String] = {
    val views = cfg.get(TEMP_VIEWS_KEY) match {
      case Some(rawViews) =>
        if (cfg.contains(TEMP_VIEW_KEY)) {
          throw new PuddingException("tempView and tempViews can not be used together!")
        }

        val configuredViews = rawViews match {
          case viewsMap: Map[_, _] =>
            viewsMap.map {
              case (id, viewName) if id != null && viewName != null => id.toString -> viewName.toString
              case _ => throw new PuddingException("tempViews can not contain null keys or values!")
            }.toMap
          case _ =>
            throw new PuddingException("tempViews must be a map from dependency id to view name!")
        }

        if (configuredViews.isEmpty) {
          throw new PuddingException("tempViews can not be empty!")
        }

        val unknownIds = configuredViews.keySet.diff(dataFrames.keySet)
        if (unknownIds.nonEmpty) {
          throw new PuddingException(
            s"tempViews contains unknown dependencies: ${unknownIds.mkString(", ")}")
        }

        val missingIds = dataFrames.keySet.diff(configuredViews.keySet)
        if (missingIds.nonEmpty) {
          throw new PuddingException(
            s"tempViews must configure a view for every dependency, missing: ${missingIds.mkString(", ")}")
        }
        configuredViews

      case None =>
        cfg.get(TEMP_VIEW_KEY) match {
          case Some(tempView) =>
            if (tempView == null) {
              throw new PuddingException("tempView can not be null!")
            }
            if (dataFrames.size != 1) {
              throw new PuddingException(
                "tempView can only be used with one dependency, use tempViews for multiple dependencies!")
            }
            Map(dataFrames.keys.head -> tempView.toString)

          case None =>
            dataFrames.keys.map(id => id -> this.defaultViewName(id)).toMap
        }
    }

    views.foreach { case (_, viewName) => this.validateViewName(viewName) }

    val duplicateViewNames = views.values.toSeq.groupBy(identity)
      .collect { case (viewName, occurrences) if occurrences.size > 1 => viewName }
      .toSeq
      .sorted
    if (duplicateViewNames.nonEmpty) {
      throw new PuddingException(
        s"temp view names must be unique, duplicated: ${duplicateViewNames.mkString(", ")}")
    }

    views
  }

  /**
   * Build a valid view name from a dependency id, for example `source-1` becomes `source_1`.
   */
  private def defaultViewName(id: String): String = {
    if (id == null || id.trim.isEmpty) {
      throw new PuddingException("dependency id can not be empty!")
    }

    val normalized = id.replaceAll("[^A-Za-z0-9_]", "_")
    if (normalized.isEmpty || Character.isDigit(normalized.charAt(0))) {
      s"_$normalized"
    } else {
      normalized
    }
  }

  private def validateViewName(viewName: String): Unit = {
    if (!VIEW_NAME_PATTERN.pattern.matcher(viewName).matches()) {
      throw new PuddingException(
        s"invalid temp view name: $viewName, only letters, digits and underscore are allowed " +
          "and it must not start with a digit!")
    }
  }

  /**
   * numPartitions is optional. When configured, it is applied to the SQL result.
   */
  private def resolveNumPartitions(cfg: Map[String, Any]): Option[Int] = {
    cfg.get(NUM_PARTITIONS_KEY).map {
      case value: Int => value
      case value: Long => value.toInt
      case value: BigInt => value.toInt
      case value: java.math.BigInteger => value.intValue()
      case value: java.math.BigDecimal if value.stripTrailingZeros().scale() <= 0 => value.intValue()
      case value: Double if value == value.floor => value.toInt
      case value: String =>
        try {
          value.trim.toInt
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"numPartitions must be an integer, but got: $value")
        }
      case value =>
        throw new PuddingException(s"numPartitions must be an integer, but got: $value")
    }.map { numPartitions =>
      if (numPartitions <= 0) {
        throw new PuddingException(s"numPartitions must be greater than zero, but got: $numPartitions")
      }
      numPartitions
    }
  }
}
