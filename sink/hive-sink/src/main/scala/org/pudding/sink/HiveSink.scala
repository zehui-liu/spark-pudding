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

package org.pudding.sink

import java.util.Locale

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.definition.{DataSink, SinkContext}

/**
 * Writes a DataFrame to a Hive table using Spark's save modes and optional partitioning.
 */
class HiveSink extends DataSink {

  private val TABLE_KEY = "table"

  private val MODE_KEY = "mode"

  private val PARTITION_BY_KEY = "partitionBy"

  private val DEFAULT_MODE = "append"

  override def identifier: String = "hive-sink"

  override def supportsFullRefresh: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("hive")

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "hive") {
        throw new PuddingException(
          s"hive-sink requires a hive catalog, but got: ${catalog.`type`}")
      }
    }

    val cfg = context.config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val table = this.getMapValueThrow[String](cfg, TABLE_KEY)
    if (table.trim.isEmpty) {
      throw new PuddingException("table can not be empty!")
    }

    val mode = cfg.get(MODE_KEY) match {
      case Some(mode: String) => this.parseMode(mode)
      case Some(mode) => throw new PuddingException(s"mode must be string, but got: $mode")
      case None => DEFAULT_MODE
    }

    // Accept either a sequence or a comma-separated string for partition columns.
    val partitionBy = cfg.get(PARTITION_BY_KEY) match {
      case Some(columns: Seq[_]) => columns.map(_.toString)
      case Some(columns: String) => columns.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      case Some(_) => throw new PuddingException("partitionBy must be a array or comma-separated string!")
      case None => Seq.empty[String]
    }

    val dataFrameWriter = dataFrame.write.mode(mode)
    if (partitionBy.nonEmpty) {
      dataFrameWriter.partitionBy(partitionBy: _*).saveAsTable(table)
    } else {
      dataFrameWriter.saveAsTable(table)
    }
  }

  private def parseMode(mode: String): String = mode.toLowerCase(Locale.ROOT) match {
    case "append" | "overwrite" | "ignore" | "error" | "errorifexists" => mode
    case _ => throw new PuddingException(s"unsupported save mode: $mode")
  }
}
