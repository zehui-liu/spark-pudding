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

package org.pudding.source

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.PuddingException
import org.pudding.core.definition.{
  DataSource,
  IncrementalRead,
  IncrementalRequest,
  IncrementalSemantics,
  IncrementalSource,
  SourceContext
}
import org.pudding.connector.jdbc.{JdbcConnectorConfig, JdbcIncremental}
import org.pudding.connector.jdbc.JdbcConnectorConfig.DriverKey

/**
 * Reads a MySQL table through Spark's JDBC data source. Batch mode supports any table; sync
 * incremental reads require a top-level `cursorField` and support `append` or `upsert` semantics.
 */
class MysqlSource extends DataSource with IncrementalSource {

  override def identifier: String = "mysql-source"

  override def supportedConnectionTypes: Set[String] = Set("jdbc", "mysql")

  override def supportsIncrementalSync: Boolean = true

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert)

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val options = MysqlSource.readerOptions(context)
    sparkSession.read.format("jdbc").options(options).load()
  }

  override def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead = {
    val cfg = context.config.getOrElse(
      throw new PuddingException("config can not be null!"))
    JdbcIncremental.read(sparkSession, MysqlSource.readerOptions(context), cfg, request)
  }
}

private[source] object MysqlSource {

  private val DefaultDriver = "com.mysql.cj.jdbc.Driver"

  def readerOptions(context: SourceContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val nativeOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "jdbc" && connection.`type` != "mysql") {
          throw new PuddingException(
            s"mysql-source requires a jdbc or mysql connection, but got: ${connection.`type`}")
        }
        connection.options
      case None => Map.empty[String, String]
    }
    val effectiveConfig = if (!nativeOptions.contains(DriverKey) && !cfg.contains(DriverKey)) {
      cfg + (DriverKey -> DefaultDriver)
    } else {
      cfg
    }
    JdbcConnectorConfig.connectionOptions(effectiveConfig, nativeOptions)
  }
}
