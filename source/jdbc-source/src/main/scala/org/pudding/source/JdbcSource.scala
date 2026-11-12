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

/**
 * Reads a table through Spark's JDBC data source. Batch mode supports any table; sync incremental
 * reads require a top-level `cursorField` and support `append` or `upsert` semantics. PostgreSQL
 * and MySQL drivers are bundled; other databases need an explicit `driver` class on the Spark
 * classpath. Credentials are resolved by the server and are never logged by this connector.
 */
class JdbcSource extends DataSource with IncrementalSource {

  override def identifier: String = "jdbc-source"

  override def supportedConnectionTypes: Set[String] = Set("jdbc", "mysql")

  override def supportsIncrementalSync: Boolean = true

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert)

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    sparkSession.read.format("jdbc").options(JdbcSource.readerOptions(context)).load()
  }

  override def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead = {
    val cfg = context.config.getOrElse(
      throw new PuddingException("config can not be null!"))
    JdbcIncremental.read(sparkSession, JdbcSource.readerOptions(context), cfg, request)
  }
}

private[source] object JdbcSource {

  def readerOptions(context: SourceContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "jdbc" && connection.`type` != "mysql") {
          throw new PuddingException(
            s"jdbc-source requires a jdbc connection, but got: ${connection.`type`}")
        }
        connection.options
      case None => Map.empty[String, String]
    }
    JdbcConnectorConfig.connectionOptions(cfg, connectionOptions)
  }
}
