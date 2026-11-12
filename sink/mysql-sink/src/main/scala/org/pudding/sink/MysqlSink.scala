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

package org.pudding.sink

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{
  DataSink,
  IncrementalSemantics,
  IncrementalWrite,
  SinkContext
}
import org.pudding.connector.jdbc.{JdbcAtomicReplace, JdbcConnectorConfig, JdbcStreaming}
import org.pudding.connector.jdbc.JdbcConnectorConfig.DriverKey

/**
 * Writes a DataFrame to MySQL through Spark's JDBC data sink.
 *
 * Streaming `append` writes each microbatch through Spark JDBC. Streaming `update` merges full
 * rows by `recordKeyFields` with a keyed `UPDATE ... JOIN` and `INSERT ... WHERE NOT EXISTS`.
 */
class MysqlSink extends DataSink {

  override def identifier: String = "mysql-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedStreamingOutputModes: Set[String] =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update)

  override def supportsStreamingUpsert: Boolean = true

  override def supportsFullRefresh: Boolean = true

  override def supportsAtomicReplace: Boolean = true

  override def supportsIncrementalSync: Boolean = true

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert, IncrementalSemantics.Cdc)

  override def isIdempotentIncremental(semantics: IncrementalSemantics): Boolean =
    semantics != IncrementalSemantics.Append

  override def supportedConnectionTypes: Set[String] = Set("jdbc", "mysql")

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val options = MysqlSink.writerOptions(context)
    if (JdbcStreaming.streamingEnabled(cfg, context.executionMode)) {
      JdbcStreaming.write(dataFrame, context, options, this.identifier)
    } else {
      dataFrame.write.format("jdbc")
        .options(options)
        .mode(JdbcConnectorConfig.saveMode(cfg))
        .save()
    }
  }

  override def writeFullRefresh(dataFrame: DataFrame, context: SinkContext): Unit = {
    JdbcAtomicReplace.write(dataFrame, MysqlSink.writerOptions(context))
  }

  override def writeIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: IncrementalWrite): Unit = {
    val cfg = context.config.getOrElse(
      throw new PuddingException("config can not be null!"))
    JdbcStreaming.writeIncremental(
      dataFrame, cfg, MysqlSink.writerOptions(context), incremental)
  }
}

private[sink] object MysqlSink {

  private val DefaultDriver = "com.mysql.cj.jdbc.Driver"

  def writerOptions(context: SinkContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val nativeOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "jdbc" && connection.`type` != "mysql") {
          throw new PuddingException(
            s"mysql-sink requires a jdbc or mysql connection, but got: ${connection.`type`}")
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
