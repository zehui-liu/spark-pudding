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

import java.util.Locale

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{DataSink, SinkContext}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Displays a DataFrame on the console for batch or streaming jobs. Streaming supports append,
 * update and complete output; stateful streaming is rejected because the console has no
 * checkpoint to persist state.
 */
class ConsoleSink extends DataSink {

  private val IS_STREAMING_KEY = "isStreaming"

  /** Legacy alias for `isStreaming`; kept for existing job configurations. */
  private val IS_STREAM_KEY = "isStream"

  /** Maximum number of rows printed per batch; defaults to 20. */
  private val NUM_ROWS_KEY = "numRows"

  /** Maximum width of a printed value; defaults to 20. */
  private val TRUNCATE_KEY = "truncate"

  /** Prints each row vertically when true; defaults to false. */
  private val VERTICAL_KEY = "vertical"

  /** Prints the DataFrame schema before the rows when true; defaults to false. */
  private val PRINT_SCHEMA_KEY = "printSchema"

  override def identifier: String = "console-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportsStatefulStreaming: Boolean = false

  override def supportedStreamingOutputModes: Set[String] =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update, StreamingOutputMode.Complete)

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    context.config match {
      case Some(cfg) =>
        if (streamingEnabled(cfg, context.executionMode)) {
          // Console streaming has no checkpoint to retain, so delete-on-success is rejected.
          StreamingCheckpoint.validate(cfg, context.trigger, None)
          val outputMode = StreamingOutputMode.resolve(
            Some(cfg), identifier, this.supportedStreamingOutputModes)
          if (booleanValue(cfg, PRINT_SCHEMA_KEY).getOrElse(false)) {
            dataFrame.printSchema()
          }
          val writer = dataFrame.writeStream
            .format("console")
            .outputMode(outputMode)
            .option("numRows", intValue(cfg, NUM_ROWS_KEY, 20))
            .option("truncate", intValue(cfg, TRUNCATE_KEY, 20))
            .option("vertical", booleanValue(cfg, VERTICAL_KEY).getOrElse(false))
          val triggeredWriter = StreamingTrigger.applyTo(writer, context.trigger)
          val streamingQuery = triggeredWriter.start()
          StreamingCheckpoint.awaitTermination(
            streamingQuery, cfg, context.trigger, None)
        } else {
          if (booleanValue(cfg, PRINT_SCHEMA_KEY).getOrElse(false)) {
            dataFrame.printSchema()
          }

          dataFrame.show(
            numRows = intValue(cfg, NUM_ROWS_KEY, 20),
            truncate = intValue(cfg, TRUNCATE_KEY, 20),
            vertical = booleanValue(cfg, VERTICAL_KEY).getOrElse(false)
          )
        }
      case None =>
        dataFrame.show()
    }
  }

  private[sink] def streamingEnabled(cfg: Map[String, Any]): Boolean = {
    this.streamingEnabled(cfg, None)
  }

  private def streamingEnabled(
      cfg: Map[String, Any],
      executionMode: Option[ExecutionMode]): Boolean = {
    val configured = booleanValue(cfg, IS_STREAMING_KEY)
      .orElse(booleanValue(cfg, IS_STREAM_KEY))
      .getOrElse(false)
    this.effectiveStreaming(configured, executionMode)
  }

  /**
   * Configuration values can be typed (YAML/JSON) or strings (HOCON), so accept both.
   */
  private def booleanValue(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case other => throw new PuddingException(s"$key must be true or false, got '$other'")
        }
      case value =>
        throw new PuddingException(s"$key must be a boolean, got ${value.getClass.getSimpleName}")
    }
  }

  private def intValue(cfg: Map[String, Any], key: String, default: Int): Int = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: Number) => value.intValue()
      case Some(value: String) =>
        try {
          value.trim.toInt
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$key must be a number, got '$value'")
        }
      case Some(value) =>
        throw new PuddingException(s"$key must be a number, got ${value.getClass.getSimpleName}")
      case None => default
    }
  }
}
