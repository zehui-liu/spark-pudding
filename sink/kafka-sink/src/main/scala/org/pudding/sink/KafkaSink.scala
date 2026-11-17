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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSink, SinkContext}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Writes a DataFrame to Kafka through Spark's Kafka connector.
 *
 * Spark expects `value` and optional `key`, `topic`, `partition` and `headers` columns. A fixed
 * topic can be configured instead of a topic column; `valueColumn` and `keyColumn` select and
 * rename existing columns. Batch and structured streaming writes are supported.
 */
class KafkaSink extends DataSink {

  override def identifier: String = "kafka-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedConnectionTypes: Set[String] = Set("kafka")

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val prepared = KafkaSink.prepare(dataFrame, cfg)
    val options = KafkaSink.writerOptions(context)

    if (KafkaSink.streamingEnabled(cfg, context.executionMode)) {
      val checkpoint = KafkaSink.optionalString(cfg, KafkaSink.CHECKPOINT_KEY)
        .getOrElse {
          throw new PuddingException(
            s"${KafkaSink.CHECKPOINT_KEY} is required for a Kafka streaming write!")
        }
      StreamingCheckpoint.validate(
        cfg, context.trigger, Some(checkpoint), context.stateContract)
      val writer = prepared.writeStream
        .format("kafka")
        .options(options)
        .option(KafkaSink.CHECKPOINT_KEY, checkpoint)
        .outputMode(KafkaSink.outputMode(cfg))
      val query = StreamingTrigger.applyTo(writer, context.trigger).start()
      StreamingCheckpoint.awaitTermination(query, cfg, context.trigger, Some(checkpoint))
    } else {
      prepared.write
        .format("kafka")
        .options(options)
        .mode(KafkaSink.saveMode(cfg))
        .save()
    }
  }
}

private[sink] object KafkaSink extends org.pudding.core.EasyHandle {

  private val TOPIC_KEY = "topic"

  private val VALUE_COLUMN_KEY = "valueColumn"

  private val KEY_COLUMN_KEY = "keyColumn"

  private val OPTIONS_KEY = "options"

  private val MODE_KEY = "mode"

  private val CHECKPOINT_KEY = "checkpointLocation"

  private val OUTPUT_MODE_KEY = "outputMode"

  private val KAFKA_BOOTSTRAP_SERVERS_KEY = "kafkaBootstrapServers"

  private val BOOTSTRAP_OPTION = "kafka.bootstrap.servers"

  private val STREAMING_KEYS = Seq("isStreaming", "streamWrite", "streamWriter")

  private val SAVE_MODES = Set("append", "error", "errorifexists")

  private val OUTPUT_MODES = Set("append")

  def writerOptions(context: SinkContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val userOptions = getStringOptions(cfg, OPTIONS_KEY)
    if (userOptions.contains(TOPIC_KEY)) {
      throw new PuddingException(
        s"kafka-sink $TOPIC_KEY must be configured as config.$TOPIC_KEY, not under $OPTIONS_KEY")
    }

    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "kafka") {
          throw new PuddingException(
            s"kafka-sink requires a kafka connection, but got: ${connection.`type`}")
        }
        connection.options
      case None =>
        Map(BOOTSTRAP_OPTION -> requiredString(cfg, KAFKA_BOOTSTRAP_SERVERS_KEY))
    }

    var options = userOptions ++ connectionOptions
    if (!options.contains(BOOTSTRAP_OPTION)) {
      options.get("bootstrap.servers")
        .foreach(value => options += BOOTSTRAP_OPTION -> value)
    }
    if (!options.contains(BOOTSTRAP_OPTION)) {
      throw new PuddingException(
        s"kafka-sink requires a kafka connection or config.$KAFKA_BOOTSTRAP_SERVERS_KEY")
    }
    optionalString(cfg, TOPIC_KEY).foreach(topic => options += "topic" -> topic)
    options
  }

  def prepare(dataFrame: DataFrame, cfg: Map[String, Any]): DataFrame = {
    val schema = dataFrame.schema
    val configuredTopic = optionalString(cfg, TOPIC_KEY)
    val valueColumn = optionalString(cfg, VALUE_COLUMN_KEY).getOrElse("value")
    if (!schema.fieldNames.contains(valueColumn)) {
      throw new PuddingException(
        s"kafka-sink requires a $valueColumn column for the Kafka value")
    }
    requireStringOrBinary(schema(valueColumn).dataType, valueColumn)

    val keyColumn = optionalString(cfg, KEY_COLUMN_KEY)
      .orElse(if (schema.fieldNames.contains("key")) Some("key") else None)
    keyColumn.foreach { name =>
      if (!schema.fieldNames.contains(name)) {
        throw new PuddingException(s"kafka-sink key column does not exist: $name")
      }
      requireStringOrBinary(schema(name).dataType, name)
    }

    val topicColumn = if (configuredTopic.isEmpty) {
      if (!schema.fieldNames.contains("topic")) {
        throw new PuddingException(
          "kafka-sink requires config.topic or a topic column")
      }
      if (schema("topic").dataType != StringType) {
        throw new PuddingException("kafka-sink topic column must be string")
      }
      Some("topic")
    } else {
      None
    }

    if (schema.fieldNames.contains("partition") && schema("partition").dataType != IntegerType) {
      throw new PuddingException("kafka-sink partition column must be integer")
    }

    val columns = Seq.newBuilder[Column]
    keyColumn.foreach(name => columns += col(name).as("key"))
    columns += col(valueColumn).as("value")
    topicColumn.foreach(name => columns += col(name).as("topic"))
    if (schema.fieldNames.contains("partition")) {
      columns += col("partition")
    }
    if (schema.fieldNames.contains("headers")) {
      columns += col("headers")
    }
    dataFrame.select(columns.result(): _*)
  }

  def saveMode(cfg: Map[String, Any]): SaveMode = {
    val mode = cfg.get(MODE_KEY).filter(_ != null).map(_.toString.trim.toLowerCase(Locale.ROOT))
      .getOrElse("append")
    if (!SAVE_MODES.contains(mode)) {
      throw new PuddingException(
        s"unsupported kafka save mode: $mode; Kafka supports append, error and errorifexists")
    }
    mode match {
      case "append" => SaveMode.Append
      case "error" | "errorifexists" => SaveMode.ErrorIfExists
    }
  }

  def streamingEnabled(cfg: Map[String, Any]): Boolean = {
    this.streamingEnabled(cfg, None)
  }

  def streamingEnabled(
      cfg: Map[String, Any],
      executionMode: Option[ExecutionMode]): Boolean = {
    val configured = STREAMING_KEYS.iterator
      .flatMap(key => booleanOption(cfg, key))
      .find(_ == true)
      .getOrElse(false)
    this.effectiveStreaming(configured, executionMode)
  }

  def outputMode(cfg: Map[String, Any]): String = {
    val mode = optionalString(cfg, OUTPUT_MODE_KEY)
      .map(_.toLowerCase(Locale.ROOT))
      .getOrElse("append")
    if (!OUTPUT_MODES.contains(mode)) {
      throw new PuddingException(
        s"kafka streaming supports output mode append, but got: $mode")
    }
    mode
  }

  private def requireStringOrBinary(dataType: DataType, column: String): Unit = {
    if (dataType != StringType && dataType != BinaryType) {
      throw new PuddingException(
        s"kafka-sink column $column must be string or binary, but got: $dataType")
    }
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => throw new PuddingException(s"can not find param: $key")
    }
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def booleanOption(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ => throw new PuddingException(s"$key must be true or false, but got: $value")
        }
      case value => throw new PuddingException(s"$key must be a boolean, but got: $value")
    }
  }
}
