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

import org.apache.spark.sql.{DataFrame, SaveMode}

import org.pudding.core.PuddingException
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{DataSink, SinkContext}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Writes a DataFrame to Elasticsearch through the official Elasticsearch Hadoop connector.
 *
 * The destination index is selected with `esIndex`; connection settings come from an
 * `elasticsearch` connection or the legacy `esNodes`/`esPort`/`esUser`/`esPass` keys. Batch and
 * structured streaming writes are supported.
 */
class ElasticsearchSink extends DataSink {

  override def identifier: String = "elasticsearch-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedStreamingOutputModes: Set[String] =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update)

  override def supportsStreamingUpsert: Boolean = true

  override def supportedConnectionTypes: Set[String] = Set("elasticsearch")

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val esIndex = ElasticsearchSink.requiredString(cfg, ElasticsearchSink.INDEX_KEY)
    val options = ElasticsearchSink.writerOptions(context, esIndex)

    if (ElasticsearchSink.streamingEnabled(cfg, context.executionMode)) {
      val checkpoint = ElasticsearchSink.optionalString(cfg, ElasticsearchSink.CHECKPOINT_KEY)
        .getOrElse {
          throw new PuddingException(
            s"${ElasticsearchSink.CHECKPOINT_KEY} is required for an Elasticsearch streaming write!")
        }
      val outputMode = ElasticsearchSink.outputMode(cfg)
      StreamingCheckpoint.validate(
        cfg, context.trigger, Some(checkpoint), context.stateContract)
      val writer = dataFrame.writeStream
        .format(ElasticsearchSink.FORMAT)
        .options(options)
        .option(ElasticsearchSink.CHECKPOINT_KEY, checkpoint)
        .outputMode(outputMode)
      val query = StreamingTrigger.applyTo(writer, context.trigger).start(esIndex)
      StreamingCheckpoint.awaitTermination(query, cfg, context.trigger, Some(checkpoint))
    } else {
      dataFrame.write
        .format(ElasticsearchSink.FORMAT)
        .options(options)
        .mode(ElasticsearchSink.saveMode(cfg))
        .save(esIndex)
    }
  }
}

private[sink] object ElasticsearchSink extends org.pudding.core.EasyHandle {

  private val FORMAT = "org.elasticsearch.spark.sql"

  private val INDEX_KEY = "esIndex"

  private val OPTIONS_KEY = "options"

  private val OPERATION_KEY = "operation"

  private val ID_FIELD_KEY = "idField"

  private val MODE_KEY = "mode"

  private val CHECKPOINT_KEY = "checkpointLocation"

  private val OUTPUT_MODE_KEY = "outputMode"

  private val ES_NODES_KEY = "esNodes"

  private val ES_PORT_KEY = "esPort"

  private val ES_USER_KEY = "esUser"

  private val ES_PASS_KEY = "esPass"

  private val STREAMING_KEYS = Seq("isStreaming", "streamWrite", "streamWriter")

  private val OPERATIONS = Set("index", "create", "update", "upsert", "delete")

  private val SAVE_MODES = Set("append", "overwrite", "ignore", "error", "errorifexists")

  private val OUTPUT_MODES = Set("append", "update")

  def writerOptions(context: SinkContext, esIndex: String): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val userOptions = getStringOptions(cfg, OPTIONS_KEY)

    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "elasticsearch") {
          throw new PuddingException(
            s"elasticsearch-sink requires an elasticsearch connection, but got: ${connection.`type`}")
        }
        connection.options
      case None =>
        legacyConnectionOptions(cfg)
    }

    // The destination index is fixed by esIndex; connection settings can not replace it.
    var options = userOptions ++ connectionOptions
    options += "es.resource" -> esIndex

    optionalString(cfg, OPERATION_KEY).foreach { value =>
      val operation = value.toLowerCase(Locale.ROOT)
      if (!OPERATIONS.contains(operation)) {
        throw new PuddingException(
          s"unsupported elasticsearch $OPERATION_KEY: $value, supported: " +
            OPERATIONS.toSeq.sorted.mkString(", "))
      }
      options += "es.write.operation" -> operation
    }
    optionalString(cfg, ID_FIELD_KEY).foreach(value => options += "es.mapping.id" -> value)
    options
  }

  def saveMode(cfg: Map[String, Any]): SaveMode = {
    val mode = cfg.get(MODE_KEY).filter(_ != null).map(_.toString.trim.toLowerCase(Locale.ROOT))
      .getOrElse("append")
    if (!SAVE_MODES.contains(mode)) {
      throw new PuddingException(s"unsupported elasticsearch save mode: $mode")
    }
    mode match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "ignore" => SaveMode.Ignore
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
      .getOrElse(StreamingOutputMode.Append)
    if (!OUTPUT_MODES.contains(mode)) {
      throw new PuddingException(
        s"elasticsearch streaming supports output modes append and update, but got: $mode")
    }
    if (mode == StreamingOutputMode.Update) {
      if (optionalString(cfg, ID_FIELD_KEY).isEmpty) {
        throw new PuddingException(
          "elasticsearch streaming outputMode=update requires idField")
      }
      optionalString(cfg, OPERATION_KEY)
        .map(_.toLowerCase(Locale.ROOT))
        .foreach { operation =>
          if (!Set("index", "upsert", "update").contains(operation)) {
            throw new PuddingException(
              "elasticsearch streaming outputMode=update requires operation=index, " +
                s"upsert or update, but got: $operation")
          }
        }
    }
    mode
  }

  private def legacyConnectionOptions(cfg: Map[String, Any]): Map[String, String] = {
    val esNodes = requiredString(cfg, ES_NODES_KEY)
    val esPort = cfg.get(ES_PORT_KEY).filter(_ != null)
      .map(_.toString.trim).filter(_.nonEmpty).getOrElse("9200")
    var options = Map("es.nodes" -> esNodes, "es.port" -> esPort)
    optionalString(cfg, ES_USER_KEY)
      .foreach(user => options += "es.net.http.auth.user" -> user)
    optionalString(cfg, ES_PASS_KEY)
      .foreach(password => options += "es.net.http.auth.pass" -> password)
    options
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
