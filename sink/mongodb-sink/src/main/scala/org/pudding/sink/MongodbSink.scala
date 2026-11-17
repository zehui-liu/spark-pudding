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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SaveMode}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSink, SinkContext}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Writes a DataFrame to a MongoDB collection through the official MongoDB Spark connector.
 *
 * The connection URI comes from a `mongodb` connection (or the legacy `uri`/`connectionUri`
 * config), while database and collection identify the target resource. Batch and structured
 * streaming writes are supported.
 */
class MongodbSink extends DataSink {

  override def identifier: String = "mongodb-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedConnectionTypes: Set[String] = Set("mongodb")

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val options = MongodbSink.writerOptions(context)

    if (MongodbSink.streamingEnabled(cfg, context.executionMode)) {
      val checkpoint = MongodbSink.optionalString(cfg, MongodbSink.CHECKPOINT_KEY)
        .getOrElse {
          throw new PuddingException(
            s"${MongodbSink.CHECKPOINT_KEY} is required for a MongoDB streaming write!")
        }
      val outputMode = MongodbSink.outputMode(cfg)
      StreamingCheckpoint.validate(
        cfg, context.trigger, Some(checkpoint), context.stateContract)
      val writer = dataFrame.writeStream
        .format("mongodb")
        .options(options)
        .option(MongodbSink.CHECKPOINT_KEY, checkpoint)
        .outputMode(outputMode)
      val query = StreamingTrigger.applyTo(writer, context.trigger).start()
      StreamingCheckpoint.awaitTermination(query, cfg, context.trigger, Some(checkpoint))
    } else {
      dataFrame.write
        .format("mongodb")
        .options(options)
        .mode(MongodbSink.saveMode(cfg))
        .save()
    }
  }
}

private[sink] object MongodbSink extends org.pudding.core.EasyHandle {

  private val DATABASE_KEY = "database"

  private val COLLECTION_KEY = "collection"

  private val OPTIONS_KEY = "options"

  private val URI_KEY = "uri"

  private val CONNECTION_URI_KEY = "connectionUri"

  private val OPERATION_KEY = "operation"

  private val ID_FIELD_KEY = "idField"

  private val ID_FIELDS_KEY = "idFields"

  private val UPSERT_KEY = "upsert"

  private val MODE_KEY = "mode"

  private val CHECKPOINT_KEY = "checkpointLocation"

  private val OUTPUT_MODE_KEY = "outputMode"

  private val STREAMING_KEYS = Seq("isStreaming", "streamWrite", "streamWriter")

  private val WRITE_PREFIX = "spark.mongodb.write."

  private val WRITE_URI_KEY = "spark.mongodb.write.connection.uri"

  private val OPERATIONS = Set("insert", "replace", "update")

  private val SAVE_MODES = Set("append", "overwrite", "ignore", "error", "errorifexists")

  private val OUTPUT_MODES = Set("append")

  def writerOptions(context: SinkContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val database = requiredString(cfg, DATABASE_KEY)
    val collection = requiredString(cfg, COLLECTION_KEY)
    val userOptions = getStringOptions(cfg, OPTIONS_KEY).map { case (key, value) =>
      if (key.startsWith("spark.mongodb.")) key -> value else s"$WRITE_PREFIX$key" -> value
    }

    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "mongodb") {
          throw new PuddingException(
            s"mongodb-sink requires a mongodb connection, but got: ${connection.`type`}")
        }
        connection.options.flatMap {
          case ("connection.uri", value) => Some(WRITE_URI_KEY -> value)
          case (key, value) if key.startsWith("spark.mongodb.") &&
            !key.startsWith("spark.mongodb.read.") => Some(key -> value)
          // Read-only preferences from a shared connection are not write options.
          case (key, _) if key.startsWith("spark.mongodb.read.") => None
          case _ => None
        }
      case None =>
        cfg.get(URI_KEY)
          .orElse(cfg.get(CONNECTION_URI_KEY))
          .map(_.toString)
          .filter(_.trim.nonEmpty)
          .map(value => WRITE_URI_KEY -> value)
          .toMap
    }

    var options = userOptions ++ connectionOptions
    if (!options.contains(WRITE_URI_KEY)) {
      throw new PuddingException(
        s"mongodb-sink requires a mongodb connection or config.$URI_KEY")
    }
    options += "spark.mongodb.write.database" -> database
    options += "spark.mongodb.write.collection" -> collection

    optionalString(cfg, OPERATION_KEY).foreach { value =>
      val operation = value.toLowerCase(Locale.ROOT)
      if (!OPERATIONS.contains(operation)) {
        throw new PuddingException(
          s"unsupported mongodb $OPERATION_KEY: $value, supported: " +
            OPERATIONS.toSeq.sorted.mkString(", "))
      }
      options += "spark.mongodb.write.operationType" -> operation
    }

    val idFields = optionalString(cfg, ID_FIELD_KEY).map(Seq(_))
      .orElse(stringSeq(cfg.get(ID_FIELDS_KEY), ID_FIELDS_KEY))
      .getOrElse(Seq.empty)
    if (idFields.nonEmpty) {
      options += "spark.mongodb.write.idFieldList" -> idFields.mkString(",")
    }

    booleanOption(cfg, UPSERT_KEY).foreach { upsert =>
      if (upsert) {
        options += "spark.mongodb.write.upsertDocument" -> "true"
      } else {
        options += "spark.mongodb.write.upsertDocument" -> "false"
      }
    }
    options
  }

  def saveMode(cfg: Map[String, Any]): SaveMode = {
    val mode = cfg.get(MODE_KEY).filter(_ != null).map(_.toString.trim.toLowerCase(Locale.ROOT))
      .getOrElse("append")
    if (!SAVE_MODES.contains(mode)) {
      throw new PuddingException(s"unsupported mongodb save mode: $mode")
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
      .getOrElse("append")
    if (!OUTPUT_MODES.contains(mode)) {
      throw new PuddingException(
        s"mongodb streaming supports output mode append, but got: $mode")
    }
    mode
  }

  private def stringSeq(value: Option[Any], key: String): Option[Seq[String]] = value.map {
    case text: String =>
      val values = text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      if (values.isEmpty) throw new PuddingException(s"$key can not be empty!")
      values
    case seq: Seq[_] =>
      val values = seq.map(_.toString.trim).filter(_.nonEmpty).toSeq
      if (values.isEmpty) throw new PuddingException(s"$key can not be empty!")
      values
    case array: Array[_] =>
      val values = array.map(_.toString.trim).filter(_.nonEmpty).toSeq
      if (values.isEmpty) throw new PuddingException(s"$key can not be empty!")
      values
    case list: java.util.List[_] =>
      val values = list.asScala.map(_.toString.trim).filter(_.nonEmpty).toSeq
      if (values.isEmpty) throw new PuddingException(s"$key can not be empty!")
      values
    case _ => throw new PuddingException(s"$key must be a string or a list of strings!")
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
