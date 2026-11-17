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

package org.pudding.source

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSource, SourceContext}

/**
 * Reads CSV, text, JSON, Parquet or ORC files from a local filesystem or Hadoop-compatible
 * storage in batch or streaming mode.
 */
class FileSource extends DataSource {

  /** One of `csv`, `text`, `json`, `parquet` or `orc`. */
  private val FILE_TYPE_KEY = "fileType"

  private val PATH_KEY = "path"

  private val STREAM_READ_KEY = "streamRead"

  private val OPTIONS_KEY = "options"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  private val TEXT_DELIMITER_KEY = "textDelimiter"

  private val DEFAULT_TEXT_DELIMITER_KEY = ","

  override def identifier: String = "file-common-source"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val cfg = context.config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val paths = this.requiredString(cfg, PATH_KEY)
      .split(",").map(_.trim).filter(_.nonEmpty)
    if (paths.isEmpty) {
      throw new PuddingException(s"$PATH_KEY can not be empty!")
    }
    val fileType = this.requiredString(cfg, FILE_TYPE_KEY)

    // The explicit execution mode wins; streamRead remains the connector-level fallback.
    val streamRead = this.effectiveStreaming(
      this.booleanValue(cfg, STREAM_READ_KEY).getOrElse(false),
      context.executionMode)

    val options = this.getStringOptions(cfg, OPTIONS_KEY)

    val forceConvertSchema = this.booleanValue(cfg, FORCE_CONVERT_SCHEMA_KEY).getOrElse(false)

    val configuredSchema = cfg.get(SCHEMA_KEY).filter(_ != null) match {
      case Some(schemas: Map[_, _]) =>
        Some(schemas.map {
          case (name, schemaType) =>
            if (name == null || schemaType == null) {
              throw new PuddingException(s"$SCHEMA_KEY can not contain a null key or value")
            }
            name.toString -> schemaType.toString
        }.toMap)
      case Some(schemas: java.util.Map[_, _]) =>
        Some(schemas.asScala.map {
          case (name, schemaType) =>
            if (name == null || schemaType == null) {
              throw new PuddingException(s"$SCHEMA_KEY can not contain a null key or value")
            }
            name.toString -> schemaType.toString
        }.toMap)
      case Some(_) => throw new PuddingException(s"$SCHEMA_KEY must be a map")
      case None => None
    }

    val fileCaseLowerType = fileType.toLowerCase(Locale.ROOT)
    val supportedTypes = Set("csv", "text", "orc", "parquet", "json")
    if (!supportedTypes.contains(fileCaseLowerType)) {
      throw new PuddingException(s"unsupported file type: $fileType")
    }

    val originalDataFrame = if (streamRead) {
      if (paths.length != 1) {
        throw new PuddingException(
          s"streamRead supports exactly one path, got ${paths.length}; " +
            "read multiple directories as separate sources or use batch mode")
      }

      // Spark can not infer a schema for a streaming CSV or JSON source. Apply the
      // configured schema at read time instead of relying on forceConvertSchema.
      val streamingSchema = if (fileCaseLowerType == "csv" || fileCaseLowerType == "json") {
        Some(configuredSchema.getOrElse(throw new PuddingException(
          s"streamRead with fileType '$fileCaseLowerType' requires a configured schema, " +
            "because Spark can not infer a streaming file schema")))
      } else {
        None
      }

      val dataStreamReader = sparkSession.readStream.options(options)
      streamingSchema.foreach(schema => dataStreamReader.schema(this.createStructType(schema)))
      dataStreamReader.format(fileCaseLowerType).load(paths.head)
    } else {
      val dataFrameReader = sparkSession.read.options(options)
      dataFrameReader.format(fileCaseLowerType).load(paths: _*)
    }

    if (forceConvertSchema) {
      val schemas = configuredSchema.getOrElse(throw new PuddingException(
        "forceConvertSchema set true, schema parameter must be have value, please check configure!"))

      // text file spark read line only have a string column, this code handle it to convert schema
      if (fileCaseLowerType == "text") {
        val textDelimiter = cfg.get(TEXT_DELIMITER_KEY).filter(_ != null) match {
          case Some(delimiter: String) if delimiter.nonEmpty => delimiter
          case Some(_: String) =>
            throw new PuddingException(s"$TEXT_DELIMITER_KEY can not be empty!")
          case Some(_) => throw new PuddingException(s"$TEXT_DELIMITER_KEY must be a string!")
          case None => DEFAULT_TEXT_DELIMITER_KEY
        }

        originalDataFrame.map(row => {
          val columns = row.getString(0).split(textDelimiter)
          if (columns.size < schemas.size) {
            throw new PuddingException(
              s"text line has ${columns.size} columns, but $SCHEMA_KEY requires ${schemas.size}")
          }

          var i = -1
          val cols = schemas.map { case (_, v) =>
            i += 1
            buildRow(columns(i), v)
          }.toSeq
          Row(cols: _*)
        })(RowEncoder(this.createStructType(schemas)))
      } else {
        val schemasSize = schemas.size
        val dataFrameLength = originalDataFrame.schema.length
        if (dataFrameLength < schemasSize) {
          throw new PuddingException(
            s"schemas size is $schemasSize can not more" +
              s" than dataframe field size is $dataFrameLength, check schema configure!")
        }
        originalDataFrame.select(this.getColumns(schemas).toArray: _*)
      }
    } else {
      originalDataFrame
    }
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None =>
        throw new PuddingException(s"can not find param: $key, $key must be exist!")
    }
  }

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
}
