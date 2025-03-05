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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.pudding.core.PuddingException
import org.pudding.core.definition.DataSource

import java.util.Locale

/**
 * read data from file system,such as hdfs、 local file, support csv、text、json、parquet、orc file type.
 */
class FileSource extends DataSource {

  /**
   * json, csv, parquet, orc, text
   */
  private val FILE_TYPE_KEY = "fileType"

  private val PATH_KEY = "path"

  private val STREAM_READ_KEY = "streamRead"

  private val OPTIONS_KEY = "options"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  private val TEXT_DELIMITER_KEY = "textDelimiter"

  private val DEFAULT_TEXT_DELIMITER_KEY = ","

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the yaml configuration
   *
   * @return String
   */
  override def identifier: String = "file-common-source"

  /**
   * Initial data source, use this. have a surprised
   *
   * @param sparkSession SparkSession
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return DataFrame
   */
  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    // config parameter must exist
    val cfg = config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    // path and fileType must exist
    val paths = this.getMapValueThrow[String](cfg, PATH_KEY).split(",")
    val fileType = this.getMapValueThrow[String](cfg, FILE_TYPE_KEY)

    // support steam or batch read, default batch read
    val streamRead = cfg.getOrElse(STREAM_READ_KEY, false).asInstanceOf[Boolean]

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    // if true dataframe force converted schema type
    val forceConvertSchema = cfg.get(FORCE_CONVERT_SCHEMA_KEY) match {
      case Some(fcs) => fcs.asInstanceOf[Boolean]
      case None => false
    }

    val fileCaseLowerType = fileType.toLowerCase(Locale.ROOT)
    val originalDataFrame = fileCaseLowerType match {
      case "csv" | "text" | "orc" | "parquet" | "json" =>
        if (streamRead) {
          // stream read data
          val dataStreamReader = sparkSession.readStream.options(options)
          dataStreamReader.format(fileCaseLowerType).load(paths.head)
        } else {
          // batch read data
          val dataFrameReader = sparkSession.read.options(options)
          dataFrameReader.format(fileCaseLowerType).load(paths: _*)
        }
      case _ =>
        throw new PuddingException(s"unsupported file type: $fileType")
    }

    if (forceConvertSchema) {
      val schemas = cfg.get(SCHEMA_KEY) match {
        case Some(schemas) => schemas.asInstanceOf[Map[String, String]]
        case None =>
          throw new PuddingException(
            "forceConvertSchema set true, schema parameter must be have value, please check configure!")
      }

      // text file spark read line only have a string column, this code handle it to convert schema
      if (fileCaseLowerType == "text") {
        val textDelimiter = cfg.get(TEXT_DELIMITER_KEY) match {
          case Some(delimiter) => delimiter.asInstanceOf[String]
          case None => DEFAULT_TEXT_DELIMITER_KEY
        }

        originalDataFrame.map(row => {
          val columns = row.getString(0).split(textDelimiter)
          assert(columns.size >= schemas.size)

          var i = -1
          val cols = schemas.map { case (k, v) =>
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
        originalDataFrame.show(2)
        originalDataFrame.select(this.getColumns(schemas).toArray: _*)
      }
    } else {
      originalDataFrame
    }
  }
}
