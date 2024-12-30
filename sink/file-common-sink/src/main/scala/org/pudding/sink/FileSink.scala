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
import org.pudding.core.definition.DataSink

/**
 * write data to file system, such as hdfs、local file，support csv、text、json、parquet、orc file type.
 */
class FileSink extends DataSink {

  private val PATH_KEY = "path"

  /**
   * json, csv, parquet, orc, text
   */
  private val FILE_TYPE_KEY = "fileType"

  private val OPTIONS_KEY = "options"

  /**
   * writer mode, such as: Append、Overwrite、ErrorIfExists、Ignore
   */
  private val SAVE_MODE_KEY = "saveMode"

  /**
   * sink writer support streaming， if set isStreaming ture, writer data use streaming
   */
  private val STREAM_WRITER_KEY = "streamWriter"

  /**
   * if set isStreaming ture, outputMode is optional
   */
  private val OUTPUT_MODE_KEY = "outputMode"

  /**
   * if set isStreaming ture, timeoutMs is optional
   */
  private val TIMEOUT_MS_KYE = "timeoutMs"

  /**
   * Data sink identifier，Require implementation of class unique, And consistent with the yaml configuration
   *
   * @return String
   */
  override def identifier: String = "file-sink"

  /**
   * Writer dataframe
   *
   * @param dataFrame DataFrame
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   */
  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    // config param must exist
    val cfg = config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    // path and fileType must exist
    val path = this.getMapValueThrow[String](cfg, PATH_KEY)
    val fileType = this.getMapValueThrow[String](cfg, FILE_TYPE_KEY).toLowerCase(Locale.ROOT)

    val streamWriter = cfg.get(STREAM_WRITER_KEY) match {
      case Some(streamingWriter) => streamingWriter.asInstanceOf[String].toBoolean
      case None => false
    }

    // options param is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    fileType match {
      case "csv" | "text" | "orc" | "parquet" | "json" =>
        if (streamWriter) {
          // streaming writer handle
          // output mode default is append
          val outputMode = cfg.get(OUTPUT_MODE_KEY) match {
            case Some(outputMode) => outputMode.asInstanceOf[String]
            case None => "append"
          }
          val dataStreamWriter = dataFrame.writeStream.options(options).outputMode(outputMode)
          val streamingQuery = dataStreamWriter.format(fileType).start(path)

          // set timeout if exist
          cfg.get(TIMEOUT_MS_KYE) match {
            case Some(timeoutMs) =>
              streamingQuery.awaitTermination(timeoutMs.asInstanceOf[String].toLong)
            case None =>
              streamingQuery.awaitTermination()
          }
          streamingQuery.stop()
        } else {
          // batch writer handle
          val saveMode = this.getMapValueThrow[String](cfg, SAVE_MODE_KEY)
          val dataFrameWriter = dataFrame.write.options(options).mode(saveMode)
          dataFrameWriter.format(fileType).save(path)
        }
      case _ => throw new PuddingException(s"unsupported file type: $fileType")
    }
  }
}
