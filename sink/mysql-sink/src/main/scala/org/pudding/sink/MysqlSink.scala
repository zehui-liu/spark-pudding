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
import org.pudding.core.definition.DataSink

/**
 *
 */
class MysqlSink extends DataSink{

  private val DEFAULT_DRIVER = "com.mysql.cj.jdbc.Driver"

  private val STREAM_WRITE_KEY = "streamWrite"

  private val URL_KEY = "url"

  private val TABLE_KEY = "table"

  private val USER_KEY = "user"

  private val PASS_WORD_KEY = "password"

  private val MODE_KEY = "mode"

  private val OPTIONS_KEY = "options"

  /**
   * Data sink identifier，Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "mysql-sink"

  /**
   * Writer dataframe
   *
   * @param dataFrame DataFrame
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   */
  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    // config parameter must exist
    val cfg = config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    // url parameter must exist
    val url = this.getMapValueThrow[String](cfg, URL_KEY)

    // table parameter must exist
    val table = this.getMapValueThrow[String](cfg, TABLE_KEY)

    val user = this.getMapValueThrow[String](cfg, USER_KEY)
    val password = this.getMapValueThrow[String](cfg, PASS_WORD_KEY)

    val mode = cfg.get(MODE_KEY) match {
      case Some(mode) => mode.asInstanceOf[String]
      case None => "overwrite"
    }

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    val streamWrite = cfg.getOrElse(STREAM_WRITE_KEY, false).asInstanceOf[Boolean]

    // more configure see apache spark code org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
    val optionsMap = Map(
      URL_KEY -> url,
      TABLE_KEY -> table,
      USER_KEY -> user,
      PASS_WORD_KEY -> password,
      "driver" -> DEFAULT_DRIVER
    ) ++ options

    if (streamWrite) {
      val query = dataFrame.writeStream.format("jdbc").options(optionsMap).start()
      query.awaitTermination()
    } else {
      dataFrame.write.format("jdbc").options(optionsMap).mode(mode).save()
    }
  }
}
