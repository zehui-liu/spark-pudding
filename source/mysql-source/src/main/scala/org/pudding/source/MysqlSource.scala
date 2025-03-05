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
import org.pudding.core.definition.DataSource

/**
 *
 */
class MysqlSource extends DataSource {

  private val DEFAULT_DRIVER = "com.mysql.cj.jdbc.Driver"

  private val STREAM_READ_KEY = "streamRead"

  private val URL_KEY = "url"

  private val TABLE_KEY = "table"

  private val USER_KEY = "user"

  private val PASS_WORD_KEY = "password"

  private val OPTIONS_KEY = "options"

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "mysql-source"

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

    // url parameter must exist
    val url = this.getMapValueThrow[String](cfg, URL_KEY)

    // table parameter must exist
    val table = this.getMapValueThrow[String](cfg, TABLE_KEY)

    val user = this.getMapValueThrow[String](cfg, USER_KEY)
    val password = this.getMapValueThrow[String](cfg, PASS_WORD_KEY)

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    val streamRead = cfg.getOrElse(STREAM_READ_KEY, false).asInstanceOf[Boolean]

    // more configure see apache spark code org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
    val optionsMap = Map(
      URL_KEY -> url,
      TABLE_KEY -> table,
      USER_KEY -> user,
      PASS_WORD_KEY -> password,
      "driver" -> DEFAULT_DRIVER
    ) ++ options

    if (streamRead) {
      sparkSession.readStream.format("jdbc").options(optionsMap).load()
    } else {
      sparkSession.read.format("jdbc").options(optionsMap).load()
    }
  }
}
