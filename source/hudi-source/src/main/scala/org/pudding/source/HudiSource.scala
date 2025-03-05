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

import org.apache.hudi.DataSourceReadOptions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.definition.DataSource

/**
 *
 */
class HudiSource extends DataSource {

  private val TABLE_KEY = "table"

  private val TABLE_PATH_KEY = "tablePath"

  /**
   * Snapshot、Incremental、cdc
   */
  private val QUERY_TYPE_KEY = "queryType"

  private val BEGIN_TIME_KEY = "beginTime"

  private val END_TIME_KEY = "endTime"

  private val OPTIONS_KEY = "options"

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "hudi-source"

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

    // table parameter must exist
    val table = this.getMapValueThrow[String](cfg, TABLE_KEY)

    val tablePath = this.getMapValueThrow[String](cfg, TABLE_PATH_KEY)

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    val commonOptions = Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.sql.hudi.table.name" -> table
    ) ++ options

    val finallyOptions = this.getMapValueThrow[String](cfg, TABLE_KEY) match {
      case "snapshot" =>
        commonOptions
      case "incremental" =>
        val beginTime = this.getMapValueThrow[String](cfg, BEGIN_TIME_KEY)
        val endTime = this.getMapValueThrow[String](cfg, END_TIME_KEY)

        commonOptions ++ Map(
          QUERY_TYPE.key -> QUERY_TYPE_INCREMENTAL_OPT_VAL,
          BEGIN_INSTANTTIME.key -> beginTime,
          END_INSTANTTIME.key() -> endTime)
      case "read_optimized" =>
        //        Map(QUERY_TYPE.key() -> QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
        // todo
        throw new PuddingException("unrealized!")
      case "cdc" =>
        // todo
        throw new PuddingException("unrealized!")
      case _ =>
        throw new PuddingException("Query type not supported!")
    }

    sparkSession.read
      .format("org.apache.hudi")
      .options(finallyOptions)
      .load(tablePath)
  }
}
