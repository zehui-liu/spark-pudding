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
class HiveSource extends DataSource {

  private val TABLE_KEY = "table"

  private val CONDITION_EXPR_KEY = "conditionExpr"

  private val OPTIONS_KEY = "options"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the yaml configuration
   *
   * @return String
   */
  override def identifier: String = "hive-source"

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

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    val forceConvertSchema = cfg.get(FORCE_CONVERT_SCHEMA_KEY) match {
      case Some(fcs) => fcs.asInstanceOf[String].toBoolean
      case None => false
    }

    // sql filter, if conditionExpr exist
    val originalDataFrame = sparkSession.read.options(options).table(table)
    val processDataFrame = cfg.get(CONDITION_EXPR_KEY) match {
      case Some(conditionExpr) => originalDataFrame.where(conditionExpr.asInstanceOf[String])
      case None => originalDataFrame
    }

    if (forceConvertSchema) {
      val schemas = cfg.get(SCHEMA_KEY) match {
        case Some(schemas) => schemas.asInstanceOf[Map[String, String]]
        case None =>
          throw new PuddingException(
            "forceConvertSchema set true, schema parameter must be have value, please check configure!")
      }
      val schemasSize = schemas.size
      val dataFrameLength = processDataFrame.schema.length
      if (dataFrameLength < schemasSize) {
        throw new PuddingException(
          s"schemas size is $schemasSize can not more " +
            s"than dataframe field size is $dataFrameLength, check schema configure!")
      }
      processDataFrame.select(this.getColumns(schemas).toArray: _*)
    } else {
      processDataFrame
    }
  }
}
