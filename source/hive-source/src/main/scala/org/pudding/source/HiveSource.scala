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
import org.pudding.core.definition.{DataSource, SourceContext}

/**
 * Reads a Hive table through Spark SQL, with an optional post-read filter and schema projection.
 */
class HiveSource extends DataSource {

  private val TABLE_KEY = "table"

  private val CONDITION_EXPR_KEY = "conditionExpr"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  override def identifier: String = "hive-source"

  override def supportedCatalogTypes: Set[String] = Set("hive")

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "hive") {
        throw new PuddingException(
          s"hive-source requires a hive catalog, but got: ${catalog.`type`}")
      }
    }

    val cfg = context.config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val table = this.optionalString(cfg, TABLE_KEY).getOrElse {
      throw new PuddingException(s"can not find param: $TABLE_KEY, $TABLE_KEY must be exist!")
    }

    val originalDataFrame = sparkSession.read.table(table)
    val processDataFrame = this.optionalString(cfg, CONDITION_EXPR_KEY) match {
      case Some(conditionExpr) => originalDataFrame.where(conditionExpr)
      case None => originalDataFrame
    }

    if (this.booleanValue(cfg, FORCE_CONVERT_SCHEMA_KEY)) {
      val schemas = this.getStringOptions(cfg, SCHEMA_KEY)
      if (schemas.isEmpty) {
        throw new PuddingException(
          s"$FORCE_CONVERT_SCHEMA_KEY set true, $SCHEMA_KEY parameter must have a value")
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

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => Some(value.trim)
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def booleanValue(cfg: Map[String, Any], key: String): Boolean = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: Boolean) => value
      case Some(value: String) =>
        value.trim.toLowerCase(java.util.Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ =>
            throw new PuddingException(s"$key must be true or false, but got: $value")
        }
      case Some(value) =>
        throw new PuddingException(s"$key must be boolean, but got: $value")
      case None => false
    }
  }
}
