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
 * Read data from Apache Iceberg tables
 */
class IcebergSource extends DataSource {

  private val TABLE_KEY = "table"

  private val PATH_KEY = "path"

  private val CATALOG_KEY = "catalog"

  private val OPTIONS_KEY = "options"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  private val FILTER_EXPR_KEY = "filterExpr"

  private val SNAPSHOT_ID_KEY = "snapshotId"

  private val AS_OF_TIMESTAMP_KEY = "asOfTimestamp"

  private val BRANCH_KEY = "branch"

  private val TAG_KEY = "tag"

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "iceberg-source"

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

    // Enable Iceberg support
    sparkSession.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    // Get the table identifier (either table name or path)
    val tableIdentifier = cfg.get(TABLE_KEY) match {
      case Some(table) => table.asInstanceOf[String]
      case None =>
        cfg.get(PATH_KEY) match {
          case Some(path) => path.asInstanceOf[String]
          case None => throw new PuddingException("Either 'table' or 'path' parameter must be specified for Iceberg source!")
        }
    }

    // Handle catalog configuration
    val catalogName = cfg.get(CATALOG_KEY).map(_.asInstanceOf[String]).getOrElse("spark_catalog")

    // Build the full table identifier
    val fullTableIdentifier = if (tableIdentifier.contains(".")) {
      tableIdentifier
    } else {
      s"$catalogName.$tableIdentifier"
    }

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    // Apply Iceberg-specific options to Spark session
    options.foreach { case (key, value) =>
      sparkSession.conf.set(key, value)
    }

    // Read the base DataFrame
    val dataFrameReader = sparkSession.read
      .format("iceberg")

    // Apply options to the reader
    val readerWithOptions = options.foldLeft(dataFrameReader) { case (reader, (key, value)) =>
      reader.option(key, value)
    }

    // Handle snapshot-specific reads
    val readerWithTimeTravel = readerWithOptions

    val readerWithSnapshotId = cfg.get(SNAPSHOT_ID_KEY) match {
      case Some(snapshotId) =>
        readerWithTimeTravel.option("snapshot-id", snapshotId.toString)
      case None => readerWithTimeTravel
    }

    val readerWithTimestamp = cfg.get(AS_OF_TIMESTAMP_KEY) match {
      case Some(timestamp) =>
        readerWithSnapshotId.option("as-of-timestamp", timestamp.toString)
      case None => readerWithSnapshotId
    }

    val readerWithBranch = cfg.get(BRANCH_KEY) match {
      case Some(branch) =>
        readerWithTimestamp.option("branch", branch.asInstanceOf[String])
      case None => readerWithTimestamp
    }

    val finalReader = cfg.get(TAG_KEY) match {
      case Some(tag) =>
        readerWithBranch.option("tag", tag.asInstanceOf[String])
      case None => readerWithBranch
    }

    val originalDataFrame = finalReader.load(fullTableIdentifier)

    // Apply filter expression if provided
    val filteredDataFrame = cfg.get(FILTER_EXPR_KEY) match {
      case Some(filterExpr) => originalDataFrame.filter(filterExpr.asInstanceOf[String])
      case None => originalDataFrame
    }

    val forceConvertSchema = cfg.get(FORCE_CONVERT_SCHEMA_KEY) match {
      case Some(fcs) => fcs.asInstanceOf[Boolean]
      case None => false
    }

    if (forceConvertSchema) {
      val schemas = cfg.get(SCHEMA_KEY) match {
        case Some(schemas) => schemas.asInstanceOf[Map[String, String]]
        case None =>
          throw new PuddingException(
            "forceConvertSchema set true, schema parameter must be have value, please check configure!")
      }
      val schemasSize = schemas.size
      val dataFrameLength = filteredDataFrame.schema.length
      if (dataFrameLength < schemasSize) {
        throw new PuddingException(
          s"schemas size is $schemasSize can not more " +
            s"than dataframe field size is $dataFrameLength, check schema configure!")
      }
      filteredDataFrame.select(this.getColumns(schemas).toArray: _*)
    } else {
      filteredDataFrame
    }
  }
}
