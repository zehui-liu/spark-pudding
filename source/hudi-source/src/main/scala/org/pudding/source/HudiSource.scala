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

import java.util.{Locale, UUID}

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, lit, lower, when}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSource, IncrementalRead, IncrementalRequest, IncrementalSemantics, IncrementalSource, SourceContext}

/**
 * Reads Hudi tables.
 *
 * Path mode uses Spark's Hudi data source and supports snapshot, incremental, read-optimized and
 * CDC queries, time travel, schema evolution and arbitrary Hudi read options. Catalog mode reads
 * a table registered in a Hudi catalog (Spark session catalog) and is intended for snapshot
 * queries; advanced options require a table path.
 */
class HudiSource extends DataSource with IncrementalSource {

  private val TABLE_KEY = "table"

  private val TABLE_PATH_KEY = "tablePath"

  private val QUERY_TYPE_KEY = "queryType"

  private val BEGIN_TIME_KEY = "beginTime"

  private val END_TIME_KEY = "endTime"

  private val AS_OF_INSTANT_KEY = "asOfInstant"

  private val FILTER_EXPR_KEY = "filterExpr"

  private val OPTIONS_KEY = "options"

  private val STREAM_READ_KEY = "streamRead"

  private val IS_STREAMING_KEY = "isStreaming"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  private val SCHEMA_EVOLUTION_KEY = "schemaEvolution"

  private val EXTRACT_PARTITION_VALUES_KEY = "extractPartitionValues"

  private val NUM_PARTITIONS_KEY = "numPartitions"

  private val HUDI_TABLE_NAME_KEY = "hoodie.table.name"

  override def identifier: String = "hudi-source"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportsLakeIncremental: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("hudi")

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Upsert, IncrementalSemantics.Cdc)

  override def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "hudi") {
        throw new PuddingException(
          s"hudi-source requires a hudi catalog, but got: ${catalog.`type`}")
      }
    }
    val table = this.requiredString(cfg, TABLE_KEY)
    val tablePath = this.optionalString(cfg, TABLE_PATH_KEY).getOrElse {
      context.catalog match {
        case Some(_) => this.catalogTablePath(sparkSession, table)
        case None => throw new PuddingException(
          s"hudi incremental reads require $TABLE_PATH_KEY or a hudi catalog reference")
      }
    }
    val end = request.end.getOrElse(this.latestCommit(sparkSession, tablePath))
    val start = request.start.orElse {
      request.initial match {
        case "latest" => Some(end)
        case "full" => None
        case _ =>
          throw new PuddingException(
            "hudi incremental read requires incremental.start, initial=latest or initial=full")
      }
    }
    val fullRead = start.isEmpty
    val from = start
    if (from.exists(_.compareTo(end) > 0)) {
      throw new PuddingException(
        s"hudi incremental start ${from.get} is after end $end")
    }
    if (request.semantics == IncrementalSemantics.Cdc && !fullRead) {
      this.requireCdc(sparkSession, tablePath)
    }

    val userOptions = this.getStringOptions(cfg, OPTIONS_KEY)
    val options = userOptions ++ (if (fullRead) {
      Map(
        DataSourceReadOptions.QUERY_TYPE.key ->
          DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key -> end,
        HUDI_TABLE_NAME_KEY -> table)
    } else {
      var incremental = Map(
        DataSourceReadOptions.QUERY_TYPE.key ->
          DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
        DataSourceReadOptions.BEGIN_INSTANTTIME.key -> from.get,
        DataSourceReadOptions.END_INSTANTTIME.key -> end,
        HUDI_TABLE_NAME_KEY -> table)
      if (request.semantics == IncrementalSemantics.Cdc) {
        incremental += "hoodie.datasource.query.incremental.format" -> "cdc"
      }
      incremental
    })

    val raw = sparkSession.read.format("org.apache.hudi").options(options).load(tablePath)
    val (normalized, orderFields) = request.semantics match {
      case IncrementalSemantics.Cdc if fullRead =>
        (this.dropHudiMetadata(raw).withColumn(request.opColumn, lit("I")), Seq.empty[String])
      case IncrementalSemantics.Cdc =>
        this.normalizeCdc(sparkSession, tablePath, raw, request.opColumn)
      case _ =>
        (this.dropHudiMetadata(raw), Seq.empty[String])
    }
    val filtered = cfg.get(FILTER_EXPR_KEY).map(expression =>
      normalized.filter(expression.toString)).getOrElse(normalized)
    val preserve = if (request.semantics == IncrementalSemantics.Cdc) {
      Seq(request.opColumn) ++ orderFields
    } else {
      Seq.empty[String]
    }
    val converted = this.convertSchema(filtered, cfg, preserve)
    val result = this.numPartitions(cfg) match {
      case Some(partitions) => converted.repartition(partitions)
      case None => converted
    }

    IncrementalRead(
      dataFrame = result,
      semantics = request.semantics,
      keyFields = request.keyFields,
      opColumn = request.opColumn,
      fromCursor = from,
      toCursor = end,
      cursorType = "commit_time",
      orderFields = orderFields)
  }

  /**
   * Resolve a Hudi catalog table to its storage location. Hudi 0.14 does not expose an
   * incremental reader for catalog tables, but the catalog table metadata contains the base
   * path the path-based reader already understands.
   */
  private def catalogTablePath(sparkSession: SparkSession, table: String): String = {
    val identifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(table)
    try {
      sparkSession.sessionState.catalog.getTableMetadata(identifier).location.toString
    } catch {
      case error: AnalysisException =>
        throw new PuddingException(
          "hudi incremental read could not resolve the catalog table location; " +
            "the table must exist and be visible through the configured hudi catalog",
          error)
    }
  }

  private def latestCommit(sparkSession: SparkSession, tablePath: String): String = {
    val timeline = this.metaClient(sparkSession, tablePath).getCommitsTimeline
      .filterCompletedInstants
    val latest = timeline.lastInstant()
    if (!latest.isPresent) {
      throw new PuddingException(s"hudi table at $tablePath has no completed commit")
    }
    latest.get().getTimestamp
  }

  private def metaClient(
      sparkSession: SparkSession,
      tablePath: String): HoodieTableMetaClient = {
    try {
      HoodieTableMetaClient.builder()
        .setConf(sparkSession.sessionState.newHadoopConf())
        .setBasePath(tablePath)
        .build()
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"failed to read hudi table metadata at $tablePath: ${error.getMessage}", error)
    }
  }

  private def requireCdc(sparkSession: SparkSession, tablePath: String): Unit = {
    val tableConfig = this.metaClient(sparkSession, tablePath).getTableConfig
    if (!tableConfig.isCDCEnabled) {
      throw new PuddingException(
        "hudi cdc incremental read requires the table property " +
          "'hoodie.table.cdc.enabled'=true")
    }
    if (tableConfig.cdcSupplementalLoggingMode() !=
      HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER) {
      throw new PuddingException(
        "hudi cdc incremental read requires " +
          "'hoodie.table.cdc.supplemental.logging.mode'=DATA_BEFORE_AFTER so that every " +
          "change has a complete before or after image")
    }
  }

  /**
   * Flatten Hudi's CDC relation (`op`, `ts_ms`, `before`, `after` JSON images) into ordinary
   * rows plus one canonical operation column. A delete uses the before image and every other
   * operation uses the after image.
   */
  private def normalizeCdc(
      sparkSession: SparkSession,
      tablePath: String,
      dataFrame: DataFrame,
      opColumn: String): (DataFrame, Seq[String]) = {
    val missing = Seq("op", "ts_ms", "before", "after").filterNot(dataFrame.columns.contains)
    if (missing.nonEmpty) {
      throw new PuddingException(
        "hudi cdc result is missing columns: " + missing.mkString(", ") +
          "; set hoodie.table.cdc.supplemental.logging.mode=DATA_BEFORE_AFTER")
    }
    val metaClient = this.metaClient(sparkSession, tablePath)
    val avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema
    val tableSchema = AvroConversionUtils.convertAvroSchemaToStructType(avroSchema)
    val payloadColumn = "__pudding_cdc_payload"
    val tsColumn = "__pudding_cdc_commit_time"
    // Hudi writes before/after images with Avro logical types encoded as their base values.
    // Spark's from_json would silently turn DATE, TIMESTAMP and DECIMAL values into NULL or
    // apply the wrong epoch unit, so decode the images against the table Avro schema.
    val decoderName = "__pudding_hudi_cdc_" + UUID.randomUUID().toString.replace("-", "")
    sparkSession.udf.register(
      decoderName,
      new HudiCdcImageDecoder(
        avroSchema.toString,
        tableSchema,
        metaClient.getTableConfig.populateMetaFields()),
      tableSchema)
    val businessColumns = tableSchema.fields
      .filterNot(_.name.startsWith("_hoodie_"))
      .map(field => col(payloadColumn).getField(field.name).as(field.name))
      .toSeq
    val operation = lower(col("op"))
    val output = dataFrame
      .withColumn(
        payloadColumn,
        callUDF(decoderName, col("op"), col("before"), col("after")))
      .select(
        (businessColumns ++ Seq(
          when(operation === "i", lit("I"))
            .when(operation === "u", lit("U"))
            .when(operation === "d", lit("D"))
            .otherwise(lit("?")).as(opColumn),
          col("ts_ms").cast("long").as(tsColumn))): _*)
    (output, Seq(tsColumn))
  }

  private def dropHudiMetadata(dataFrame: DataFrame): DataFrame = {
    val metadata = dataFrame.columns.filter(_.startsWith("_hoodie_"))
    if (metadata.isEmpty) dataFrame else dataFrame.drop(metadata: _*)
  }

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val catalog = context.catalog
    catalog.foreach { resolved =>
      if (resolved.`type` != "hudi") {
        throw new PuddingException(
          s"hudi-source requires a hudi catalog, but got: ${resolved.`type`}")
      }
    }

    val table = this.requiredString(cfg, TABLE_KEY)
    val tablePath = this.optionalString(cfg, TABLE_PATH_KEY)
    if (tablePath.isEmpty && catalog.isEmpty) {
      throw new PuddingException(
        s"$TABLE_PATH_KEY is required when no hudi catalog is configured!")
    }

    val userOptions = this.getStringOptions(cfg, OPTIONS_KEY)
    val configuredStreaming = this.booleanOption(cfg, STREAM_READ_KEY)
      .orElse(this.booleanOption(cfg, IS_STREAMING_KEY))
      .getOrElse(false)
    val streaming = this.effectiveStreaming(configuredStreaming, context.executionMode)

    val queryType = this.resolveQueryType(cfg)
    val originalDataFrame = if (tablePath.isDefined) {
      val derivedOptions = this.readOptions(cfg, table, queryType)
      val options = userOptions ++ derivedOptions
      if (streaming) {
        sparkSession.readStream.format("org.apache.hudi").options(options).load(tablePath.get)
      } else {
        sparkSession.read.format("org.apache.hudi").options(options).load(tablePath.get)
      }
    } else {
      if (queryType != "snapshot" || userOptions.nonEmpty ||
        cfg.get(AS_OF_INSTANT_KEY).isDefined) {
        logWarning(
          "hudi catalog mode uses the Spark table API; queryType, options and time-travel " +
            "settings require tablePath and are not applied")
      }
      if (streaming) sparkSession.readStream.table(table) else sparkSession.read.table(table)
    }

    val filtered = cfg.get(FILTER_EXPR_KEY).map { expression =>
      originalDataFrame.filter(expression.toString)
    }.getOrElse(originalDataFrame)

    val converted = this.convertSchema(filtered, cfg)
    this.numPartitions(cfg) match {
      case Some(partitions) => converted.repartition(partitions)
      case None => converted
    }
  }

  private def resolveQueryType(cfg: Map[String, Any]): String = {
    val value = cfg.get(QUERY_TYPE_KEY)
      .map(_.toString.trim.toLowerCase(Locale.ROOT).replace("-", "_"))
      .filter(_.nonEmpty)
      .getOrElse("snapshot")
    value match {
      case "snapshot" | "incremental" | "read_optimized" | "readoptimized" | "cdc" => value
      case other =>
        throw new PuddingException(
          s"unsupported $QUERY_TYPE_KEY: $other, supported: snapshot, incremental, " +
            "read_optimized, cdc")
    }
  }

  private def readOptions(
      cfg: Map[String, Any],
      table: String,
      queryType: String): Map[String, String] = {
    var options = Map[String, String](HUDI_TABLE_NAME_KEY -> table)

    queryType match {
      case "snapshot" =>
        options += DataSourceReadOptions.QUERY_TYPE.key ->
          DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
      case "read_optimized" | "readoptimized" =>
        options += DataSourceReadOptions.QUERY_TYPE.key ->
          DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL
      case "incremental" =>
        options += DataSourceReadOptions.QUERY_TYPE.key ->
          DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
        options += DataSourceReadOptions.BEGIN_INSTANTTIME.key ->
          this.requiredString(cfg, BEGIN_TIME_KEY)
        options += DataSourceReadOptions.END_INSTANTTIME.key ->
          this.requiredString(cfg, END_TIME_KEY)
      case "cdc" =>
        options += DataSourceReadOptions.QUERY_TYPE.key ->
          DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
        options += "hoodie.datasource.query.incremental.format" -> "cdc"
        cfg.get(BEGIN_TIME_KEY).foreach(value =>
          options += DataSourceReadOptions.BEGIN_INSTANTTIME.key -> value.toString)
        cfg.get(END_TIME_KEY).foreach(value =>
          options += DataSourceReadOptions.END_INSTANTTIME.key -> value.toString)
    }

    cfg.get(AS_OF_INSTANT_KEY).foreach { value =>
      options += DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key -> value.toString
    }
    this.booleanOption(cfg, SCHEMA_EVOLUTION_KEY).foreach { value =>
      options += DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key -> value.toString
    }
    this.booleanOption(cfg, EXTRACT_PARTITION_VALUES_KEY).foreach { value =>
      options += DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key ->
        value.toString
    }
    options
  }

  private def convertSchema(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      preserve: Seq[String] = Seq.empty): DataFrame = {
    val forceConvertSchema = this.booleanOption(cfg, FORCE_CONVERT_SCHEMA_KEY).getOrElse(false)
    if (!forceConvertSchema) {
      dataFrame
    } else {
      val schemas = cfg.get(SCHEMA_KEY) match {
        case Some(value: Map[_, _]) =>
          value.map {
            case (key, item) => key.toString -> item.toString
          }.toMap
        case Some(_) => throw new PuddingException(s"$SCHEMA_KEY must be a map!")
        case None =>
          throw new PuddingException(
            "forceConvertSchema set true, schema parameter must be have value, " +
              "please check configure!")
      }
      if (schemas.isEmpty) {
        throw new PuddingException(s"$SCHEMA_KEY can not be empty!")
      }
      if (dataFrame.schema.length < schemas.size) {
        throw new PuddingException(
          s"schemas size is ${schemas.size} can not more than dataframe field size is " +
            s"${dataFrame.schema.length}, check schema configure!")
      }
      val selected = this.getColumns(schemas)
      val extra = preserve
        .filterNot(column => schemas.keys.exists(_.equalsIgnoreCase(column)))
        .map(column => col(column))
      dataFrame.select((selected ++ extra).toArray: _*)
    }
  }

  private def numPartitions(cfg: Map[String, Any]): Option[Int] = {
    cfg.get(NUM_PARTITIONS_KEY).filter(_ != null).map { value =>
      val partitions = try {
        value.toString.trim.toInt
      } catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"$NUM_PARTITIONS_KEY must be an integer, but got: $value")
      }
      if (partitions <= 0) {
        throw new PuddingException(
          s"$NUM_PARTITIONS_KEY must be greater than zero, but got: $partitions")
      }
      partitions
    }
  }


  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => throw new PuddingException(s"can not find param: $key, $key must be exist!")
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
