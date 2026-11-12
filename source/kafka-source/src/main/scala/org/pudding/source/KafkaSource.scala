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

import java.lang.reflect.{InvocationTargetException, Method}
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  from_json,
  lit,
  lower,
  trim,
  udf,
  when
}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}

import org.pudding.core.EasyHandle
import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSource, SourceContext}
import org.pudding.core.incremental.StreamingCdc
import org.pudding.core.util.ConfigRedactor

/**
 * Reads a bounded batch or a Structured Streaming query from Kafka through Spark's Kafka data
 * source. The reader keeps key/value as strings by default; `valueFormat: json` decodes a
 * configured schema, and `cdcFormat` supports a generic operation column or a Debezium envelope.
 *
 * See https://spark.apache.org/docs/3.4.0/structured-streaming-kafka-integration.html for the
 * underlying reader options.
 */
class KafkaSource extends DataSource {

  override def identifier: String = "kafka-source"

  override def supportedConnectionTypes: Set[String] = Set("kafka")

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    if (context.connection.isDefined && cfg.contains(KafkaSource.BootstrapServersKey)) {
      logWarning(s"${KafkaSource.BootstrapServersKey} is ignored because a connection is configured")
    }
    val streaming = context.executionMode.contains(ExecutionMode.Streaming)
    val options = KafkaSource.readerOptions(context)
    KafkaSource.validateOptions(options, streaming)
    if (streaming) {
      KafkaLagMetrics.register(
        sparkSession, options, KafkaLagMetrics.interval(cfg))
    }

    val raw = if (streaming) {
      sparkSession.readStream.format("kafka").options(options).load()
    } else {
      sparkSession.read.format("kafka").options(options).load()
    }
    KafkaSource.decode(raw, cfg, streaming)
  }
}

private[source] object KafkaSource extends EasyHandle {

  val BootstrapServersKey = "kafkaBootstrapServers"

  private val SubscribeKey = "subscribe"

  private val AssignKey = "assign"

  private val SubscribePatternKey = "subscribePattern"

  private val ValueFormatKey = "valueFormat"

  private val ValueSchemaKey = "valueSchema"

  private val SchemaRegistryKey = "schemaRegistry"

  private val SchemaRegistryUrlKey = "schemaRegistryUrl"

  private val SchemaRegistrySubjectKey = "subject"

  private val ParseModeKey = "parseMode"

  private val IncludeMetadataKey = "includeMetadata"

  private val KeepRawValueKey = "keepRawValue"

  private val BadRecordsPathKey = "badRecordsPath"

  private val IncludeCorruptRecordKey = "includeCorruptRecord"

  val CorruptRecordColumn = "_kafka_corrupt_record"

  private case class CorruptionOptions(
      badRecordsPath: Option[String],
      includeCorruptRecord: Boolean)

  private case class SchemaRegistryOptions(
      properties: java.util.Map[String, String],
      subject: Option[String])

  /**
   * Serializable bridge to the optional schema-registry runtime. The concrete decoder is loaded
   * lazily on each executor so the connector jar has no Avro/Protobuf/Schema Registry dependency.
   */
  private class SchemaRegistryDecoder(
      schema: StructType,
      properties: java.util.Map[String, String],
      format: String,
      subject: Option[String]) extends Serializable {

    @transient private var decoder: AnyRef = _
    @transient private var decodeMethod: Method = _

    def ensureAvailable(): Unit = {
      if (this.decoder == null) {
        this.load()
      }
    }

    def decode(topic: String, value: Array[Byte]): Row = {
      if (value == null) {
        null
      } else {
        if (this.decoder == null) {
          this.load()
        }
        try {
          this.decodeMethod
            .invoke(this.decoder, subject.getOrElse(topic), value)
            .asInstanceOf[Row]
        } catch {
          case error: InvocationTargetException =>
            val cause = Option(error.getCause).getOrElse(error)
            val detail = Option(cause.getMessage).filter(_.nonEmpty)
              .getOrElse(cause.getClass.getSimpleName)
            throw new PuddingException(
              s"schema registry $format decode failed for topic '$topic': " +
                ConfigRedactor.redactLine(detail),
              cause)
          case error: ReflectiveOperationException =>
            throw new PuddingException(
              s"failed to invoke schema registry $format decoder: " +
                error.getClass.getSimpleName,
              error)
        }
      }
    }

    private def load(): Unit = {
      try {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(this.getClass.getClassLoader)
        // The optional runtime is loaded by name so the connector has no Confluent dependency.
        // scalastyle:off classforname
        val clazz = Class.forName(SchemaRegistryDecoder.ClassName, true, loader)
        // scalastyle:on classforname
        val constructor = clazz.getConstructor(
          classOf[StructType],
          classOf[java.util.Map[_, _]],
          classOf[String])
        this.decoder = constructor
          .newInstance(schema, properties, format)
          .asInstanceOf[AnyRef]
        this.decodeMethod = clazz.getMethod(
          "decode", classOf[String], classOf[Array[Byte]])
      } catch {
        case error: ClassNotFoundException =>
          throw new PuddingException(
            "schema registry runtime is missing; add kafka-schema-registry-runtime " +
              "to the Spark classpath",
            error)
        case error: ReflectiveOperationException =>
          throw new PuddingException(
            s"failed to create schema registry $format decoder: " +
              error.getClass.getSimpleName,
            error)
      }
    }
  }

  private object SchemaRegistryDecoder {
    val ClassName = "org.pudding.kafka.schema.ConfluentValueDecoder"
  }

  private val FixedSubjectStrategyClass =
    "org.pudding.kafka.schema.FixedSubjectNameStrategy"

  private val FixedSubjectKey = "pudding.schema.registry.subject"

  private val CdcFormatKey = "cdcFormat"

  private val OpColumnKey = "opColumn"

  private val OpFieldKey = "opField"

  private val BatchOnlyOptions = Set("endingoffsets", "endingoffsetsbytimestamp")

  private val MetadataColumns = Seq(
    StreamingCdc.KafkaTopicColumn,
    StreamingCdc.KafkaPartitionColumn,
    StreamingCdc.KafkaOffsetColumn,
    StreamingCdc.KafkaTimestampColumn,
    StreamingCdc.KafkaTimestampTypeColumn,
    StreamingCdc.KafkaKeyColumn)

  def readerOptions(context: SourceContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val subscriptionKeys = Seq(SubscribeKey, AssignKey, SubscribePatternKey)
      .flatMap(key => this.optionalString(cfg, key).map(key -> _))
    if (subscriptionKeys.size != 1) {
      throw new PuddingException(
        s"kafka-source requires exactly one of $SubscribeKey, $AssignKey or $SubscribePatternKey")
    }
    val options = this.getStringOptions(cfg, "options")

    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "kafka") {
          throw new PuddingException(
            s"kafka-source requires a kafka connection, but got: ${connection.`type`}")
        }
        connection.options
      case None =>
        Map("kafka.bootstrap.servers" -> requiredString(cfg, BootstrapServersKey))
    }

    options ++ connectionOptions ++ subscriptionKeys.toMap
  }

  def validateOptions(options: Map[String, String], streaming: Boolean): Unit = {
    if (streaming) {
      val unsupported = options.keys
        .filter(key => BatchOnlyOptions.contains(key.toLowerCase(Locale.ROOT)))
        .toSeq
        .sorted
      if (unsupported.nonEmpty) {
        throw new PuddingException(
          "kafka streaming does not support batch-only option: " + unsupported.mkString(", "))
      }
    }
  }

  /**
   * Decode the raw Kafka columns. Kept package-private so the JSON and CDC contracts can be
   * tested without a broker.
   */
  def decode(
      raw: DataFrame,
      cfg: Map[String, Any],
      streaming: Boolean = false): DataFrame = {
    val valueFormat = this.valueFormat(cfg)
    val cdcFormat = this.cdcFormat(cfg)
    if (cdcFormat.contains("debezium") && valueFormat != "json") {
      throw new PuddingException(
        s"$CdcFormatKey=debezium requires $ValueFormatKey=json")
    }
    val includeMetadata = this.booleanValue(cfg, IncludeMetadataKey).getOrElse(false) ||
      cdcFormat.nonEmpty
    val keepRawValue = this.booleanValue(cfg, KeepRawValueKey).getOrElse(false)
    val opColumn = this.optionalString(cfg, OpColumnKey).getOrElse(StreamingCdc.DefaultOpColumn)
    val opField = this.optionalString(cfg, OpFieldKey).getOrElse(opColumn)
    val badRecordsPath = this.optionalString(cfg, BadRecordsPathKey)
    val includeCorruptRecord =
      this.booleanValue(cfg, IncludeCorruptRecordKey).getOrElse(false)
    this.validateCorruptOptions(
      valueFormat,
      cdcFormat,
      cfg,
      streaming,
      badRecordsPath,
      includeCorruptRecord)

    val base = raw.select(
      col("key").cast(StringType).as("key"),
      col("value").as("value"),
      col("topic").as(StreamingCdc.KafkaTopicColumn),
      col("partition").as(StreamingCdc.KafkaPartitionColumn),
      col("offset").as(StreamingCdc.KafkaOffsetColumn),
      col("timestamp").as(StreamingCdc.KafkaTimestampColumn),
      col("timestampType").as(StreamingCdc.KafkaTimestampTypeColumn))

    valueFormat match {
      case "raw" =>
        if (keepRawValue) {
          throw new PuddingException(s"$KeepRawValueKey is only valid with valueFormat=json")
        }
        val text = base.withColumn("value", col("value").cast(StringType))
        if (includeMetadata) {
          text.select((Seq(col("key"), col("value")) ++ this.metadataColumns()): _*)
        } else {
          text.select(col("key"), col("value"))
        }

      case "json" =>
        val schema = this.valueSchema(cfg)
        this.checkColumnConflicts(schema, includeMetadata, keepRawValue, opColumn, opField)
        val parseMode = this.parseMode(cfg)
        if (cdcFormat.contains("debezium")) {
          this.decodeDebezium(
            base, schema, parseMode, includeMetadata, keepRawValue, opColumn)
        } else {
          this.decodeJson(
            base,
            schema,
            parseMode,
            this.dropsMalformed(cfg),
            includeMetadata,
            keepRawValue,
            cdcFormat.isDefined,
            opColumn,
            opField,
            CorruptionOptions(badRecordsPath, includeCorruptRecord))
        }

      case "avro" | "protobuf" =>
        val schema = this.valueSchema(cfg)
        this.checkColumnConflicts(schema, includeMetadata, keepRawValue, opColumn, opField)
        if (cdcFormat.contains("debezium")) {
          throw new PuddingException(
            s"$CdcFormatKey=debezium requires $ValueFormatKey=json")
        }
        this.decodeSchemaRegistry(
          base,
          schema,
          valueFormat,
          cfg,
          includeMetadata,
          keepRawValue,
          cdcFormat.isDefined,
          opColumn,
          opField)

      case other =>
        throw new PuddingException(
          s"unsupported $ValueFormatKey: $other, supported: raw, string, json, avro, protobuf")
    }
  }

  private def decodeJson(
      base: DataFrame,
      schema: StructType,
      parseMode: String,
      dropMalformed: Boolean,
      includeMetadata: Boolean,
      keepRawValue: Boolean,
      cdc: Boolean,
      opColumn: String,
      opField: String,
      corruption: CorruptionOptions): DataFrame = {
    if (cdc && !schema.fieldNames.contains(opField)) {
      throw new PuddingException(
        s"cdcFormat=op-column requires valueSchema to contain operation field '$opField'")
    }
    val corruptColumn = "__pudding_corrupt_record"
    val trackCorrupt = dropMalformed || corruption.badRecordsPath.nonEmpty ||
      corruption.includeCorruptRecord
    val parseSchema = if (trackCorrupt) {
      StructType(schema.fields ++ Seq(StructField(corruptColumn, StringType)))
    } else {
      schema
    }
    val parseOptions = if (trackCorrupt) {
      Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" -> corruptColumn)
    } else {
      Map("mode" -> parseMode)
    }
    val textValue = col("value").cast(StringType)
    val parsed = from_json(textValue, parseSchema, parseOptions)
    var rows = base.filter(textValue.isNotNull)
      .withColumn("_pudding_value", parsed)
    val corruptRows = if (trackCorrupt) {
      rows.filter(col(s"_pudding_value.$corruptColumn").isNotNull)
    } else {
      rows.limit(0)
    }
    corruption.badRecordsPath.foreach { path =>
      corruptRows.select(
        col("key"),
        textValue.as("value"),
        col(StreamingCdc.KafkaTopicColumn),
        col(StreamingCdc.KafkaPartitionColumn),
        col(StreamingCdc.KafkaOffsetColumn),
        col(s"_pudding_value.$corruptColumn").as("_error"))
        .write.mode("append").format("json").save(path)
    }
    if (dropMalformed) {
      rows = rows.filter(col(s"_pudding_value.$corruptColumn").isNull)
    }

    val parsedColumns = schema.fieldNames.map { name =>
      col(s"_pudding_value.$name").as(name)
    }
    val selected = this.metadataPrefix(includeMetadata) ++ parsedColumns ++
      (if (keepRawValue) Seq(textValue.as("_kafka_value")) else Seq.empty) ++
      (if (corruption.includeCorruptRecord) {
        Seq(col(s"_pudding_value.$corruptColumn").as(CorruptRecordColumn))
      } else {
        Seq.empty
      })
    var result = rows.select(selected: _*)
    if (cdc) {
      if (opField != opColumn) {
        result = result.withColumn(opColumn, col(opField)).drop(opField)
      }
      result = StreamingCdc.normalizeOps(result, opColumn)
    }
    result
  }

  private def decodeSchemaRegistry(
      base: DataFrame,
      schema: StructType,
      format: String,
      cfg: Map[String, Any],
      includeMetadata: Boolean,
      keepRawValue: Boolean,
      cdc: Boolean,
      opColumn: String,
      opField: String): DataFrame = {
    if (cdc && !schema.fieldNames.contains(opField)) {
      throw new PuddingException(
        s"cdcFormat=op-column requires valueSchema to contain operation field '$opField'")
    }
    val registry = this.schemaRegistryOptions(cfg, format)
    val decoder = new SchemaRegistryDecoder(
      schema, registry.properties, format, registry.subject)
    decoder.ensureAvailable()
    val decode = udf(new UDF2[String, Array[Byte], Row] {
      override def call(topic: String, value: Array[Byte]): Row =
        decoder.decode(topic, value)
    }, schema)
    val rows = base
      .filter(col("value").isNotNull)
      .withColumn(
        "_pudding_decoded",
        decode(col(StreamingCdc.KafkaTopicColumn), col("value")))
    val parsedColumns = schema.fieldNames.map { name =>
      col(s"_pudding_decoded.$name").as(name)
    }
    val selected = this.metadataPrefix(includeMetadata) ++ parsedColumns ++
      (if (keepRawValue) Seq(col("value").as("_kafka_value")) else Seq.empty)
    var result = rows.select(selected: _*)
    if (cdc) {
      if (opField != opColumn) {
        result = result.withColumn(opColumn, col(opField)).drop(opField)
      }
      result = StreamingCdc.normalizeOps(result, opColumn)
    }
    result
  }

  private def schemaRegistryOptions(
      cfg: Map[String, Any],
      format: String): SchemaRegistryOptions = {
    val values = cfg.get(SchemaRegistryKey).filter(_ != null) match {
      case Some(value: Map[_, _]) => this.stringKeyMap(value, SchemaRegistryKey)
      case Some(value: java.util.Map[_, _]) =>
        this.stringKeyMap(value.asScala.toMap, SchemaRegistryKey)
      case Some(_) =>
        throw new PuddingException(s"$SchemaRegistryKey must be a map")
      case None => Map.empty[String, Any]
    }
    val url = this.optionalString(values, "url")
      .orElse(this.optionalString(cfg, SchemaRegistryUrlKey))
      .getOrElse {
        throw new PuddingException(
          s"$SchemaRegistryKey.url or $SchemaRegistryUrlKey is required for " +
            s"$ValueFormatKey=$format")
      }
    val properties = this.getStringOptions(values, "properties")
    val username = this.optionalString(values, "username")
    val password = this.optionalString(values, "password")
    if (username.isDefined != password.isDefined) {
      throw new PuddingException(
        s"$SchemaRegistryKey.username and $SchemaRegistryKey.password must be set together")
    }
    val props = new java.util.HashMap[String, String]()
    properties.foreach { case (key, value) => props.put(key, value) }
    props.put("schema.registry.url", url)
    if (format == "avro") {
      props.put("specific.avro.reader", "false")
    } else {
      props.put("derive.type", "false")
    }
    for (user <- username; secret <- password) {
      props.put("basic.auth.credentials.source", "USER_INFO")
      props.put("basic.auth.user.info", s"$user:$secret")
    }
    val subject = this.optionalString(values, SchemaRegistrySubjectKey)
    subject.foreach { value =>
      props.put("value.subject.name.strategy", FixedSubjectStrategyClass)
      props.put(FixedSubjectKey, value)
    }
    SchemaRegistryOptions(
      properties = props,
      subject = subject)
  }

  private def stringKeyMap(
      values: Map[_, _],
      field: String): Map[String, Any] = {
    values.map {
      case (key: String, value) if value != null => key -> value
      case _ =>
        throw new PuddingException(s"$field must contain non-null string keys and values")
    }.toMap
  }

  private def validateCorruptOptions(
      valueFormat: String,
      cdcFormat: Option[String],
      cfg: Map[String, Any],
      streaming: Boolean,
      badRecordsPath: Option[String],
      includeCorruptRecord: Boolean): Unit = {
    if (valueFormat != "json" && (badRecordsPath.nonEmpty || includeCorruptRecord)) {
      throw new PuddingException(
        s"$BadRecordsPathKey and $IncludeCorruptRecordKey require $ValueFormatKey=json")
    }
    if (cdcFormat.contains("debezium") &&
      (badRecordsPath.nonEmpty || includeCorruptRecord)) {
      throw new PuddingException(
        s"$BadRecordsPathKey and $IncludeCorruptRecordKey are not supported for debezium")
    }
    if (badRecordsPath.nonEmpty) {
      if (streaming) {
        throw new PuddingException(
          s"$BadRecordsPathKey is batch-only; use $IncludeCorruptRecordKey with " +
            "parseMode=permissive for streaming")
      }
      if (!this.dropsMalformed(cfg)) {
        throw new PuddingException(
          s"$BadRecordsPathKey requires parseMode=dropMalformed")
      }
    }
    if (includeCorruptRecord) {
      if (this.dropsMalformed(cfg)) {
        throw new PuddingException(
          s"$IncludeCorruptRecordKey can not be combined with parseMode=dropMalformed")
      }
      if (this.parseMode(cfg) != "PERMISSIVE") {
        throw new PuddingException(
          s"$IncludeCorruptRecordKey requires parseMode=permissive")
      }
    }
  }

  private def decodeDebezium(
      base: DataFrame,
      schema: StructType,
      parseMode: String,
      includeMetadata: Boolean,
      keepRawValue: Boolean,
      opColumn: String): DataFrame = {
    if (schema.fieldNames.contains(opColumn)) {
      throw new PuddingException(
        s"valueSchema for debezium must not contain opColumn '$opColumn'")
    }
    val envelope = StructType(Seq(
      StructField("before", schema),
      StructField("after", schema),
      StructField("op", StringType),
      StructField("ts_ms", LongType)))
    val textValue = col("value").cast(StringType)
    val parsed = from_json(textValue, envelope, Map("mode" -> parseMode))
    val rows = base.filter(textValue.isNotNull)
      .withColumn("_pudding_cdc", parsed)
      .filter(col("_pudding_cdc").isNotNull)

    val rawOp = lower(trim(col("_pudding_cdc.op")))
    val opError = org.apache.spark.sql.functions.concat(
      lit("unsupported debezium operation: "),
      coalesce(col("_pudding_cdc.op"), lit("null")))
    val rowExpression = when(rawOp === "d", col("_pudding_cdc.before"))
      .otherwise(col("_pudding_cdc.after"))
    val rowColumns = schema.fieldNames.map { name =>
      rowExpression.getField(name).as(name)
    }
    val selected = this.metadataPrefix(includeMetadata) ++ rowColumns ++ Seq(
      when(rawOp === "c" || rawOp === "r", lit("I"))
        .when(rawOp === "u", lit("U"))
        .when(rawOp === "d", lit("D"))
        .otherwise(org.apache.spark.sql.functions.raise_error(opError)).as(opColumn),
      col("_pudding_cdc.ts_ms").as(StreamingCdc.DebeziumTimestampColumn)) ++
      (if (keepRawValue) Seq(textValue.as("_kafka_value")) else Seq.empty)
    rows.select(selected: _*)
  }

  private def metadataPrefix(include: Boolean): Seq[Column] = {
    if (include) this.metadataColumns() else Seq.empty
  }

  private def metadataColumns(): Seq[Column] = Seq(
    col(StreamingCdc.KafkaTopicColumn),
    col(StreamingCdc.KafkaPartitionColumn),
    col(StreamingCdc.KafkaOffsetColumn),
    col(StreamingCdc.KafkaTimestampColumn),
    col(StreamingCdc.KafkaTimestampTypeColumn),
    col("key").as(StreamingCdc.KafkaKeyColumn))

  private def valueFormat(cfg: Map[String, Any]): String = {
    this.optionalString(cfg, ValueFormatKey)
      .map(_.toLowerCase(Locale.ROOT))
      .getOrElse("raw") match {
      case "raw" | "string" => "raw"
      case "json" => "json"
      case "avro" => "avro"
      case "protobuf" | "proto" => "protobuf"
      case other =>
        throw new PuddingException(
          s"unsupported $ValueFormatKey: $other, supported: raw, string, json, avro, protobuf")
    }
  }

  private def cdcFormat(cfg: Map[String, Any]): Option[String] = {
    this.optionalString(cfg, CdcFormatKey).map(_.toLowerCase(Locale.ROOT)) match {
      case Some("op-column") | Some("op") | Some("operation") => Some("op")
      case Some("debezium") => Some("debezium")
      case Some("none") => None
      case Some(other) =>
        throw new PuddingException(
          s"unsupported $CdcFormatKey: $other, supported: none, op-column, debezium")
      case None => None
    }
  }

  private def parseMode(cfg: Map[String, Any]): String = {
    this.optionalString(cfg, ParseModeKey)
      .map(_.toLowerCase(Locale.ROOT))
      .getOrElse("failfast") match {
      case "fail" | "failfast" => "FAILFAST"
      // from_json has no DROPMALFORMED mode in Spark 3.4; drop semantics are implemented with
      // the corrupt-record column in decodeJson.
      case "drop" | "dropmalformed" => "PERMISSIVE"
      case "permissive" => "PERMISSIVE"
      case other =>
        throw new PuddingException(
          s"unsupported $ParseModeKey: $other, supported: failfast, dropMalformed, permissive")
    }
  }

  private def dropsMalformed(cfg: Map[String, Any]): Boolean = {
    this.optionalString(cfg, ParseModeKey)
      .map(_.toLowerCase(Locale.ROOT))
      .exists(value => value == "drop" || value == "dropmalformed")
  }

  private def valueSchema(cfg: Map[String, Any]): StructType = {
    cfg.get(ValueSchemaKey).filter(_ != null) match {
      case Some(text: String) if text.trim.nonEmpty =>
        val ddl = text.trim
        if (ddl.toLowerCase(Locale.ROOT).startsWith("struct<")) {
          DataType.fromDDL(ddl) match {
            case value: StructType => value
            case _ =>
              throw new PuddingException(s"$ValueSchemaKey must describe a struct")
          }
        } else {
          StructType.fromDDL(ddl)
        }
      case Some(values: Map[_, _]) =>
        val fields = values.map { case (name, kind) =>
          this.requiredSchemaEntry(name, kind)
        }.toMap
        if (fields.isEmpty) {
          throw new PuddingException(s"$ValueSchemaKey can not be empty")
        }
        this.createStructType(fields)
      case Some(values: java.util.Map[_, _]) =>
        val fields = values.asScala.map { case (name, kind) =>
          this.requiredSchemaEntry(name, kind)
        }.toMap
        if (fields.isEmpty) {
          throw new PuddingException(s"$ValueSchemaKey can not be empty")
        }
        this.createStructType(fields)
      case Some(_) =>
        throw new PuddingException(s"$ValueSchemaKey must be a map or a DDL string")
      case None =>
        throw new PuddingException(s"$ValueSchemaKey is required when $ValueFormatKey=json")
    }
  }

  private def requiredSchemaEntry(name: Any, kind: Any): (String, String) = {
    if (name == null || kind == null) {
      throw new PuddingException(s"$ValueSchemaKey can not contain a null field or type")
    }
    val field = name.toString.trim
    val fieldType = kind.toString.trim
    if (field.isEmpty || fieldType.isEmpty) {
      throw new PuddingException(s"$ValueSchemaKey can not contain an empty field or type")
    }
    field -> fieldType
  }

  private def checkColumnConflicts(
      schema: StructType,
      includeMetadata: Boolean,
      keepRawValue: Boolean,
      opColumn: String,
      opField: String): Unit = {
    if (includeMetadata) {
      val conflicts = schema.fieldNames.intersect(MetadataColumns)
      if (conflicts.nonEmpty) {
        throw new PuddingException(
          s"valueSchema contains reserved metadata columns: ${conflicts.mkString(", ")}")
      }
    }
    if (keepRawValue && schema.fieldNames.contains("_kafka_value")) {
      throw new PuddingException(
        "valueSchema contains reserved column: _kafka_value")
    }
    if (schema.fieldNames.contains(CorruptRecordColumn)) {
      throw new PuddingException(
        s"valueSchema contains reserved column: $CorruptRecordColumn")
    }
    val reservedPrefixes = schema.fieldNames.filter(StreamingCdc.isInternalColumn)
    if (reservedPrefixes.nonEmpty) {
      throw new PuddingException(
        "valueSchema contains reserved metadata columns: " +
          reservedPrefixes.mkString(", "))
    }
    if (opColumn == StreamingCdc.KafkaKeyColumn || StreamingCdc.isInternalColumn(opColumn)) {
      throw new PuddingException(s"opColumn uses a reserved column name: $opColumn")
    }
    if (opField != opColumn && schema.fieldNames.contains(opColumn)) {
      throw new PuddingException(
        s"valueSchema contains opColumn '$opColumn' while opField is '$opField'")
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

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    this.optionalString(cfg, key).getOrElse {
      throw new PuddingException(s"can not find param: $key")
    }
  }
}
