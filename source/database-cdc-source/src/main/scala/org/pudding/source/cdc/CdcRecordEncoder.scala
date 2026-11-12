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

package org.pudding.source.cdc

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.Base64

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.apache.spark.sql.types._
import org.bson.{BsonArray, BsonBinary, BsonDateTime, BsonDocument, BsonNull,
  BsonNumber, BsonObjectId, BsonRegularExpression, BsonString, BsonTimestamp, BsonValue,
  BsonUndefined}
import org.bson.Document

import org.pudding.core.PuddingException

private[cdc] object CdcRecordEncoder {

  private val Mapper = new ObjectMapper()

  private val InstantFormat = DateTimeFormatter.ISO_INSTANT

  private val DateFormat = DateTimeFormatter.ISO_LOCAL_DATE

  private val LocalDateTimeFormat = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  /**
   * Convert one Debezium row into the JSON document consumed by the Spark `from_json` projection.
   * Tombstones are skipped by returning None.
   */
  def debezium(record: SourceRecord, config: CdcConfig, sequence: Long): Option[String] = {
    val value = record.value()
    if (value == null) {
      None
    } else {
      val struct = value match {
        case value: Struct => value
        case other =>
          throw new PuddingException(
            s"${config.label} expected a Debezium struct value, got " +
              other.getClass.getSimpleName)
      }
      // Debezium heartbeat and transaction-metadata records share the Connect queue but do not
      // carry a data operation. Skipping them keeps heartbeat.interval.ms and transaction
      // metadata enabled without failing the stream.
      if (struct.schema() == null || struct.schema().field("op") == null) {
        return None
      }
      val op = this.debeziumOp(struct, config)
      val row = op match {
        case "D" => this.structField(struct, "before", config)
        case _ => this.structField(struct, "after", config)
      }
      if (row == null) {
        throw new PuddingException(
          s"${config.label} received a ${op} record without row data")
      }
      if (!row.isInstanceOf[Struct]) {
        throw new PuddingException(
          s"${config.label} received a ${op} record without a struct row")
      }
      val timestamp = this.debeziumTimestamp(struct, record)
      Some(this.encode(config, row.asInstanceOf[Struct], op, timestamp, sequence))
    }
  }

  /**
   * Snapshot documents are emitted as inserts with timestamp zero so any later change event
   * wins the sink's per-key ordering without depending on the snapshot scan time.
   */
  def mongoSnapshot(
      document: Document,
      config: MongodbConfig,
      sequence: Long): String = {
    this.encodeJson(config, this.documentValue(document, config), "I", 0L, sequence)
  }

  def mongoInsert(
      document: Document,
      timestamp: Long,
      config: MongodbConfig,
      sequence: Long): String = {
    this.encodeJson(config, this.documentValue(document, config), "I", timestamp, sequence)
  }

  def mongoUpdate(
      document: Document,
      timestamp: Long,
      config: MongodbConfig,
      sequence: Long): String = {
    this.encodeJson(config, this.documentValue(document, config), "U", timestamp, sequence)
  }

  def mongoDelete(
      id: Any,
      timestamp: Long,
      config: MongodbConfig,
      sequence: Long): String = {
    val values = config.schema.fields.map { field =>
      if (field.name == "_id") {
        field.name -> this.convertMongo(id, field.dataType, config, "column '_id'")
      } else {
        field.name -> null
      }
    }.toMap
    this.encodeJson(config, values, "D", timestamp, sequence)
  }

  private def encode(
      config: CdcConfig,
      struct: Struct,
      op: String,
      timestamp: Long,
      sequence: Long): String = {
    val values = config.schema.fields.map { field =>
      field.name -> this.structValue(struct, field.name, field.dataType, config)
    }.toMap
    this.encodeJson(config, values, op, timestamp, sequence)
  }

  private def encodeJson(
      config: CdcConfig,
      values: Map[String, Any],
      op: String,
      timestamp: Long,
      sequence: Long): String = {
    val root = Mapper.createObjectNode()
    config.schema.fields.foreach { field =>
      val valueNode: com.fasterxml.jackson.databind.JsonNode = values.get(field.name) match {
        case Some(value) => Mapper.valueToTree(value)
        case None => NullNode.getInstance()
      }
      root.set[com.fasterxml.jackson.databind.JsonNode](field.name, valueNode)
    }
    root.put(config.opColumn, op)
    root.put(CdcSchema.TimestampColumn, timestamp)
    root.put(CdcSchema.SequenceColumn, sequence)
    root.put(CdcSchema.TableColumn, config.table)
    Mapper.writeValueAsString(root)
  }

  private def structValue(
      struct: Struct,
      name: String,
      dataType: DataType,
      config: CdcConfig): Any = {
    if (struct.schema() == null || struct.schema().field(name) == null) {
      throw new PuddingException(
        s"${config.label} record is missing source column '$name'")
    }
    val value = struct.get(name)
    this.convertConnect(value, dataType, config, s"column '$name'")
  }

  private def structField(struct: Struct, name: String, config: CdcConfig): Any = {
    if (struct.schema() == null || struct.schema().field(name) == null) {
      throw new PuddingException(
        s"${config.label} Debezium record is missing '$name'")
    }
    struct.get(name)
  }

  private def debeziumOp(struct: Struct, config: CdcConfig): String = {
    if (struct.schema() == null || struct.schema().field("op") == null) {
      throw new PuddingException(
        s"${config.label} Debezium record is missing operation field 'op'")
    }
    Option(struct.getString("op")).map(_.toLowerCase(java.util.Locale.ROOT)) match {
      case Some("c") | Some("r") => "I"
      case Some("u") => "U"
      case Some("d") => "D"
      case Some(other) =>
        throw new PuddingException(
          s"${config.label} does not support Debezium operation '$other'")
      case None =>
        throw new PuddingException(s"${config.label} Debezium record has no operation")
    }
  }

  private def debeziumTimestamp(struct: Struct, record: SourceRecord): Long = {
    if (struct.schema() != null && struct.schema().field("source") != null) {
      struct.get("source") match {
        case source: Struct
            if source.schema() != null && source.schema().field("ts_ms") != null =>
          source.getInt64("ts_ms")
        case _ =>
          Option(record.timestamp()).map(_.longValue()).getOrElse(System.currentTimeMillis())
      }
    } else {
      Option(record.timestamp()).map(_.longValue()).getOrElse(System.currentTimeMillis())
    }
  }

  private def documentValue(document: Document, config: MongodbConfig): Map[String, Any] = {
    config.schema.fields.map { field =>
      field.name -> this.convertMongo(document.get(field.name), field.dataType, config,
        s"column '${field.name}'")
    }.toMap
  }

  private def convertConnect(
      value: Any,
      dataType: DataType,
      config: CdcConfig,
      location: String): Any = {
    if (value == null) {
      null
    } else {
      dataType match {
        case _: StructType => this.convertStruct(value, dataType.asInstanceOf[StructType],
          config, location)
        case array: ArrayType =>
          this.convertArray(value, array.elementType, config, location)
        case map: MapType =>
          this.convertMap(value, map.valueType, config, location)
        case other => this.convertScalar(value, other, config, location)
      }
    }
  }

  private def convertMongo(
      value: Any,
      dataType: DataType,
      config: CdcConfig,
      location: String): Any = {
    val unwrapped = this.mongoValue(value)
    if (unwrapped == null) {
      null
    } else {
      dataType match {
        case _: StructType =>
          unwrapped match {
            case document: Document =>
              this.convertDocument(document, dataType.asInstanceOf[StructType], config, location)
            case document: BsonDocument =>
              this.convertDocument(new Document(document), dataType.asInstanceOf[StructType],
                config, location)
            case map: util.Map[_, _] =>
              this.convertJavaMap(map, dataType.asInstanceOf[StructType], config, location)
            case _ =>
              throw this.mismatch(config, location, "document", unwrapped)
          }
        case array: ArrayType =>
          unwrapped match {
            case values: util.Collection[_] =>
              values.asScala.zipWithIndex.map { case (item, index) =>
                this.convertMongo(item, array.elementType, config, s"$location[$index]")
              }.asJava
            case values: Array[_] =>
              values.zipWithIndex.map { case (item, index) =>
                this.convertMongo(item, array.elementType, config, s"$location[$index]")
              }.toSeq.asJava
            case _ =>
              throw this.mismatch(config, location, "array", unwrapped)
          }
        case map: MapType =>
          unwrapped match {
            case values: util.Map[_, _] =>
              values.asScala.map { case (key, value) =>
                key.toString -> this.convertMongo(value, map.valueType, config, s"$location.$key")
              }.toMap.asJava
            case _ =>
              throw this.mismatch(config, location, "map", unwrapped)
          }
        case other => this.convertScalar(unwrapped, other, config, location)
      }
    }
  }

  private def convertStruct(
      value: Any,
      dataType: StructType,
      config: CdcConfig,
      location: String): util.Map[String, Object] = {
    value match {
      case struct: Struct =>
        val result = new util.LinkedHashMap[String, Object]()
        dataType.fields.foreach { field =>
          if (struct.schema() == null || struct.schema().field(field.name) == null) {
            throw new PuddingException(
              s"${config.label} record is missing nested column '${field.name}' at $location")
          }
          result.put(field.name,
            this.convertConnect(struct.get(field.name), field.dataType, config,
              s"$location.${field.name}").asInstanceOf[Object])
        }
        result
      case map: util.Map[_, _] =>
        this.convertJavaMap(map, dataType, config, location)
      case _ =>
        throw this.mismatch(config, location, "struct", value)
    }
  }

  private def convertJavaMap(
      value: util.Map[_, _],
      dataType: StructType,
      config: CdcConfig,
      location: String): util.Map[String, Object] = {
    val result = new util.LinkedHashMap[String, Object]()
    dataType.fields.foreach { field =>
      result.put(field.name,
        this.convertMongo(value.get(field.name), field.dataType, config,
          s"$location.${field.name}").asInstanceOf[Object])
    }
    result
  }

  private def convertDocument(
      document: Document,
      dataType: StructType,
      config: CdcConfig,
      location: String): util.Map[String, Object] = {
    val result = new util.LinkedHashMap[String, Object]()
    dataType.fields.foreach { field =>
      result.put(field.name,
        this.convertMongo(document.get(field.name), field.dataType, config,
          s"$location.${field.name}").asInstanceOf[Object])
    }
    result
  }

  private def convertArray(
      value: Any,
      elementType: DataType,
      config: CdcConfig,
      location: String): util.List[Object] = {
    val result = new util.ArrayList[Object]()
    value match {
      case values: util.Collection[_] =>
        values.asScala.zipWithIndex.foreach { case (item, index) =>
          result.add(this.convertConnect(item, elementType, config, s"$location[$index]")
            .asInstanceOf[Object])
        }
      case values: Array[_] =>
        values.zipWithIndex.foreach { case (item, index) =>
          result.add(this.convertConnect(item, elementType, config, s"$location[$index]")
            .asInstanceOf[Object])
        }
      case other =>
        throw this.mismatch(config, location, "array", other)
    }
    result
  }

  private def convertMap(
      value: Any,
      valueType: DataType,
      config: CdcConfig,
      location: String): util.Map[String, Object] = {
    value match {
      case values: util.Map[_, _] =>
        val result = new util.LinkedHashMap[String, Object]()
        values.asScala.foreach { case (key, item) =>
          result.put(key.toString,
            this.convertConnect(item, valueType, config, s"$location.$key")
              .asInstanceOf[Object])
        }
        result
      case other =>
        throw this.mismatch(config, location, "map", other)
    }
  }

  private def convertScalar(
      value: Any,
      dataType: DataType,
      config: CdcConfig,
      location: String): Any = {
    dataType match {
      case StringType => this.asString(value)
      case BooleanType => this.asBoolean(value, config, location)
      case IntegerType => this.asInt(value, config, location)
      case LongType => this.asLong(value, config, location)
      case FloatType => this.asFloat(value, config, location)
      case DoubleType => this.asDouble(value, config, location)
      case _: DecimalType => this.asDecimal(value, config, location)
      case DateType => this.asDate(value, config, location)
      case TimestampType => this.asTimestamp(value, config, location)
      case BinaryType => this.asBinary(value, config, location)
      case other =>
        throw new PuddingException(
          s"${config.label} does not support Spark type ${other.simpleString} at $location")
    }
  }

  private def asString(value: Any): String = {
    value match {
      case text: String => text
      case bytes: Array[Byte] => Base64.getEncoder.encodeToString(bytes)
      case buffer: ByteBuffer =>
        val bytes = new Array[Byte](buffer.remaining())
        buffer.duplicate().get(bytes)
        Base64.getEncoder.encodeToString(bytes)
      case timestamp: Timestamp => InstantFormat.format(timestamp.toInstant)
      case date: java.util.Date => InstantFormat.format(date.toInstant)
      case text: java.time.temporal.TemporalAccessor => text.toString
      case decimal: java.math.BigDecimal => decimal.toPlainString
      case other => other.toString
    }
  }

  private def asBoolean(value: Any, config: CdcConfig, location: String): Boolean = {
    value match {
      case bool: java.lang.Boolean => bool.booleanValue()
      case number: java.lang.Number => number.longValue() != 0L
      case text: String =>
        text.trim.toLowerCase(java.util.Locale.ROOT) match {
          case "true" | "1" => true
          case "false" | "0" => false
          case _ => throw this.mismatch(config, location, "boolean", value)
        }
      case other => throw this.mismatch(config, location, "boolean", other)
    }
  }

  private def asInt(value: Any, config: CdcConfig, location: String): Int = {
    value match {
      case bool: java.lang.Boolean => if (bool) 1 else 0
      case text: String =>
        try text.trim.toInt catch {
          case _: NumberFormatException => throw this.mismatch(config, location, "integer", value)
        }
      case number: java.lang.Number =>
        val exact = this.integral(number, config, location, "integer", value)
        if (exact < BigInt(Int.MinValue) || exact > BigInt(Int.MaxValue)) {
          throw this.mismatch(config, location, "integer", value)
        }
        exact.toInt
      case other => throw this.mismatch(config, location, "integer", other)
    }
  }

  private def asLong(value: Any, config: CdcConfig, location: String): Long = {
    value match {
      case bool: java.lang.Boolean => if (bool) 1L else 0L
      case text: String =>
        try text.trim.toLong catch {
          case _: NumberFormatException => throw this.mismatch(config, location, "long", value)
        }
      case number: java.lang.Number =>
        val exact = this.integral(number, config, location, "long", value)
        if (exact < BigInt(Long.MinValue) || exact > BigInt(Long.MaxValue)) {
          throw this.mismatch(config, location, "long", value)
        }
        exact.toLong
      case other => throw this.mismatch(config, location, "long", other)
    }
  }

  private def integral(
      number: java.lang.Number,
      config: CdcConfig,
      location: String,
      targetType: String,
      original: Any): BigInt = {
    val decimal = try {
      number match {
        case value: java.math.BigDecimal => value
        case value: java.math.BigInteger => new java.math.BigDecimal(value)
        case value => new java.math.BigDecimal(value.toString)
      }
    } catch {
      case _: NumberFormatException => throw this.mismatch(config, location, targetType, original)
    }
    try {
      BigInt(decimal.toBigIntegerExact)
    } catch {
      case _: ArithmeticException => throw this.mismatch(config, location, targetType, original)
    }
  }

  private def asFloat(value: Any, config: CdcConfig, location: String): Float = {
    value match {
      case number: java.lang.Number => number.floatValue()
      case text: String =>
        try text.trim.toFloat catch {
          case _: NumberFormatException => throw this.mismatch(config, location, "float", value)
        }
      case other => throw this.mismatch(config, location, "float", other)
    }
  }

  private def asDouble(value: Any, config: CdcConfig, location: String): Double = {
    value match {
      case number: java.lang.Number => number.doubleValue()
      case text: String =>
        try text.trim.toDouble catch {
          case _: NumberFormatException => throw this.mismatch(config, location, "double", value)
        }
      case other => throw this.mismatch(config, location, "double", other)
    }
  }

  private def asDecimal(value: Any, config: CdcConfig, location: String): String = {
    value match {
      case decimal: java.math.BigDecimal => decimal.toPlainString
      case number: java.lang.Number => new java.math.BigDecimal(number.toString).toPlainString
      case text: String =>
        try new java.math.BigDecimal(text.trim).toPlainString
        catch {
          case _: NumberFormatException =>
            throw this.mismatch(config, location, "decimal", value)
        }
      case other => throw this.mismatch(config, location, "decimal", other)
    }
  }

  private def asDate(value: Any, config: CdcConfig, location: String): String = {
    value match {
      case date: java.util.Date => DateFormat.format(date.toInstant.atZone(ZoneOffset.UTC))
      case date: LocalDate => DateFormat.format(date)
      case number: java.lang.Number => DateFormat.format(LocalDate.ofEpochDay(number.longValue()))
      case text: String => text
      case other => throw this.mismatch(config, location, "date", other)
    }
  }

  private def asTimestamp(value: Any, config: CdcConfig, location: String): String = {
    value match {
      case timestamp: Timestamp => InstantFormat.format(timestamp.toInstant)
      case date: java.util.Date => InstantFormat.format(date.toInstant)
      case instant: Instant => InstantFormat.format(instant)
      case dateTime: LocalDateTime => LocalDateTimeFormat.format(dateTime)
      case dateTime: OffsetDateTime => DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dateTime)
      case dateTime: ZonedDateTime => DateTimeFormatter.ISO_ZONED_DATE_TIME.format(dateTime)
      case number: java.lang.Long =>
        // Debezium's connect time precision and Connect Timestamp use epoch milliseconds.
        InstantFormat.format(Instant.ofEpochMilli(number.longValue()))
      case text: String => text
      case other => throw this.mismatch(config, location, "timestamp", other)
    }
  }

  private def asBinary(value: Any, config: CdcConfig, location: String): String = {
    value match {
      case bytes: Array[Byte] => Base64.getEncoder.encodeToString(bytes)
      case buffer: ByteBuffer =>
        val bytes = new Array[Byte](buffer.remaining())
        buffer.duplicate().get(bytes)
        Base64.getEncoder.encodeToString(bytes)
      case text: String =>
        Base64.getEncoder.encodeToString(text.getBytes(StandardCharsets.UTF_8))
      case other => throw this.mismatch(config, location, "binary", other)
    }
  }

  private def mismatch(
      config: CdcConfig,
      location: String,
      expected: String,
      value: Any): PuddingException = {
    new PuddingException(
      s"${config.label} can not convert $location to $expected; got " +
        value.getClass.getSimpleName)
  }

  private def mongoValue(value: Any): Any = {
    value match {
      case null => null
      // MongoCollection[Document] decodes BSON BINARY, ObjectId and Decimal128 to the
      // org.bson.types classes, not to BsonValue. Handle them before the generic fallback so
      // binary keys and decimal values are encoded identically in snapshot, stream and delete
      // rows.
      case binary: org.bson.types.Binary => binary.getData
      case decimal: org.bson.types.Decimal128 => decimal.bigDecimalValue()
      case objectId: org.bson.types.ObjectId => objectId.toHexString
      case value: BsonValue => this.mongoBsonValue(value)
      case values: util.Map[_, _] =>
        val document = new Document()
        values.asScala.foreach { case (key, item) =>
          document.put(key.toString, this.mongoValue(item).asInstanceOf[AnyRef])
        }
        document
      case values: util.Collection[_] =>
        values.asScala.map(this.mongoValue).asJava
      case other => other
    }
  }

  private def mongoBsonValue(value: BsonValue): Any = {
    value match {
      case _: BsonNull | _: BsonUndefined => null
      case value: BsonObjectId => value.getValue.toHexString
      case value: BsonString => value.getValue
      case value: BsonNumber => this.mongoNumber(value)
      case value: BsonDateTime => new java.util.Date(value.getValue)
      case value: BsonTimestamp =>
        new java.util.Date(value.getTime.toLong * 1000L)
      case value: BsonBinary => value.getData
      case value: BsonRegularExpression => value.getPattern
      case value: BsonArray => value.getValues.asScala.map(this.mongoBsonValue).asJava
      case value: BsonDocument => new Document(value)
      case other => other.toString
    }
  }

  private def mongoNumber(value: BsonNumber): Any = {
    value.getBsonType match {
      case org.bson.BsonType.INT32 => value.intValue()
      case org.bson.BsonType.INT64 => value.longValue()
      case org.bson.BsonType.DOUBLE => value.doubleValue()
      case _ => value.decimal128Value().bigDecimalValue()
    }
  }
}
