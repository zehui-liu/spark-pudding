/*
 * Copyright 2024-2026 the spark-pudding authors
 *
 * Licensed under the Apache License, Version 2.0
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

import java.math.{BigDecimal => JBigDecimal, BigInteger, RoundingMode}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.util.Base64

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructType,
  TimestampNTZType,
  TimestampType
}

import org.pudding.core.PuddingException

/**
 * Decodes the JSON payloads Hudi writes for CDC `before` and `after` images.
 *
 * Hudi uses two encodings in the same CDC relation:
 *  - CDC log images use Avro's JSON representation without union type tags. Logical values are
 *    base Avro values: DATE is an epoch-day integer, TIMESTAMP is an epoch microsecond long,
 *    DECIMAL is a fixed/bytes array or an ISO-8859-1 byte string, and BYTES is an ISO-8859-1
 *    string;
 *  - base-file and replace-commit images use `InternalRowToJsonStringConverter` (Jackson).
 *    DECIMAL is serialized as a lossy `{"zero":...}` object, BYTES as base64, STRUCT as
 *    `{"values":[...]}`, ARRAY as `{"array":[...]}` and MAP as key/value arrays.
 *
 * The decoder detects the encoding per image. It fails closed when a value can not be restored,
 * notably DECIMAL in the Jackson encoding, instead of returning a silently wrong value.
 */
private[source] object HudiCdcJson {

  private val mapper = new ObjectMapper()
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)

  private val MetadataFieldPrefix = "_hoodie_"

  /**
   * Decode one image into an external Spark row.
   *
   * @param json               Hudi image JSON; `null` means that the image is absent
   * @param avroSchema         table Avro schema describing the image
   * @param schema             Spark schema of the image
   * @param populateMetaFields whether the table writes `_hoodie_` metadata fields
   * @return external row, or `null` when the image is absent
   */
  def decodeRecord(
      json: String,
      avroSchema: Schema,
      schema: StructType,
      populateMetaFields: Boolean): Row = {
    if (json == null) {
      return null
    }
    val node = mapper.readTree(json)
    if (node == null || node.isNull) {
      return null
    }
    val jackson = this.isJacksonPayload(node, schema)
    // Without metadata fields an Avro raw-bytes string and a Jackson base64 string are
    // indistinguishable. Reject binary fields instead of guessing.
    val ambiguousBinary = !populateMetaFields && !jackson
    Row.fromSeq(schema.fields.map { field =>
      val fieldSchema = Option(avroSchema.getField(field.name)).map(_.schema()).orNull
      this.decodeValue(
        node.get(field.name),
        fieldSchema,
        field.dataType,
        jackson,
        ambiguousBinary,
        field.name)
    })
  }

  private def isJacksonPayload(node: JsonNode, schema: StructType): Boolean = {
    if (node.isObject && node.fieldNames().asScala.exists(_.startsWith(MetadataFieldPrefix))) {
      true
    } else {
      schema.fields.exists(field =>
        this.hasJacksonShape(node.get(field.name), field.dataType))
    }
  }

  private def hasJacksonShape(node: JsonNode, dataType: DataType): Boolean = {
    if (node == null || node.isNull) {
      false
    } else {
      dataType match {
        case _: DecimalType => node.isObject
        case structType: StructType =>
          (node.isObject && node.has("values") && node.get("values").isArray) ||
            (node.isObject && structType.fields.exists(field =>
              this.hasJacksonShape(node.get(field.name), field.dataType)))
        case arrayType: ArrayType =>
          (node.isObject && node.has("array") && node.get("array").isArray) ||
            (node.isArray && node.elements().asScala.exists(element =>
              this.hasJacksonShape(element, arrayType.elementType)))
        case mapType: MapType =>
          (node.isObject && node.has("keyArray") && node.has("valueArray")) ||
            (node.isObject && node.fields().asScala.exists(entry =>
              this.hasJacksonShape(entry.getValue, mapType.valueType)))
        case _ => false
      }
    }
  }

  private def decodeValue(
      node: JsonNode,
      avroSchema: Schema,
      dataType: DataType,
      jackson: Boolean,
      ambiguousBinary: Boolean,
      path: String): Any = {
    if (node == null || node.isNull) {
      return null
    }
    dataType match {
      case StringType => this.decodeString(node, jackson, path)
      case BooleanType => node.booleanValue()
      case ByteType => node.intValue().toByte
      case ShortType => node.intValue().toShort
      case IntegerType => node.intValue()
      case LongType => node.longValue()
      case FloatType => node.floatValue()
      case DoubleType => node.doubleValue()
      case DateType => this.decodeDate(node)
      case TimestampType =>
        if (jackson) {
          this.decodeTimestampMicros(node)
        } else {
          this.decodeTimestamp(node, avroSchema)
        }
      case TimestampNTZType =>
        if (jackson) {
          this.decodeLocalTimestampMicros(node)
        } else {
          this.decodeLocalTimestamp(node, avroSchema)
        }
      case decimalType: DecimalType =>
        if (jackson) {
          this.decodeJacksonDecimal(node, decimalType, path)
        } else {
          this.decodeAvroDecimal(node, avroSchema, decimalType, path)
        }
      case BinaryType =>
        if (jackson) {
          this.decodeJacksonBytes(node, path)
        } else {
          this.decodeAvroBytes(node, path, ambiguousBinary)
        }
      case arrayType: ArrayType =>
        this.decodeArray(
          node, avroSchema, arrayType, jackson, ambiguousBinary, path)
      case mapType: MapType =>
        this.decodeMap(
          node, avroSchema, mapType, jackson, ambiguousBinary, path)
      case structType: StructType =>
        this.decodeStruct(
          node, avroSchema, structType, jackson, ambiguousBinary, path)
      case _ =>
        if (jackson && this.isUtf8Bean(node)) {
          this.decodeUtf8Bean(node, path)
        } else {
          node.asText()
        }
    }
  }

  private def decodeString(node: JsonNode, jackson: Boolean, path: String): String = {
    if (jackson && this.isUtf8Bean(node)) {
      this.decodeUtf8Bean(node, path)
    } else {
      node.asText()
    }
  }

  private def decodeDate(node: JsonNode): Date = {
    if (node.isTextual) {
      Date.valueOf(LocalDate.parse(node.textValue()))
    } else {
      Date.valueOf(LocalDate.ofEpochDay(node.longValue()))
    }
  }

  private def decodeTimestamp(node: JsonNode, avroSchema: Schema): Timestamp = {
    if (node.isTextual) {
      this.parseTimestamp(node.textValue())
    } else {
      this.timestampFromMicros(this.epochMicros(node.longValue(), avroSchema))
    }
  }

  private def decodeTimestampMicros(node: JsonNode): Timestamp = {
    if (node.isTextual) {
      this.parseTimestamp(node.textValue())
    } else {
      this.timestampFromMicros(node.longValue())
    }
  }

  private def parseTimestamp(text: String): Timestamp = {
    try {
      Timestamp.valueOf(text)
    } catch {
      case _: IllegalArgumentException => Timestamp.from(Instant.parse(text))
    }
  }

  private def timestampFromMicros(micros: Long): Timestamp = {
    Timestamp.from(Instant.ofEpochSecond(
      Math.floorDiv(micros, MicrosPerSecond),
      Math.floorMod(micros, MicrosPerSecond) * NanosPerMicro))
  }

  private def decodeLocalTimestamp(node: JsonNode, avroSchema: Schema): LocalDateTime = {
    if (node.isTextual) {
      LocalDateTime.parse(node.textValue())
    } else {
      this.localDateTimeFromMicros(this.epochMicros(node.longValue(), avroSchema))
    }
  }

  private def decodeLocalTimestampMicros(node: JsonNode): LocalDateTime = {
    if (node.isTextual) {
      LocalDateTime.parse(node.textValue())
    } else {
      this.localDateTimeFromMicros(node.longValue())
    }
  }

  private def localDateTimeFromMicros(micros: Long): LocalDateTime = {
    LocalDateTime.ofEpochSecond(
      Math.floorDiv(micros, MicrosPerSecond),
      (Math.floorMod(micros, MicrosPerSecond) * NanosPerMicro).toInt,
      ZoneOffset.UTC)
  }

  private def epochMicros(value: Long, avroSchema: Schema): Long = {
    val logicalName = Option(this.unwrap(avroSchema))
      .flatMap(schema => Option(schema.getLogicalType))
      .map(_.getName)
      .orNull
    logicalName match {
      case "timestamp-millis" | "local-timestamp-millis" => value * 1000L
      case _ => value
    }
  }

  private def decodeJacksonDecimal(
      node: JsonNode,
      decimalType: DecimalType,
      path: String): JBigDecimal = {
    if (node.isNumber) {
      node.decimalValue().setScale(decimalType.scale, RoundingMode.HALF_UP)
    } else if (node.isTextual) {
      try {
        new JBigDecimal(node.textValue()).setScale(decimalType.scale, RoundingMode.HALF_UP)
      } catch {
        case _: NumberFormatException => throw this.lossyJacksonDecimal(path)
      }
    } else {
      throw this.lossyJacksonDecimal(path)
    }
  }

  private def lossyJacksonDecimal(path: String): PuddingException = {
    new PuddingException(
      s"hudi cdc image field '$path' is a DECIMAL encoded by Hudi's InternalRow JSON, " +
        "which only stores a lossy zero/non-zero marker; the value can not be recovered. " +
        "Use a CDC source that emits CDC log images, or read the current snapshot instead")
  }

  private def decodeAvroDecimal(
      node: JsonNode,
      avroSchema: Schema,
      decimalType: DecimalType,
      path: String): JBigDecimal = {
    val scale = Option(this.unwrap(avroSchema))
      .flatMap(schema => Option(schema.getLogicalType))
      .collect { case decimal: LogicalTypes.Decimal => decimal.getScale }
      .getOrElse(decimalType.scale)
    val value = if (node.isArray) {
      new JBigDecimal(this.unscaledBytes(node, path), scale)
    } else if (node.isTextual) {
      // Avro JSON encodes bytes/fixed as an ISO-8859-1 string. It is never a JSON number,
      // even when the unscaled bytes happen to look like ASCII digits.
      new JBigDecimal(
        this.unscaledBytes(
          node.textValue().getBytes(StandardCharsets.ISO_8859_1), path),
        scale)
    } else if (node.isNumber) {
      node.decimalValue()
    } else {
      throw new PuddingException(
        s"hudi cdc decimal image field '$path' is not a JSON number, byte array or string")
    }
    value.setScale(decimalType.scale, RoundingMode.HALF_UP)
  }

  private def unscaledBytes(node: JsonNode, path: String): BigInteger = {
    this.unscaledBytes(this.toBytes(node), path)
  }

  private def unscaledBytes(bytes: Array[Byte], path: String): BigInteger = {
    if (bytes.isEmpty) {
      throw new PuddingException(
        s"hudi cdc decimal image field '$path' has an empty unscaled value")
    }
    new BigInteger(bytes)
  }

  private def decodeJacksonBytes(node: JsonNode, path: String): Array[Byte] = {
    if (node.isArray) {
      this.toBytes(node)
    } else if (node.isTextual) {
      this.decodeBase64(node.textValue(), path)
    } else if (node.isObject && node.has("bytes") && node.get("bytes").isTextual) {
      this.decodeBase64(node.get("bytes").asText(), path)
    } else {
      throw new PuddingException(
        s"hudi cdc binary image field '$path' has an unsupported JSON shape")
    }
  }

  private def decodeBase64(text: String, path: String): Array[Byte] = {
    try {
      Base64.getDecoder.decode(text)
    } catch {
      case _: IllegalArgumentException =>
        throw new PuddingException(
          s"hudi cdc binary image field '$path' is not valid base64")
    }
  }

  private def decodeAvroBytes(
      node: JsonNode,
      path: String,
      ambiguous: Boolean): Array[Byte] = {
    if (ambiguous) {
      throw new PuddingException(
        s"hudi cdc binary image field '$path' has an ambiguous encoding: the table does not " +
          "populate _hoodie_ metadata fields, so Avro JSON bytes and InternalRow JSON base64 " +
          "can not be distinguished; enable hoodie.populate.meta.fields")
    }
    if (node.isBinary) {
      node.binaryValue()
    } else if (node.isArray) {
      this.toBytes(node)
    } else if (node.isTextual) {
      // Avro writes each byte as the character with the same ISO-8859-1 code.
      node.textValue().getBytes(StandardCharsets.ISO_8859_1)
    } else {
      throw new PuddingException(
        s"hudi cdc binary image field '$path' is not a JSON string or byte array")
    }
  }

  private def decodeArray(
      node: JsonNode,
      avroSchema: Schema,
      arrayType: ArrayType,
      jackson: Boolean,
      ambiguousBinary: Boolean,
      path: String): Seq[Any] = {
    val elements = if (jackson && node.isObject && node.has("array")) {
      val values = node.get("array")
      if (!values.isArray) {
        throw new PuddingException(
          s"hudi cdc array image field '$path' has an invalid JSON shape")
      }
      values
    } else {
      node
    }
    if (!elements.isArray) {
      throw new PuddingException(
        s"hudi cdc array image field '$path' is not a JSON array")
    }
    val elementSchema = Option(this.unwrap(avroSchema)).map(_.getElementType).orNull
    elements.elements().asScala.zipWithIndex.map { case (element, index) =>
      this.decodeValue(
        element,
        if (jackson) null else elementSchema,
        arrayType.elementType,
        jackson,
        ambiguousBinary,
        s"$path[$index]")
    }.toSeq
  }

  private def decodeMap(
      node: JsonNode,
      avroSchema: Schema,
      mapType: MapType,
      jackson: Boolean,
      ambiguousBinary: Boolean,
      path: String): Map[String, Any] = {
    if (jackson && node.isObject && node.has("keyArray") && node.has("valueArray")) {
      val keys = node.get("keyArray").path("array")
      val values = node.get("valueArray").path("array")
      if (!keys.isArray || !values.isArray) {
        throw new PuddingException(
          s"hudi cdc map image field '$path' has an invalid key/value array shape")
      }
      val keyNodes = keys.elements().asScala.toSeq
      val valueNodes = values.elements().asScala.toSeq
      if (keyNodes.size != valueNodes.size) {
        throw new PuddingException(
          s"hudi cdc map image field '$path' has different key and value counts")
      }
      keyNodes.zip(valueNodes).map { case (keyNode, valueNode) =>
        val key = this.decodeValue(
          keyNode, null, StringType, jackson = true, ambiguousBinary = false,
          s"$path<key>").asInstanceOf[String]
        key -> this.decodeValue(
          valueNode, null, mapType.valueType, jackson = true, ambiguousBinary = false,
          s"$path[$key]")
      }.toMap
    } else {
      if (!node.isObject) {
        throw new PuddingException(
          s"hudi cdc map image field '$path' is not a JSON object")
      }
      val valueSchema = Option(this.unwrap(avroSchema)).map(_.getValueType).orNull
      node.fields().asScala.map { entry =>
        entry.getKey -> this.decodeValue(
          entry.getValue,
          valueSchema,
          mapType.valueType,
          jackson,
          ambiguousBinary,
          s"$path.${entry.getKey}")
      }.toMap
    }
  }

  private def decodeStruct(
      node: JsonNode,
      avroSchema: Schema,
      structType: StructType,
      jackson: Boolean,
      ambiguousBinary: Boolean,
      path: String): Row = {
    if (jackson && node.isObject && node.has("values") && node.get("values").isArray) {
      val values = node.get("values")
      Row.fromSeq(structType.fields.zipWithIndex.map { case (field, index) =>
        val child = if (index < values.size()) values.get(index) else null
        this.decodeValue(
          child,
          null,
          field.dataType,
          jackson = true,
          ambiguousBinary = false,
          s"$path.${field.name}")
      })
    } else {
      if (!node.isObject) {
        throw new PuddingException(
          s"hudi cdc struct image field '$path' is not a JSON object")
      }
      val recordSchema = this.unwrap(avroSchema)
      Row.fromSeq(structType.fields.map { field =>
        val fieldSchema = Option(recordSchema)
          .flatMap(schema => Option(schema.getField(field.name)))
          .map(_.schema())
          .orNull
        this.decodeValue(
          node.get(field.name),
          fieldSchema,
          field.dataType,
          jackson,
          ambiguousBinary,
          s"$path.${field.name}")
      })
    }
  }

  private def isUtf8Bean(node: JsonNode): Boolean = {
    node.isObject && node.has("bytes") && node.get("bytes").isTextual &&
      (node.has("baseObject") || node.has("byteBuffer") || node.has("baseOffset"))
  }

  private def decodeUtf8Bean(node: JsonNode, path: String): String = {
    try {
      new String(
        Base64.getDecoder.decode(node.get("bytes").asText()),
        StandardCharsets.UTF_8)
    } catch {
      case _: IllegalArgumentException =>
        throw new PuddingException(
          s"hudi cdc string image field '$path' has invalid base64 UTF8String bytes")
    }
  }

  private def toBytes(node: JsonNode): Array[Byte] = {
    node.elements().asScala.map(_.intValue().toByte).toArray
  }

  private def unwrap(avroSchema: Schema): Schema = {
    if (avroSchema == null) {
      null
    } else if (avroSchema.getType == Schema.Type.UNION) {
      avroSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL).orNull
    } else {
      avroSchema
    }
  }

  private val MicrosPerSecond = 1000000L

  private val NanosPerMicro = 1000L
}

/**
 * Serializable UDF entry point for [[HudiCdcJson]].
 *
 * Using a Catalyst UDF instead of an RDD `mapPartitions` closure keeps the decoder out of
 * Spark's closure cleaner, which can not always read the capturing class resource from a
 * dynamically loaded test or application classloader.
 */
private[source] final class HudiCdcImageDecoder(
    schemaJson: String,
    schema: StructType,
    populateMetaFields: Boolean)
  extends UDF3[String, String, String, Row] with Serializable {

  @transient private var avroSchema: Schema = _

  override def call(op: String, before: String, after: String): Row = {
    val image = if (op != null && op.equalsIgnoreCase("d")) before else after
    HudiCdcJson.decodeRecord(
      image, this.parsedSchema, schema, populateMetaFields)
  }

  private def parsedSchema: Schema = synchronized {
    if (avroSchema == null) {
      avroSchema = new Schema.Parser().parse(schemaJson)
    }
    avroSchema
  }
}
