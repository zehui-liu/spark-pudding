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

package org.pudding.kafka.schema

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.util.Locale

import scala.collection.JavaConverters._

import com.google.protobuf.{ByteString, Descriptors, DynamicMessage, Message}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Confluent Schema Registry decoder used by `kafka-source`.
 *
 * This class lives in an optional runtime so the connector jar does not pull Schema Registry,
 * Avro or Protobuf libraries into every Kafka job. The class is loaded by name through reflection
 * by the connector; its public constructor and `decode` method form the stable bridge.
 *
 * The configured Spark schema defines the output columns. Avro/Protobuf fields are matched by
 * name; missing fields are reported by name and values are never included in errors.
 */
class ConfluentValueDecoder(
    schema: StructType,
    config: java.util.Map[String, String],
    format: String) extends Serializable {

  @transient private var injectedClient: SchemaRegistryClient = _
  @transient private var avroDeserializer: KafkaAvroDeserializer = _
  @transient private var protobufDeserializer: KafkaProtobufDeserializer[Message] = _

  /**
   * Test seam. Production callers use the three-argument constructor and the runtime builds its
   * own client from `schema.registry.url` and the supplied properties.
   */
  def this(
      schema: StructType,
      config: java.util.Map[String, String],
      format: String,
      client: SchemaRegistryClient) = {
    this(schema, config, format)
    this.injectedClient = client
  }

  def decode(topic: String, value: Array[Byte]): Row = {
    if (value == null) {
      null
    } else {
      this.initialize()
      format.toLowerCase(Locale.ROOT) match {
        case "avro" =>
          this.avroValue(
            avroDeserializer.deserialize(topic, value),
            schema,
            null).asInstanceOf[Row]
        case "protobuf" =>
          val message = protobufDeserializer.deserialize(topic, value)
          this.protobufValue(message, schema).asInstanceOf[Row]
        case other =>
          throw new IllegalArgumentException(
            s"unsupported schema registry value format: $other")
      }
    }
  }

  private def initialize(): Unit = {
    if (avroDeserializer == null && protobufDeserializer == null) {
      this.synchronized {
        if (avroDeserializer == null && protobufDeserializer == null) {
          val properties = new java.util.HashMap[String, Object]()
          Option(config).foreach(_.asScala.foreach {
            case (key, value) => properties.put(key, value)
          })
          format.toLowerCase(Locale.ROOT) match {
            case "avro" =>
              val deserializer = if (injectedClient == null) {
                new KafkaAvroDeserializer()
              } else {
                new KafkaAvroDeserializer(injectedClient, properties)
              }
              if (injectedClient == null) {
                deserializer.configure(properties, false)
              }
              avroDeserializer = deserializer
            case "protobuf" =>
              val deserializer = if (injectedClient == null) {
                new KafkaProtobufDeserializer[Message]()
              } else {
                new KafkaProtobufDeserializer[Message](injectedClient, properties)
              }
              if (injectedClient == null) {
                deserializer.configure(properties, false)
              }
              protobufDeserializer = deserializer
            case other =>
              throw new IllegalArgumentException(
                s"unsupported schema registry value format: $other")
          }
        }
      }
    }
  }

  private def avroValue(
      value: Any,
      dataType: DataType,
      avroSchema: Schema): Any = {
    if (value == null) {
      return null
    }
    dataType match {
      case struct: StructType =>
        value match {
          case record: GenericRecord =>
            Row.fromSeq(struct.fields.map { field =>
              val fieldSchema = Option(record.getSchema.getField(field.name))
                .map(_.schema).orNull
              if (record.get(field.name) == null) {
                null
              } else {
                this.avroValue(record.get(field.name), field.dataType, fieldSchema)
              }
            })
          case other =>
            throw new IllegalArgumentException(
              s"Avro value for struct field is not a record: ${other.getClass.getSimpleName}")
        }
      case StringType => value.toString
      case IntegerType => this.integer(value)
      case LongType => this.longValue(value)
      case ShortType => this.integer(value).toShort
      case ByteType => this.integer(value).toByte
      case FloatType => value.asInstanceOf[Number].floatValue()
      case DoubleType => value.asInstanceOf[Number].doubleValue()
      case BooleanType => value.asInstanceOf[Boolean]
      case BinaryType => this.bytes(value)
      case decimal: DecimalType =>
        this.decimal(value, decimal.scale)
      case DateType =>
        value match {
          case number: Number =>
            Date.valueOf(LocalDate.ofEpochDay(number.longValue()))
          case date: Date => date
          case other =>
            throw new IllegalArgumentException(
              s"Avro value for date field has unsupported type: " +
                other.getClass.getSimpleName)
        }
      case TimestampType | TimestampNTZType =>
        this.timestamp(value, avroSchema)
      case array: ArrayType =>
        val values = value match {
          case collection: java.util.Collection[_] => collection.asScala
          case arrayValue: Array[_] => arrayValue.toSeq
          case other =>
            throw new IllegalArgumentException(
              s"Avro value for array field has unsupported type: " +
                other.getClass.getSimpleName)
        }
        values.map(item => this.avroValue(
          item, array.elementType, this.elementSchema(avroSchema))).toSeq
      case map: MapType =>
        value match {
          case values: java.util.Map[_, _] =>
            values.asScala.map { case (key, item) =>
              key.toString -> this.avroValue(
                item, map.valueType, this.elementSchema(avroSchema))
            }.toMap
          case other =>
            throw new IllegalArgumentException(
              s"Avro value for map field has unsupported type: " +
                other.getClass.getSimpleName)
        }
      case _ => value
    }
  }

  private def protobufValue(message: Message, dataType: DataType): Any = {
    if (message == null) {
      return null
    }
    dataType match {
      case struct: StructType =>
        val descriptor = message.getDescriptorForType
        Row.fromSeq(struct.fields.map { field =>
          val protobufField = descriptor.findFieldByName(field.name)
          if (protobufField == null) {
            throw new IllegalArgumentException(
              s"Protobuf schema is missing field: ${field.name}")
          }
          this.protobufField(message, protobufField, field.dataType)
        })
      case _ =>
        throw new IllegalArgumentException(
          s"Protobuf top-level value requires a struct schema, got ${dataType.typeName}")
    }
  }

  private def protobufField(
      message: Message,
      field: Descriptors.FieldDescriptor,
      dataType: DataType): Any = {
    if (field.isMapField) {
      val mapType = dataType.asInstanceOf[MapType]
      val keyField = field.getMessageType.getFields.get(0)
      val valueField = field.getMessageType.getFields.get(1)
      message.getField(field).asInstanceOf[java.util.List[_]].asScala.map { item =>
        val entry = item.asInstanceOf[Message]
        val key = entry.getField(keyField).toString
        key -> this.protobufScalar(entry.getField(valueField), valueField, mapType.valueType)
      }.toMap
    } else if (field.isRepeated) {
      val array = dataType.asInstanceOf[ArrayType]
      message.getField(field).asInstanceOf[java.util.List[_]].asScala
        .map(item => this.protobufScalar(item, field, array.elementType))
        .toSeq
    } else if (field.getJavaType == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
      !message.hasField(field)) {
      null
    } else {
      this.protobufScalar(message.getField(field), field, dataType)
    }
  }

  private def protobufScalar(
      value: Any,
      field: Descriptors.FieldDescriptor,
      dataType: DataType): Any = {
    if (value == null) {
      return null
    }
    if (field.getJavaType == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
      value match {
        case timestamp: Message
            if timestamp.getDescriptorForType.getFullName == "google.protobuf.Timestamp" =>
          val seconds = timestamp.getField(
            timestamp.getDescriptorForType.findFieldByName("seconds")).asInstanceOf[Long]
          val nanos = timestamp.getField(
            timestamp.getDescriptorForType.findFieldByName("nanos")).asInstanceOf[Int]
          Timestamp.from(Instant.ofEpochSecond(seconds, nanos.toLong))
        case message: Message =>
          this.protobufValue(message, dataType)
        case other =>
          throw new IllegalArgumentException(
            s"Protobuf message field has unsupported type: ${other.getClass.getSimpleName}")
      }
    } else {
      dataType match {
        case StringType =>
          field.getJavaType match {
            case Descriptors.FieldDescriptor.JavaType.ENUM => value.toString
            case _ => value.toString
          }
        case IntegerType => value.asInstanceOf[Number].intValue()
        case LongType => value.asInstanceOf[Number].longValue()
        case FloatType => value.asInstanceOf[Number].floatValue()
        case DoubleType => value.asInstanceOf[Number].doubleValue()
        case BooleanType => value.asInstanceOf[Boolean]
        case BinaryType => this.bytes(value)
        case decimal: DecimalType => this.decimal(value, decimal.scale)
        case TimestampType | TimestampNTZType =>
          val instant = Instant.ofEpochMilli(value.asInstanceOf[Number].longValue())
          Timestamp.from(instant)
        case _ => value
      }
    }
  }

  private def integer(value: Any): Int = value match {
    case number: Number => number.intValue()
    case text: String => text.toInt
    case other =>
      throw new IllegalArgumentException(
        s"Avro value for integer field has unsupported type: ${other.getClass.getSimpleName}")
  }

  private def longValue(value: Any): Long = value match {
    case number: Number => number.longValue()
    case text: String => text.toLong
    case other =>
      throw new IllegalArgumentException(
        s"Avro value for long field has unsupported type: ${other.getClass.getSimpleName}")
  }

  private def decimal(value: Any, scale: Int): BigDecimal = value match {
    case decimal: BigDecimal => decimal.setScale(scale)
    case number: Number => new BigDecimal(number.toString).setScale(scale)
    case text: String => new BigDecimal(text).setScale(scale)
    case buffer: ByteBuffer =>
      val duplicate = buffer.duplicate()
      val bytes = new Array[Byte](duplicate.remaining())
      duplicate.get(bytes)
      new BigDecimal(new java.math.BigInteger(bytes), scale)
    case other =>
      throw new IllegalArgumentException(
        s"Avro value for decimal field has unsupported type: ${other.getClass.getSimpleName}")
  }

  private def timestamp(value: Any, avroSchema: Schema): Timestamp = value match {
    case timestamp: Timestamp => timestamp
    case date: Date => new Timestamp(date.getTime)
    case number: Number =>
      val logical = this.logicalType(avroSchema)
      val millis = logical match {
        case "timestamp-micros" => number.longValue() / 1000L
        case _ => number.longValue()
      }
      Timestamp.from(Instant.ofEpochMilli(millis))
    case text: String =>
      Timestamp.from(LocalDateTime.parse(text.replace(' ', 'T')).toInstant(ZoneOffset.UTC))
    case other =>
      throw new IllegalArgumentException(
        s"Avro value for timestamp field has unsupported type: " +
          other.getClass.getSimpleName)
  }

  private def bytes(value: Any): Array[Byte] = value match {
    case buffer: ByteBuffer =>
      val duplicate = buffer.duplicate()
      val bytes = new Array[Byte](duplicate.remaining())
      duplicate.get(bytes)
      bytes
    case bytes: Array[Byte] => bytes
    case byteString: ByteString => byteString.toByteArray
    case other =>
      throw new IllegalArgumentException(
        s"Avro/Protobuf value for binary field has unsupported type: " +
          other.getClass.getSimpleName)
  }

  private def logicalType(schema: Schema): String = {
    if (schema == null) {
      ""
    } else if (schema.getType == Schema.Type.UNION) {
      schema.getTypes.asScala
        .find(_.getType != Schema.Type.NULL)
        .flatMap(value => Option(value.getLogicalType).map(_.getName))
        .getOrElse("")
    } else {
      Option(schema.getLogicalType).map(_.getName).getOrElse("")
    }
  }

  private def elementSchema(schema: Schema): Schema = {
    if (schema == null) {
      null
    } else if (schema.getType == Schema.Type.UNION) {
      schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).orNull
    } else if (schema.getType == Schema.Type.ARRAY) {
      schema.getElementType
    } else if (schema.getType == Schema.Type.MAP) {
      schema.getValueType
    } else {
      schema
    }
  }
}
