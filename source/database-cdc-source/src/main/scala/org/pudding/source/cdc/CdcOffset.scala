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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.connector.read.streaming.Offset

import org.pudding.core.PuddingException

/**
 * Spark checkpoint offset for a native CDC stream.
 *
 * `data` contains only the provider cursor (a binlog position, an LSN or a change-stream resume
 * token). Credentials, connection settings and source schema are never written to the offset.
 * `sequence` makes offsets unique within one process even when a connector emits several rows
 * for the same database cursor.
 */
final class CdcOffset(
    val kind: String,
    val data: Map[String, Any],
    val sequence: Long) extends Offset with Serializable {

  if (kind == null || kind.trim.isEmpty) {
    throw new PuddingException("cdc offset kind can not be empty")
  }
  if (sequence < 0) {
    throw new PuddingException("cdc offset sequence can not be negative")
  }

  private val rendered: String = CdcOffset.render(this.kind, this.data, this.sequence)

  override def json(): String = this.rendered

  def javaData: java.util.Map[String, Object] = {
    this.data.map { case (key, value) => key -> CdcOffset.javaValue(value) }.asJava
  }

  override def equals(other: Any): Boolean = {
    other match {
      case value: CdcOffset =>
        value.kind == this.kind &&
          value.sequence == this.sequence &&
          value.data == this.data
      case _ => false
    }
  }

  override def hashCode(): Int = {
    (this.kind, this.sequence, this.data).hashCode()
  }

  override def toString: String = {
    s"${this.kind}@${this.sequence}"
  }
}

private[cdc] object CdcOffset {

  val Version = 1

  val InitialKind = "none"

  val DebeziumKind = "debezium"

  val MongodbKind = "mongodb"

  private val mapper = new ObjectMapper()

  def initial(): CdcOffset = new CdcOffset(InitialKind, Map.empty, 0L)

  def apply(kind: String, data: Map[String, Any], sequence: Long): CdcOffset = {
    new CdcOffset(kind, normalize(data), sequence)
  }

  def debezium(data: Map[String, Any], sequence: Long): CdcOffset = {
    this.apply(DebeziumKind, data, sequence)
  }

  def mongodb(data: Map[String, Any], sequence: Long): CdcOffset = {
    this.apply(MongodbKind, data, sequence)
  }

  def parse(json: String): CdcOffset = {
    if (json == null || json.trim.isEmpty) {
      throw new PuddingException("cdc checkpoint offset is empty")
    }
    try {
      val root = mapper.readTree(json)
      val version = this.requiredInt(root, "v")
      if (version != Version) {
        throw new PuddingException(
          s"unsupported cdc checkpoint offset version: $version")
      }
      val kind = this.requiredText(root, "kind")
      val sequence = this.requiredLong(root, "seq")
      val dataNode = root.get("data")
      if (dataNode == null || dataNode.isNull) {
        throw new PuddingException("cdc checkpoint offset is missing 'data'")
      }
      if (!dataNode.isObject) {
        throw new PuddingException("cdc checkpoint offset 'data' must be an object")
      }
      val data = dataNode.fields().asScala.map { entry =>
        entry.getKey -> this.scalaValue(entry.getValue)
      }.toMap
      this.apply(kind, data, sequence)
    } catch {
      case error: PuddingException => throw error
      case error: RuntimeException =>
        throw new PuddingException(
          s"invalid cdc checkpoint offset: ${error.getClass.getSimpleName}", error)
    }
  }

  private def render(kind: String, data: Map[String, Any], sequence: Long): String = {
    val root = mapper.createObjectNode()
    root.put("v", Version)
    root.put("kind", kind)
    root.put("seq", sequence)
    val dataNode = root.putObject("data")
    normalize(data).toSeq.sortBy(_._1).foreach { case (key, value) =>
      this.write(dataNode, key, value)
    }
    mapper.writeValueAsString(root)
  }

  private def write(node: com.fasterxml.jackson.databind.node.ObjectNode, key: String, value: Any): Unit = {
    value match {
      case null => node.putNull(key)
      case text: String => node.put(key, text)
      case bool: Boolean => node.put(key, bool)
      case number: Long => node.put(key, number)
      case number: Int => node.put(key, number)
      case number: BigInt => node.put(key, number.bigInteger)
      case number: BigDecimal => node.put(key, number.bigDecimal)
      case other =>
        throw new PuddingException(
          s"cdc offset value for '$key' has unsupported type: ${other.getClass.getSimpleName}")
    }
  }

  private def normalize(data: Map[String, Any]): Map[String, Any] = {
    data.map { case (key, value) => key -> normalizeValue(value) }
  }

  private def normalizeValue(value: Any): Any = {
    value match {
      case null => null
      case number: java.lang.Byte => number.longValue()
      case number: java.lang.Short => number.longValue()
      case number: java.lang.Integer => number.longValue()
      case number: java.lang.Long => number.longValue()
      case number: java.lang.Float => BigDecimal.decimal(number.toDouble).bigDecimal
      case number: java.lang.Double => BigDecimal.decimal(number).bigDecimal
      case number: java.math.BigInteger => number
      case number: java.math.BigDecimal => number
      case text: String => text
      case bool: java.lang.Boolean => bool.booleanValue()
      case other =>
        throw new PuddingException(
          s"cdc cursor contains an unsupported value type: ${other.getClass.getSimpleName}")
    }
  }

  private def javaValue(value: Any): Object = {
    value match {
      case null => null
      case number: Long => java.lang.Long.valueOf(number)
      case number: Int => java.lang.Long.valueOf(number.toLong)
      case number: BigInt => number.bigInteger
      case number: BigDecimal => number.bigDecimal
      case number: java.math.BigInteger => number
      case number: java.math.BigDecimal => number
      case text: String => text
      case bool: Boolean => java.lang.Boolean.valueOf(bool)
      case other =>
        throw new PuddingException(
          s"cdc cursor contains an unsupported value type: ${other.getClass.getSimpleName}")
    }
  }

  private def scalaValue(node: JsonNode): Any = {
    if (node == null || node.isNull) {
      null
    } else if (node.isTextual) {
      node.textValue()
    } else if (node.isBoolean) {
      node.booleanValue()
    } else if (node.isIntegralNumber) {
      node.longValue()
    } else if (node.isFloatingPointNumber) {
      node.decimalValue()
    } else {
      throw new PuddingException("cdc checkpoint offset contains a non-scalar value")
    }
  }

  private def requiredInt(root: JsonNode, key: String): Int = {
    val node = root.get(key)
    if (node == null || !node.canConvertToInt) {
      throw new PuddingException(s"cdc checkpoint offset is missing '$key'")
    }
    node.intValue()
  }

  private def requiredLong(root: JsonNode, key: String): Long = {
    val node = root.get(key)
    if (node == null || !node.canConvertToLong) {
      throw new PuddingException(s"cdc checkpoint offset is missing '$key'")
    }
    node.longValue()
  }

  private def requiredText(root: JsonNode, key: String): String = {
    val node = root.get(key)
    if (node == null || !node.isTextual || node.textValue().trim.isEmpty) {
      throw new PuddingException(s"cdc checkpoint offset is missing '$key'")
    }
    node.textValue()
  }
}
