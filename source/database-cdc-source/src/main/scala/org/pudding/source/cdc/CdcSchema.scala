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

package org.pudding.source.cdc

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import org.pudding.core.PuddingException

/**
 * Shared row contract for native database CDC sources.
 *
 * Users declare the source columns explicitly. Spark Structured Streaming requires a stable
 * schema across micro-batches, so schema discovery is deliberately not implicit: a column added
 * or renamed at the database must be reflected in the job configuration and restarted rather
 * than changing the checkpoint stream under a running query.
 */
private[cdc] object CdcSchema {

  val SchemaKey = "schema"

  val OpColumn = "__op"

  val TimestampColumn = "_cdc_timestamp"

  val TableColumn = "_cdc_table"

  val ValueColumn = "value"

  val SequenceColumn: String = org.pudding.core.incremental.StreamingCdc.CdcSequenceColumn

  private val InternalPrefixes = Seq("_cdc_", "_kafka_", "_pudding_")

  private val InternalNames = Set(OpColumn, TimestampColumn, TableColumn)

  /**
   * Parse the `schema` config entry. A DDL string such as
   * `id bigint, name string, amount decimal(12, 2)` and a map of Spark SQL types are both
   * accepted.
   */
  def parse(config: Map[String, Any]): StructType = {
    val schema = config.get(SchemaKey).filter(_ != null) match {
      case Some(text: String) if text.trim.nonEmpty =>
        val ddl = text.trim
        if (ddl.toLowerCase(Locale.ROOT).startsWith("struct<")) {
          DataType.fromDDL(ddl) match {
            case value: StructType => value
            case _ =>
              throw new PuddingException(s"$SchemaKey must describe a struct")
          }
        } else {
          StructType.fromDDL(ddl)
        }
      case Some(values: Map[_, _]) =>
        this.fromMap(values.map { case (name, kind) => name -> kind })
      case Some(values: java.util.Map[_, _]) =>
        this.fromMap(values.asScala.toMap.map { case (name, kind) => name -> kind })
      case Some(_) =>
        throw new PuddingException(s"$SchemaKey must be a map or a DDL string")
      case None =>
        throw new PuddingException(
          s"can not find param: $SchemaKey; native CDC requires an explicit source column schema")
    }
    this.validate(schema)
  }

  /**
   * Parse the schema carried in the Spark data-source options. The struct is serialized as
   * Spark's schema JSON by the Pudding adapter.
   */
  def fromJson(json: String): StructType = {
    if (json == null || json.trim.isEmpty) {
      throw new PuddingException("pudding.cdc.schema can not be empty")
    }
    DataType.fromJson(json) match {
      case value: StructType => this.validate(value)
      case _ =>
        throw new PuddingException("pudding.cdc.schema must describe a struct")
    }
  }

  def validate(schema: StructType): StructType = {
    val names = schema.fieldNames
    val duplicated = names.groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }.toSeq.sorted
    if (duplicated.nonEmpty) {
      throw new PuddingException(
        s"$SchemaKey contains duplicate columns: ${duplicated.mkString(", ")}")
    }
    schema.fields.foreach { field =>
      this.validateName(field.name)
      this.validateType(field.dataType, field.name)
    }
    StructType(schema.fields.map(field => field.copy(nullable = true)))
  }

  private def validateType(dataType: DataType, path: String): Unit = {
    dataType match {
      case map: org.apache.spark.sql.types.MapType =>
        if (map.keyType != StringType) {
          throw new PuddingException(
            s"$SchemaKey column '$path' must use string map keys for JSON CDC rows")
        }
        this.validateType(map.valueType, s"$path.*")
      case array: org.apache.spark.sql.types.ArrayType =>
        this.validateType(array.elementType, s"$path[]")
      case struct: StructType =>
        struct.fields.foreach { field =>
          this.validateType(field.dataType, s"$path.${field.name}")
        }
      case _ => ()
    }
  }

  def envelope(schema: StructType, opColumn: String): StructType = {
    this.validateOpColumn(opColumn)
    if (schema.fieldNames.contains(opColumn)) {
      throw new PuddingException(
        s"$SchemaKey contains the operation column: $opColumn")
    }
    StructType(schema.fields ++ Seq(
      StructField(opColumn, StringType, nullable = false),
      StructField(TimestampColumn, org.apache.spark.sql.types.LongType, nullable = false),
      StructField(SequenceColumn, org.apache.spark.sql.types.LongType, nullable = false),
      StructField(TableColumn, StringType, nullable = false)))
  }

  def validateOpColumn(opColumn: String): Unit = {
    if (opColumn == null || opColumn.trim.isEmpty) {
      throw new PuddingException("opColumn can not be empty")
    }
    if (this.isInternalName(opColumn) && opColumn != OpColumn) {
      throw new PuddingException(
        s"opColumn can not use a reserved CDC column: $opColumn")
    }
  }

  def isInternalName(name: String): Boolean = {
    name != null && (
      InternalNames.contains(name) ||
        name == OpColumn ||
        InternalPrefixes.exists(name.startsWith))
  }

  private def fromMap(values: Map[Any, Any]): StructType = {
    if (values.isEmpty) {
      throw new PuddingException(s"$SchemaKey can not be empty")
    }
    val fields = values.toSeq.map { case (name, kind) =>
      if (name == null || kind == null) {
        throw new PuddingException(s"$SchemaKey can not contain a null column or type")
      }
      val fieldName = name.toString.trim
      val fieldType = kind.toString.trim
      if (fieldName.isEmpty || fieldType.isEmpty) {
        throw new PuddingException(s"$SchemaKey can not contain an empty column or type")
      }
      StructField(fieldName, this.dataType(fieldType, fieldName), nullable = true)
    }
    StructType(fields)
  }

  private def dataType(value: String, field: String): DataType = {
    try {
      DataType.fromDDL(value)
    } catch {
      case error: RuntimeException =>
        throw new PuddingException(
          s"$SchemaKey column '$field' has an unsupported type '${sanitizeType(value)}'",
          error)
    }
  }

  /**
   * Type names are not secrets, but keep the diagnostic bounded so a malformed value can not
   * print an arbitrarily large configuration fragment.
   */
  private def sanitizeType(value: String): String = {
    if (value.length <= 80) value else value.substring(0, 80) + "..."
  }

  private def validateName(name: String): Unit = {
    if (name == null || name.trim.isEmpty) {
      throw new PuddingException(s"$SchemaKey can not contain an empty column")
    }
    if (this.isInternalName(name)) {
      throw new PuddingException(
        s"$SchemaKey contains reserved CDC column: $name")
    }
  }
}
