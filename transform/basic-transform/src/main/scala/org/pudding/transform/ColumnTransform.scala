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

package org.pudding.transform

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.DataType

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Selects, renames and casts columns in declaration order.
 *
 * {{{
 * config:
 *   fields:
 *     - order_id
 *     - name: user_name
 *       source: name
 *     - name: amount
 *       type: string
 *   numPartitions: 10
 * }}}
 */
class ColumnTransform extends DataTransform {

  private val FieldsKey = "fields"

  private val NameKey = "name"

  private val SourceKey = "source"

  private val TypeKey = "type"

  private val AllowedFieldKeys = Set(NameKey, SourceKey, TypeKey)

  override def identifier: String = "column-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def preservesIncrementalMetadata: Boolean = true

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val dataFrame = TransformSupport.single(dataFrames, identifier)
    val fields = this.parseFields(cfg.get(FieldsKey).filter(_ != null).getOrElse {
      throw new PuddingException(s"can not find param: $FieldsKey, $FieldsKey must be exist!")
    })

    val projections = fields.map { field =>
      val source = this.resolveColumn(dataFrame, field.source, field.name)
      val column = field.dataType match {
        case Some(dataType) =>
          this.validateDataType(dataType, field.name)
          functions.col(source).cast(dataType)
        case None => functions.col(source)
      }
      column.as(field.name)
    }

    val selected = dataFrame.select(projections: _*)
    TransformSupport.applyNumPartitions(selected, cfg)
  }

  private def parseFields(value: Any): Seq[FieldSpec] = {
    val fields = value match {
      case seq: Seq[_] => seq.map(item => this.parseField(item)).toSeq
      case list: java.util.List[_] => list.asScala.map(item => this.parseField(item)).toSeq
      case _ =>
        throw new PuddingException(
          s"$FieldsKey must be a list of field names or field maps!")
    }

    if (fields.isEmpty) {
      throw new PuddingException(s"$FieldsKey can not be empty!")
    }

    val duplicated = fields.map(_.name.toLowerCase(Locale.ROOT)).groupBy(identity).collect {
      case (name, occurrences) if occurrences.size > 1 => name
    }.toSeq.sorted
    if (duplicated.nonEmpty) {
      throw new PuddingException(
        s"$FieldsKey contains duplicate output names: ${duplicated.mkString(", ")}")
    }
    fields
  }

  private def parseField(value: Any): FieldSpec = value match {
    case text: String if text.trim.nonEmpty =>
      FieldSpec(text.trim, text.trim, None)
    case _: String =>
      throw new PuddingException(s"$FieldsKey field name can not be empty!")
    case map: Map[_, _] =>
      this.parseFieldMap(map.map { case (key, item) => key.toString -> item }.toMap)
    case map: java.util.Map[_, _] =>
      this.parseFieldMap(map.asScala.map { case (key, item) => key.toString -> item }.toMap)
    case _ =>
      throw new PuddingException(
        s"$FieldsKey entries must be strings or maps with name, source and type!")
  }

  private def parseFieldMap(values: Map[String, Any]): FieldSpec = {
    val unknown = values.keySet.diff(AllowedFieldKeys)
    if (unknown.nonEmpty) {
      throw new PuddingException(
        s"$FieldsKey contains unknown field properties: ${unknown.toSeq.sorted.mkString(", ")}")
    }
    val name = values.get(NameKey)
      .map(_ => TransformSupport.requiredString(values, NameKey))
      .getOrElse(throw new PuddingException(s"$FieldsKey fields must contain $NameKey!"))
    val source = values.get(SourceKey)
      .map(_ => TransformSupport.requiredString(values, SourceKey))
      .getOrElse(name)
    val dataType = values.get(TypeKey)
      .map(_ => TransformSupport.requiredString(values, TypeKey))
    FieldSpec(name, source, dataType)
  }

  private def resolveColumn(
      dataFrame: DataFrame,
      source: String,
      fieldName: String): String = {
    val matches = dataFrame.columns.filter(_.equalsIgnoreCase(source))
    if (matches.isEmpty) {
      throw new PuddingException(
        s"$FieldsKey field '$fieldName' source column does not exist: $source")
    }
    if (matches.length > 1) {
      throw new PuddingException(
        s"$FieldsKey field '$fieldName' source column is ambiguous: $source")
    }
    matches.head
  }

  private def validateDataType(dataType: String, fieldName: String): Unit = {
    try {
      DataType.fromDDL(dataType)
    } catch {
      case _: Exception =>
        throw new PuddingException(
          s"$FieldsKey field '$fieldName' has an invalid type: $dataType")
    }
  }
}

private[transform] case class FieldSpec(
  name: String,
  source: String,
  dataType: Option[String])
