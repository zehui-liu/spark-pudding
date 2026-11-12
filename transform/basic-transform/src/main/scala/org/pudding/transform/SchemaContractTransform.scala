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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Fails when the input schema does not match a declared contract.
 *
 * {{{
 * config:
 *   columns:
 *     id: long
 *     name: string
 *     amount: decimal(12,2)
 *   allowExtraColumns: false
 * }}}
 *
 * The transform does not change the DataFrame. It is useful immediately after a source to stop
 * a streaming query before schema drift reaches a sink. Nullability is not part of the contract;
 * column names are compared case-insensitively.
 */
class SchemaContractTransform extends DataTransform {

  private val ColumnsKey = "columns"

  private val AllowExtraColumnsKey = "allowExtraColumns"

  override def identifier: String = "schema-contract-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def preservesIncrementalMetadata: Boolean = true

  override def validateConfig(
      config: Option[Map[String, Any]],
      executionMode: ExecutionMode): Unit = {
    val cfg = TransformSupport.requiredConfig(config)
    this.expected(cfg)
    TransformSupport.optionalBoolean(cfg, AllowExtraColumnsKey)
  }

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val dataFrame = TransformSupport.single(dataFrames, identifier)
    val expected = this.expected(cfg).map { case (name, dataType) =>
      (name.toLowerCase(Locale.ROOT), (name, dataType))
    }.toMap
    val actual = dataFrame.schema.fields.map { field =>
      field.name.toLowerCase(Locale.ROOT) -> field
    }.toMap
    val allowExtra = TransformSupport.optionalBoolean(cfg, AllowExtraColumnsKey).getOrElse(false)

    val missing = expected.keySet.diff(actual.keySet).toSeq.sorted
    val extra = actual.keySet.diff(expected.keySet).toSeq.sorted
    val mismatched = expected.keySet.intersect(actual.keySet).toSeq.sorted.flatMap { key =>
      val (name, expectedType) = expected(key)
      val field = actual(key)
      if (field.dataType == expectedType) None
      else Some(s"$name (expected ${expectedType.simpleString}, got " +
        s"${field.dataType.simpleString})")
    }
    val errors = Seq.newBuilder[String]
    if (missing.nonEmpty) {
      errors += s"missing columns: ${missing.mkString(", ")}"
    }
    if (!allowExtra && extra.nonEmpty) {
      errors += s"unexpected columns: ${extra.mkString(", ")}"
    }
    if (mismatched.nonEmpty) {
      errors += s"type mismatches: ${mismatched.mkString(", ")}"
    }
    val failures = errors.result()
    if (failures.nonEmpty) {
      throw new PuddingException(
        s"$identifier schema contract failed: ${failures.mkString("; ")}")
    }
    dataFrame
  }

  private def expected(cfg: Map[String, Any]): Map[String, DataType] = {
    val raw = cfg.get(ColumnsKey).filter(_ != null).getOrElse {
      throw new PuddingException(s"$identifier requires $ColumnsKey")
    }
    val entries = raw match {
      case values: Map[_, _] =>
        values.toSeq.map { case (name, dataType) =>
          name.toString -> dataType.toString
        }
      case values: java.util.Map[_, _] =>
        values.asScala.toSeq.map { case (name, dataType) =>
          name.toString -> dataType.toString
        }
      case _ =>
        throw new PuddingException(s"$ColumnsKey must be a map from column name to SQL type")
    }
    if (entries.isEmpty) {
      throw new PuddingException(s"$ColumnsKey can not be empty")
    }
    val duplicates = entries.map(_._1.toLowerCase(Locale.ROOT))
      .groupBy(identity).collect { case (name, values) if values.size > 1 => name }
      .toSeq.sorted
    if (duplicates.nonEmpty) {
      throw new PuddingException(
        s"$ColumnsKey contains duplicate columns: ${duplicates.mkString(", ")}")
    }
    entries.map { case (name, typeName) =>
      if (name.trim.isEmpty || typeName.trim.isEmpty) {
        throw new PuddingException(s"$ColumnsKey contains an empty column name or type")
      }
      val dataType = try DataType.fromDDL(typeName.trim)
      catch {
        case error: IllegalArgumentException =>
          throw new PuddingException(
            s"$ColumnsKey.$name is not a valid Spark SQL type: ${error.getMessage}", error)
      }
      name.trim -> dataType
    }.toMap
  }
}
