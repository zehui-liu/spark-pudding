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

import org.apache.spark.sql.DataFrame

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Validates rows against one or more SQL conditions.
 *
 * {{{
 * config:
 *   requiredColumns: [id, amount]
 *   rules:
 *     amount_positive: "amount > 0"
 *     id_present: "id is not null"
 *   mode: drop          # drop or error
 *   quarantinePath: hdfs:///data/quarantine/orders
 * }}}
 *
 * `drop` filters invalid rows. `error` fails the batch after reporting only the invalid row
 * count. A quarantine path is batch-only and writes invalid rows as JSON before dropping them.
 * Streaming uses `drop`; a streaming query can not perform a driver-side count or a side write.
 */
class ValidateTransform extends DataTransform {

  private val RequiredColumnsKey = "requiredColumns"

  private val RulesKey = "rules"

  private val ConditionKey = "condition"

  private val ModeKey = "mode"

  private val QuarantinePathKey = "quarantinePath"

  private val ErrorMode = "error"

  private val DropMode = "drop"

  override def identifier: String = "validate-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def preservesIncrementalMetadata: Boolean = true

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val dataFrame = TransformSupport.single(dataFrames, identifier)
    val requiredColumns = TransformSupport.stringSeq(cfg, RequiredColumnsKey)
      .getOrElse(Seq.empty)
    val missing = requiredColumns.filterNot(dataFrame.columns.contains)
    if (missing.nonEmpty) {
      throw new org.pudding.core.PuddingException(
        s"validate-transform required columns are missing: ${missing.mkString(", ")}")
    }
    val condition = this.condition(cfg)
    val mode = this.mode(cfg)
    val quarantinePath = TransformSupport.optionalString(cfg, QuarantinePathKey)
    val streaming = dataFrame.isStreaming
    if (quarantinePath.nonEmpty && streaming) {
      throw new org.pudding.core.PuddingException(
        "validate-transform quarantinePath is batch-only")
    }
    if (quarantinePath.nonEmpty && mode != DropMode) {
      throw new org.pudding.core.PuddingException(
        "validate-transform quarantinePath requires mode=drop")
    }
    if (streaming && mode == ErrorMode) {
      throw new org.pudding.core.PuddingException(
        "validate-transform streaming mode requires mode=drop; error mode needs a batch count")
    }

    val valid = dataFrame.filter(condition)
    if (!streaming) {
      val invalid = dataFrame.filter(s"NOT coalesce(($condition), false)")
      quarantinePath.foreach(path =>
        invalid.write.mode("append").format("json").save(path))
      if (mode == ErrorMode) {
        val count = invalid.count()
        if (count > 0) {
          throw new org.pudding.core.PuddingException(
            s"validate-transform rejected $count row(s)")
        }
      }
    }
    TransformSupport.applyNumPartitions(valid, cfg)
  }

  private def condition(cfg: Map[String, Any]): String = {
    val explicit = TransformSupport.optionalString(cfg, ConditionKey).toSeq
    val rules = if (cfg.get(RulesKey).filter(_ != null).isDefined) {
      TransformSupport.namedExpressions(cfg, RulesKey).map(_._2)
    } else {
      Seq.empty
    }
    val values = explicit ++ rules
    if (values.isEmpty) {
      throw new org.pudding.core.PuddingException(
        s"validate-transform requires $ConditionKey or $RulesKey")
    }
    values.map(value => s"coalesce(($value), false)").mkString(" AND ")
  }

  private def mode(cfg: Map[String, Any]): String = {
    val value = TransformSupport.optionalString(cfg, ModeKey)
      .map(_.toLowerCase(Locale.ROOT))
      .getOrElse(ErrorMode)
    if (value != ErrorMode && value != DropMode) {
      throw new org.pudding.core.PuddingException(
        s"validate-transform $ModeKey must be error or drop")
    }
    value
  }
}
