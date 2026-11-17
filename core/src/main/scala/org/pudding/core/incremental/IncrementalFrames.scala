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

package org.pudding.core.incremental

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

import org.pudding.core.PuddingException
import org.pudding.core.definition.IncrementalWrite

/**
 * Helpers shared by CDC sinks.
 */
object IncrementalFrames {

  private val RankColumn = "__pudding_cdc_rank"

  /**
   * Keep the last change for every key when the source provides change ordering. A batch can
   * contain several operations for the same key (for example delete then insert); applying all of
   * them as separate writes can resurrect or remove the wrong final state. Without ordering the
   * frame is returned unchanged, so the source contract remains unchanged.
   */
  def latestByKey(dataFrame: DataFrame, incremental: IncrementalWrite): DataFrame =
    this.latestByKey(dataFrame, incremental.keyFields, incremental.orderFields)

  /**
   * Keep the last change for every key using explicit metadata. Streaming CDC uses the same
   * ordering contract but does not have an [[IncrementalWrite]] cursor object.
   */
  def latestByKey(
      dataFrame: DataFrame,
      keyFields: Seq[String],
      orderFields: Seq[String]): DataFrame = {
    if (orderFields.isEmpty) {
      return dataFrame
    }
    if (keyFields.isEmpty) {
      throw new PuddingException(
        "a cdc sink needs keyFields to order multiple changes for the same key")
    }
    val missingKeys = keyFields.filterNot(dataFrame.columns.contains)
    if (missingKeys.nonEmpty) {
      throw new PuddingException(
        "cdc input is missing key columns: " + missingKeys.mkString(", "))
    }
    val missingOrder = orderFields.filterNot(dataFrame.columns.contains)
    if (missingOrder.nonEmpty) {
      throw new PuddingException(
        "cdc input is missing change ordering columns: " + missingOrder.mkString(", "))
    }

    val rankColumn = this.uniqueColumnName(dataFrame)
    val window = Window
      .partitionBy(keyFields.map(col): _*)
      .orderBy(orderFields.map(col(_).desc): _*)
    val ranked = dataFrame.withColumn(rankColumn, row_number().over(window))
    val latest = ranked.filter(col(rankColumn) === 1).drop(rankColumn)
    val keys = keyFields.toSet
    // Sources add ordering metadata as internal columns by convention (leading underscore).
    // A real payload column used as the cursor or ordering field, such as a JDBC updated_at,
    // must stay in the frame so the sink can write the target column.
    val internalOrderFields = orderFields
      .filterNot(keys.contains)
      .filter(_.startsWith("_"))
    latest.drop(internalOrderFields: _*)
  }

  private def uniqueColumnName(dataFrame: DataFrame): String = {
    val columns = dataFrame.columns.toSet
    var name = RankColumn
    var suffix = 0
    while (columns.contains(name)) {
      suffix += 1
      name = s"${RankColumn}_$suffix"
    }
    name
  }
}
