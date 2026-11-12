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

package org.pudding.core.incremental

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  concat,
  lit,
  raise_error,
  trim,
  upper,
  when
}

import org.pudding.core.PuddingException

/**
 * Shared helpers for Structured Streaming CDC sinks.
 *
 * A streaming microbatch can contain insert, update and delete rows for the same key. Every CDC
 * sink therefore normalizes the operation column, deduplicates the batch with a deterministic
 * order, and removes Kafka/Debezium bookkeeping before writing to the target table.
 */
object StreamingCdc {

  val DefaultOpColumn = "__op"

  val KafkaKeyColumn = "_kafka_key"

  val KafkaTopicColumn = "_kafka_topic"

  val KafkaPartitionColumn = "_kafka_partition"

  val KafkaOffsetColumn = "_kafka_offset"

  val KafkaTimestampColumn = "_kafka_timestamp"

  val KafkaTimestampTypeColumn = "_kafka_timestamp_type"

  val DebeziumTimestampColumn = "_cdc_timestamp"

  /**
   * Monotonic tie-breaker emitted by native CDC sources. Several changes can share a database
   * timestamp; the sequence keeps same-key updates deterministic inside one micro-batch.
   */
  val CdcSequenceColumn = "_cdc_sequence"

  private val MetadataPrefixes = Seq("_kafka_", "_cdc_", "_pudding_")

  def opColumn(config: Map[String, Any]): Option[String] = {
    config.get("cdcOpColumn").filter(_ != null).map {
      case value: String if value.trim.nonEmpty => value.trim
      case _: String =>
        throw new PuddingException("cdcOpColumn can not be empty!")
      case _ =>
        throw new PuddingException("cdcOpColumn must be a string!")
    }
  }

  def orderFields(config: Map[String, Any]): Seq[String] = {
    this.stringSeq(config.get("cdcOrderFields"), "cdcOrderFields")
  }

  /**
   * Resolve the ordering columns for a batch. Explicit configuration wins; otherwise use the
   * metadata emitted by the Kafka or native CDC source. Kafka ordering is only meaningful per
   * partition, so a caller that needs a global order should supply an event-time field
   * explicitly.
   */
  def resolveOrderFields(
      dataFrame: DataFrame,
      configured: Seq[String]): Seq[String] = {
    if (configured.nonEmpty) {
      val missing = configured.filterNot(dataFrame.columns.contains)
      if (missing.nonEmpty) {
        throw new PuddingException(
          "cdc input is missing change ordering columns: " + missing.mkString(", "))
      }
      configured
    } else {
      // For Kafka input the broker position is the authoritative arrival order. A producer or
      // database clock can move backwards (retries, skew, multi-partition pipelines), so using
      // the event timestamp first could sort a later delete before an earlier upsert. Native CDC
      // sources have no Kafka metadata and use the database timestamp plus the monotonic
      // sequence instead.
      val kafkaOrder = Seq(KafkaPartitionColumn, KafkaOffsetColumn, KafkaTimestampColumn)
      val nativeOrder = Seq(DebeziumTimestampColumn, CdcSequenceColumn)
      val candidates =
        if (dataFrame.columns.contains(KafkaOffsetColumn)) kafkaOrder ++ nativeOrder
        else nativeOrder
      candidates.filter(name => dataFrame.columns.contains(name))
    }
  }

  /**
   * Normalize I/U/D values. Common Debezium and Hudi spellings are accepted; unknown values fail
   * the query instead of being silently dropped.
   */
  def normalizeOps(dataFrame: DataFrame, opColumn: String): DataFrame = {
    if (opColumn == null || opColumn.trim.isEmpty) {
      throw new PuddingException("cdc operation column can not be empty")
    }
    if (!dataFrame.columns.contains(opColumn)) {
      throw new PuddingException(
        s"cdc input is missing operation column: $opColumn")
    }
    val value = upper(trim(col(opColumn).cast("string")))
    val error = concat(
      lit("unsupported cdc operation in "),
      lit(opColumn),
      lit(": "),
      coalesce(col(opColumn).cast("string"), lit("null")))
    val normalized = when(value.isin("I", "C", "R"), lit("I"))
      .when(value.isin("U", "-U"), lit("U"))
      .when(value === "D", lit("D"))
      .otherwise(raise_error(error))
    dataFrame.withColumn(opColumn, normalized)
  }

  /**
   * Normalize and keep one latest row per key. Internal order columns (leading underscore) are
   * dropped; real payload columns used for ordering are kept. The operation column remains so
   * the sink can split deletes from upserts.
   */
  def prepare(
      dataFrame: DataFrame,
      keyFields: Seq[String],
      opColumn: String,
      configuredOrderFields: Seq[String]): DataFrame = {
    if (keyFields.isEmpty) {
      throw new PuddingException("streaming cdc requires keyFields")
    }
    val normalized = this.normalizeOps(dataFrame, opColumn)
    val order = this.resolveOrderFields(normalized, configuredOrderFields)
    if (order.isEmpty) {
      throw new PuddingException(
        "streaming cdc requires cdcOrderFields or Kafka/Debezium ordering metadata")
    }
    IncrementalFrames.latestByKey(normalized, keyFields, order)
  }

  /** Drop Kafka/Debezium/Pudding bookkeeping columns, plus any explicitly supplied columns. */
  def dropInternal(dataFrame: DataFrame, extra: Seq[String] = Seq.empty): DataFrame = {
    val drop = dataFrame.columns.filter { name =>
      this.isInternalColumn(name) || extra.contains(name)
    }
    if (drop.isEmpty) dataFrame else dataFrame.drop(drop: _*)
  }

  def isInternalColumn(name: String): Boolean = {
    name != null && this.MetadataPrefixes.exists(name.startsWith)
  }

  private def stringSeq(value: Option[Any], key: String): Seq[String] = value.filter(_ != null) match {
    case Some(text: String) =>
      text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    case Some(seq: Seq[_]) =>
      seq.map(item => this.requiredText(item, key))
    case Some(array: Array[_]) =>
      array.map(item => this.requiredText(item, key)).toSeq
    case Some(list: java.util.List[_]) =>
      list.asScala.map(item => this.requiredText(item, key)).toSeq
    case Some(_) =>
      throw new PuddingException(s"$key must be a string or a list of strings!")
    case None => Seq.empty
  }

  private def requiredText(value: Any, key: String): String = {
    if (value == null) {
      throw new PuddingException(s"$key can not contain a null value!")
    }
    val text = value.toString.trim
    if (text.isEmpty) {
      throw new PuddingException(s"$key can not contain an empty value!")
    }
    text
  }
}
