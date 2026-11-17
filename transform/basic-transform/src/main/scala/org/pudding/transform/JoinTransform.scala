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

package org.pudding.transform

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, functions}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Joins exactly two dependencies. `on` may be a list of common column names or a map from left
 * column to right column. Join keys are emitted once, using the left column name.
 *
 * {{{
 * config:
 *   left: orders
 *   right: users
 *   joinType: left
 *   on:
 *     user_id: uid
 *   condition: "l.amount >= 10"
 * }}}
 *
 * The optional `condition` is an extra predicate and should reference columns as `l.<name>` or
 * `r.<name>`. Join key equality belongs in `on`; key columns are renamed internally during the
 * join, so do not reference them in `condition`.
 */
class JoinTransform extends DataTransform {

  private val LeftKey = "left"

  private val RightKey = "right"

  private val OnKey = "on"

  private val JoinTypeKey = "joinType"

  private val ConditionKey = "condition"

  private val LeftEventTimeKey = "leftEventTime"

  private val RightEventTimeKey = "rightEventTime"

  private val LeftWatermarkKey = "leftWatermark"

  private val RightWatermarkKey = "rightWatermark"

  private val StreamingKeys = Seq(
    LeftEventTimeKey, RightEventTimeKey, LeftWatermarkKey, RightWatermarkKey)

  private val InternalLeftPrefix = "__pudding_join_left_key_"

  private val InternalRightPrefix = "__pudding_join_right_key_"

  override def identifier: String = "join-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def streamingStateful: Boolean = true

  override def validateConfig(
      config: Option[Map[String, Any]],
      executionMode: ExecutionMode): Unit = {
    val cfg = TransformSupport.requiredConfig(config)
    executionMode match {
      case ExecutionMode.Streaming =>
        this.streamingWatermarks(cfg)
        val joinType = this.parseJoinType(cfg.get(JoinTypeKey).filter(_ != null))
        if (joinType != "inner") {
          throw new PuddingException(
            "join-transform streaming supports only joinType=inner; outer and semi/anti " +
              "joins require an event-time range condition and a separate contract")
        }
      case ExecutionMode.Batch =>
        val configured = StreamingKeys.filter(key => cfg.get(key).exists(_ != null))
        if (configured.nonEmpty) {
          throw new PuddingException(
            s"join-transform ${configured.mkString(", ")} can only be used in streaming execution")
        }
    }
  }

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    if (dataFrames == null || dataFrames.size != 2) {
      throw new PuddingException(
        s"$identifier requires exactly two dependencies, but got: " +
          Option(dataFrames).map(_.size).getOrElse(0))
    }

    val leftId = TransformSupport.requiredString(cfg, LeftKey)
    val rightId = TransformSupport.requiredString(cfg, RightKey)
    if (leftId == rightId) {
      throw new PuddingException(s"$LeftKey and $RightKey must reference different dependencies!")
    }
    val leftDataFrame = dataFrames.getOrElse(leftId,
      throw new PuddingException(s"$identifier can not find left dependency: $leftId"))
    val rightDataFrame = dataFrames.getOrElse(rightId,
      throw new PuddingException(s"$identifier can not find right dependency: $rightId"))
    val streaming = leftDataFrame.isStreaming || rightDataFrame.isStreaming
    val (leftBase, rightBase) = if (streaming) {
      if (!leftDataFrame.isStreaming || !rightDataFrame.isStreaming) {
        throw new PuddingException(
          "join-transform requires both dependencies to be streaming or both to be batch")
      }
      val state = this.streamingWatermarks(cfg)
      val leftEventTime = StreamingStateSupport.timestampColumn(
        leftDataFrame, state.leftEventTime)
      val rightEventTime = StreamingStateSupport.timestampColumn(
        rightDataFrame, state.rightEventTime)
      (
        leftDataFrame.withWatermark(leftEventTime, state.leftWatermark),
        rightDataFrame.withWatermark(rightEventTime, state.rightWatermark))
    } else {
      val configured = StreamingKeys.filter(key => cfg.get(key).exists(_ != null))
      if (configured.nonEmpty) {
        throw new PuddingException(
          s"$identifier ${configured.mkString(", ")} can only be used in streaming execution")
      }
      (leftDataFrame, rightDataFrame)
    }

    val onValue = cfg.get(OnKey).filter(_ != null).getOrElse {
      throw new PuddingException(s"can not find param: $OnKey, $OnKey must be exist!")
    }
    val pairs = this.parseOn(onValue).map { case (leftColumn, rightColumn) =>
      this.resolveColumn(leftDataFrame, leftColumn, "left") ->
        this.resolveColumn(rightDataFrame, rightColumn, "right")
    }
    val duplicatedLeftKeys = this.duplicates(pairs.map(_._1))
    if (duplicatedLeftKeys.nonEmpty) {
      throw new PuddingException(
        s"$OnKey maps the same left column more than once: " +
          duplicatedLeftKeys.mkString(", "))
    }

    val joinType = this.parseJoinType(cfg.get(JoinTypeKey).filter(_ != null))
    if (streaming && joinType != "inner") {
      throw new PuddingException(
        "join-transform streaming supports only joinType=inner; outer and semi/anti joins " +
          "require an event-time range condition and a separate contract")
    }
    val semiOrAnti = joinType == "left_semi" || joinType == "left_anti"
    val (leftKeyNames, rightKeyNames) =
      this.internalNames(pairs, leftDataFrame, rightDataFrame)

    val leftPrepared = if (semiOrAnti) {
      leftBase
    } else {
      leftKeyNames.foldLeft(leftBase) { case (dataFrame, (column, internal)) =>
        dataFrame.withColumnRenamed(column, internal)
      }
    }
    val rightPrepared = rightKeyNames.foldLeft(rightBase) {
      case (dataFrame, (column, internal)) =>
        dataFrame.withColumnRenamed(column, internal)
    }

    val equalities = pairs.map { case (leftColumn, rightColumn) =>
      val leftReference = if (semiOrAnti) leftColumn else leftKeyNames(leftColumn)
      functions.col(s"l.$leftReference") === functions.col(s"r.${rightKeyNames(rightColumn)}")
    }
    val baseCondition = equalities.reduceLeft(_ && _)
    val joinCondition = TransformSupport.optionalString(cfg, ConditionKey)
      .map(condition => baseCondition && functions.expr(condition))
      .getOrElse(baseCondition)
    val joined = leftPrepared.as("l")
      .join(rightPrepared.as("r"), joinCondition, joinType)

    val result = if (semiOrAnti) {
      joined
    } else {
      val leftToRight = pairs.toMap
      val rightKeySet = rightKeyNames.keySet
      val leftProjection = leftDataFrame.columns.map { column =>
        leftKeyNames.get(column) match {
          case Some(internalLeft) =>
            val internalRight = rightKeyNames(leftToRight(column))
            functions.coalesce(
              functions.col(s"l.$internalLeft"),
              functions.col(s"r.$internalRight")).as(column)
          case None =>
            functions.col(s"l.$column").as(column)
        }
      }
      val rightProjection = rightDataFrame.columns.filterNot(rightKeySet.contains).map { column =>
        functions.col(s"r.$column").as(column)
      }
      joined.select(leftProjection ++ rightProjection: _*)
    }

    TransformSupport.applyNumPartitions(result, cfg)
  }

  private case class JoinStreamingState(
      leftEventTime: String,
      rightEventTime: String,
      leftWatermark: String,
      rightWatermark: String)

  private def streamingWatermarks(cfg: Map[String, Any]): JoinStreamingState = {
    def required(key: String): String = {
      TransformSupport.optionalString(cfg, key).getOrElse {
        throw new PuddingException(s"$identifier streaming requires $key")
      }
    }
    JoinStreamingState(
      leftEventTime = required(LeftEventTimeKey),
      rightEventTime = required(RightEventTimeKey),
      leftWatermark = required(LeftWatermarkKey),
      rightWatermark = required(RightWatermarkKey))
  }

  private def parseOn(value: Any): Seq[(String, String)] = {
    val pairs = value match {
      case map: Map[_, _] =>
        map.toSeq.map { case (left, right) =>
          this.requiredColumn(left, s"$OnKey left column") ->
            this.requiredColumn(right, s"$OnKey right column")
        }
      case map: java.util.Map[_, _] =>
        map.asScala.toSeq.map { case (left, right) =>
          this.requiredColumn(left, s"$OnKey left column") ->
            this.requiredColumn(right, s"$OnKey right column")
        }
      case seq: Seq[_] =>
        seq.map(item => {
          val name = this.requiredColumn(item, s"$OnKey column")
          name -> name
        })
      case list: java.util.List[_] =>
        list.asScala.toSeq.map(item => {
          val name = this.requiredColumn(item, s"$OnKey column")
          name -> name
        })
      case _ =>
        throw new PuddingException(
          s"$OnKey must be a list of column names or a map from left to right columns!")
    }

    if (pairs.isEmpty) {
      throw new PuddingException(s"$OnKey can not be empty!")
    }
    pairs
  }

  private def parseJoinType(value: Option[Any]): String = {
    val raw = value.map(_.toString).getOrElse("inner")
    TransformSupport.normalize(raw) match {
      case "inner" => "inner"
      case "left" | "leftouter" => "left"
      case "right" | "rightouter" => "right"
      case "full" | "fullouter" | "outer" => "full"
      case "semi" | "leftsemi" => "left_semi"
      case "anti" | "leftanti" => "left_anti"
      case other =>
        throw new PuddingException(
          s"$JoinTypeKey is not supported: $other, supported: inner, left, right, full, " +
            "left_semi, left_anti")
    }
  }

  private def requiredColumn(value: Any, key: String): String = value match {
    case text: String if text.trim.nonEmpty => text.trim
    case _: String => throw new PuddingException(s"$key can not be empty!")
    case _ => throw new PuddingException(s"$key must be a string!")
  }

  private def resolveColumn(
      dataFrame: DataFrame,
      requested: String,
      side: String): String = {
    val matches = dataFrame.columns.filter(_.equalsIgnoreCase(requested))
    if (matches.isEmpty) {
      throw new PuddingException(
        s"join $side column '$requested' does not exist, available: " +
          dataFrame.columns.sorted.mkString(", "))
    }
    if (matches.length > 1) {
      throw new PuddingException(
        s"join $side column '$requested' is ambiguous: ${matches.mkString(", ")}")
    }
    matches.head
  }

  private def duplicates(values: Seq[String]): Seq[String] = {
    values.groupBy(identity).collect {
      case (value, occurrences) if occurrences.size > 1 => value
    }.toSeq.sorted
  }

  /**
   * Generate internal key names that cannot collide with an input column. Renaming keys before
   * the join is what lets the output keep exactly one copy of every join key.
   */
  private def internalNames(
      pairs: Seq[(String, String)],
      leftDataFrame: DataFrame,
      rightDataFrame: DataFrame): (Map[String, String], Map[String, String]) = {
    val used = mutable.Set[String]()
    (leftDataFrame.columns ++ rightDataFrame.columns)
      .foreach(column => used += column.toLowerCase(Locale.ROOT))
    var counter = 0

    def next(prefix: String): String = {
      var candidate = prefix + counter
      while (used.contains(candidate.toLowerCase(Locale.ROOT))) {
        counter += 1
        candidate = prefix + counter
      }
      used += candidate.toLowerCase(Locale.ROOT)
      counter += 1
      candidate
    }

    val leftNames = pairs.map(_._1).distinct
      .map(column => column -> next(InternalLeftPrefix)).toMap
    val rightNames = pairs.map(_._2).distinct
      .map(column => column -> next(InternalRightPrefix)).toMap
    (leftNames, rightNames)
  }
}
