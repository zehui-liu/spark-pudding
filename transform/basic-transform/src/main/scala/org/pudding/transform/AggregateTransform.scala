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

import org.apache.spark.sql.{DataFrame, functions}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Groups one dependency and computes aggregate expressions. `groupBy` is optional; when it is
 * absent the result is a single global aggregate row.
 *
 * {{{
 * config:
 *   groupBy: [city]
 *   aggregations:
 *     order_count: "count(*)"
 *     total_amount: "sum(amount)"
 * }}}
 *
 * The list form preserves output order:
 * {{{
 * aggregations:
 *   - name: order_count
 *     expr: "count(*)"
 *   - name: total_amount
 *     expr: "sum(amount)"
 * }}}
 *
 * In streaming execution the aggregation is stateful. `eventTime`, `watermark` and a bounded
 * `window` are required. The result has `window_start` and `window_end` columns followed by the
 * group-by columns and the aggregations. Windows are emitted in append mode after the watermark
 * passes their end, which bounds the state store.
 *
 * {{{
 * config:
 *   eventTime: order_time
 *   watermark: "10 minutes"
 *   window: "5 minutes"
 *   groupBy: [order_city]
 *   aggregations:
 *     order_count: "count(*)"
 * }}}
 */
class AggregateTransform extends DataTransform {

  private val GroupByKey = "groupBy"

  private val AggregationsKey = "aggregations"

  private val WindowAlias = "__pudding_window"

  override def identifier: String = "aggregate-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def streamingStateful: Boolean = true

  override def validateConfig(
      config: Option[Map[String, Any]],
      executionMode: ExecutionMode): Unit = {
    val cfg = TransformSupport.requiredConfig(config)
    val groupBy = TransformSupport.stringSeq(cfg, GroupByKey).getOrElse(Seq.empty)
    val aggregations = TransformSupport.namedExpressions(cfg, AggregationsKey)
    executionMode match {
      case ExecutionMode.Streaming =>
        StreamingStateSupport.validate(cfg, identifier, requireWindow = true)
        StreamingStateSupport.validateWindowOutputNames(
          groupBy ++ aggregations.map(_._1), identifier)
      case ExecutionMode.Batch =>
        StreamingStateSupport.rejectStreamingKeys(cfg, identifier)
    }
  }

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val dataFrame = TransformSupport.single(dataFrames, identifier)
    val groupBy = TransformSupport.stringSeq(cfg, GroupByKey).getOrElse(Seq.empty)
    val aggregations = TransformSupport.namedExpressions(cfg, AggregationsKey)

    val aggregated = if (dataFrame.isStreaming) {
      this.streamingAggregate(dataFrame, cfg, groupBy, aggregations)
    } else {
      StreamingStateSupport.rejectStreamingKeys(cfg, identifier)
      this.batchAggregate(dataFrame, groupBy, aggregations)
    }
    TransformSupport.applyNumPartitions(aggregated, cfg)
  }

  private def batchAggregate(
      dataFrame: DataFrame,
      groupBy: Seq[String],
      aggregations: Seq[(String, String)]): DataFrame = {
    val projections = aggregations.map { case (name, expression) =>
      functions.expr(expression).as(name)
    }
    if (groupBy.nonEmpty) {
      dataFrame
        .groupBy(groupBy.head, groupBy.tail: _*)
        .agg(projections.head, projections.tail: _*)
    } else {
      dataFrame.agg(projections.head, projections.tail: _*)
    }
  }

  private def streamingAggregate(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      groupBy: Seq[String],
      aggregations: Seq[(String, String)]): DataFrame = {
    val state = StreamingStateSupport.validate(cfg, identifier, requireWindow = true)
    StreamingStateSupport.validateWindowOutputNames(
      groupBy ++ aggregations.map(_._1), identifier)
    val eventTime = StreamingStateSupport.timestampColumn(dataFrame, state.eventTime)
    val resolvedGroupBy = StreamingStateSupport.resolveColumns(dataFrame, groupBy, GroupByKey)
    val window = state.window.getOrElse {
      throw new PuddingException(s"$identifier streaming requires a window")
    }
    val windowExpression = window.slide match {
      case Some(slide) =>
        functions.window(functions.col(eventTime), window.duration, slide)
      case None =>
        functions.window(functions.col(eventTime), window.duration)
    }
    val projections = aggregations.map { case (name, expression) =>
      functions.expr(expression).as(name)
    }
    val grouped = dataFrame
      .withWatermark(eventTime, state.watermark)
      .groupBy(
        (windowExpression.as(WindowAlias) +: resolvedGroupBy.map(functions.col)): _*)
      .agg(projections.head, projections.tail: _*)

    val windowColumn = functions.col(WindowAlias)
    val output = Seq(
      windowColumn.getField("start").as(StreamingStateSupport.WindowStartColumn),
      windowColumn.getField("end").as(StreamingStateSupport.WindowEndColumn)) ++
      resolvedGroupBy.map(functions.col) ++
      aggregations.map { case (name, _) => functions.col(name) }
    grouped.select(output: _*)
  }
}
