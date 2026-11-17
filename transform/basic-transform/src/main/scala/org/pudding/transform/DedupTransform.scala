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

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform

/**
 * Keeps one row per key.
 *
 * {{{
 * config:
 *   keys: [order_id]
 * }}}
 *
 * Streaming deduplication is stateful and requires an event-time watermark:
 * {{{
 * config:
 *   keys: [order_id]
 *   eventTime: order_time
 *   watermark: "10 minutes"
 * }}}
 *
 * Spark uses the watermark to evict deduplication state. A duplicate that arrives after the
 * watermark has passed its event time can be emitted again, so use a sink primary key when exact
 * cross-window deduplication is required. Which row is retained for a key is not deterministic.
 *
 * This transform is not allowed in `sync` incremental pipelines: `dropDuplicates` keeps an
 * arbitrary row and would discard the source's operation/ordering metadata, so a delete can be
 * lost. Use a keyed sink or deduplicate before the incremental source instead.
 */
class DedupTransform extends DataTransform {

  private val KeysKey = "keys"

  override def identifier: String = "dedup-transform"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def streamingStateful: Boolean = true

  override def preservesIncrementalMetadata: Boolean = false

  override def validateConfig(
      config: Option[Map[String, Any]],
      executionMode: ExecutionMode): Unit = {
    val cfg = TransformSupport.requiredConfig(config)
    this.keys(cfg)
    executionMode match {
      case ExecutionMode.Streaming =>
        StreamingStateSupport.validate(cfg, identifier, requireWindow = false)
      case ExecutionMode.Batch =>
        StreamingStateSupport.rejectStreamingKeys(cfg, identifier)
    }
  }

  override def handle(
      dataFrames: Map[String, DataFrame],
      config: Option[Map[String, Any]]): DataFrame = {
    val cfg = TransformSupport.requiredConfig(config)
    val dataFrame = TransformSupport.single(dataFrames, identifier)
    val keys = StreamingStateSupport.resolveColumns(dataFrame, this.keys(cfg), KeysKey)

    val deduplicated = if (dataFrame.isStreaming) {
      val state = StreamingStateSupport.validate(cfg, identifier, requireWindow = false)
      val eventTime = StreamingStateSupport.timestampColumn(dataFrame, state.eventTime)
      dataFrame
        .withWatermark(eventTime, state.watermark)
        .dropDuplicates(keys)
    } else {
      StreamingStateSupport.rejectStreamingKeys(cfg, identifier)
      dataFrame.dropDuplicates(keys)
    }
    TransformSupport.applyNumPartitions(deduplicated, cfg)
  }

  private def keys(cfg: Map[String, Any]): Seq[String] = {
    TransformSupport.stringSeq(cfg, KeysKey).getOrElse {
      throw new PuddingException(s"$identifier requires $KeysKey")
    }
  }
}
