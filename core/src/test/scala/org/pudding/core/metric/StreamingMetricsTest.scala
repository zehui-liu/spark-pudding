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

package org.pudding.core.metric

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for streaming query counters. Spark query lifecycle tests exercise the listener path.
 */
class StreamingMetricsTest extends AnyFunSuite with Matchers {

  test("count started, progressed and terminated queries") {
    StreamingMetrics.reset()
    StreamingMetrics.recordStarted(UUID.randomUUID().toString, UUID.randomUUID().toString, "q")
    StreamingMetrics.recordBatch("q", "run", 1L, 10L, 25L)
    StreamingMetrics.recordBatch("q", "run", 2L, 5L, 15L)

    StreamingMetrics.value(StreamingMetrics.ActiveQueries) shouldBe 1L
    StreamingMetrics.value(StreamingMetrics.Batches) shouldBe 2L
    StreamingMetrics.value(StreamingMetrics.InputRows) shouldBe 15L
    StreamingMetrics.value(StreamingMetrics.BatchDurationMs) shouldBe 40L
    StreamingMetrics.value(StreamingMetrics.LastBatchId) shouldBe 2L

    StreamingMetrics.recordTerminated(
      UUID.randomUUID().toString, UUID.randomUUID().toString, None)

    StreamingMetrics.value(StreamingMetrics.ActiveQueries) shouldBe 0L
    StreamingMetrics.value(StreamingMetrics.QueryCompleted) shouldBe 1L
    StreamingMetrics.snapshot() should contain key StreamingMetrics.QueryStarted
  }

  test("record state operator totals") {
    StreamingMetrics.reset()
    StreamingMetrics.recordStateValues(
      total = 100L,
      updated = 20L,
      removed = 5L,
      memoryBytes = 4096L,
      droppedByWatermark = 3L)

    StreamingMetrics.value(StreamingMetrics.StateRowsTotal) shouldBe 100L
    StreamingMetrics.value(StreamingMetrics.StateRowsUpdated) shouldBe 20L
    StreamingMetrics.value(StreamingMetrics.StateRowsRemoved) shouldBe 5L
    StreamingMetrics.value(StreamingMetrics.StateMemoryBytes) shouldBe 4096L
    StreamingMetrics.value(StreamingMetrics.StateRowsDroppedByWatermark) shouldBe 3L
  }

  test("record the event-time watermark and its lag") {
    StreamingMetrics.reset()
    val watermark = "2024-01-01T00:00:10.000Z"
    val watermarkMs = java.time.Instant.parse(watermark).toEpochMilli

    StreamingMetrics.recordWatermark(
      Map("watermark" -> watermark),
      watermarkMs + 5000L)

    StreamingMetrics.value(StreamingMetrics.LastWatermarkMs) shouldBe watermarkMs
    StreamingMetrics.value(StreamingMetrics.WatermarkLagMs) shouldBe 5000L

    StreamingMetrics.recordWatermark(Map("watermark" -> "null"), watermarkMs + 6000L)
    StreamingMetrics.value(StreamingMetrics.WatermarkLagMs) shouldBe 5000L
  }

  test("record source lag and preserve the last lag on a failed measurement") {
    StreamingMetrics.reset()
    StreamingMetrics.recordSourceLag(
      records = 120L,
      maxLag = 40L,
      partitions = 3L,
      healthy = true,
      updatedMs = 1000L)

    StreamingMetrics.value(StreamingMetrics.SourceLagRecords) shouldBe 120L
    StreamingMetrics.value(StreamingMetrics.SourceLagMax) shouldBe 40L
    StreamingMetrics.value(StreamingMetrics.SourcePartitions) shouldBe 3L
    StreamingMetrics.value(StreamingMetrics.SourceLagUpdatedMs) shouldBe 1000L
    StreamingMetrics.value(StreamingMetrics.SourceHealthy) shouldBe 1L

    StreamingMetrics.recordSourceLagFailure(updatedMs = 2000L)

    StreamingMetrics.value(StreamingMetrics.SourceLagRecords) shouldBe 120L
    StreamingMetrics.value(StreamingMetrics.SourceHealthy) shouldBe 0L
    StreamingMetrics.value(StreamingMetrics.SourceLagUpdatedMs) shouldBe 2000L
  }

  test("count a failed query separately") {
    StreamingMetrics.reset()
    StreamingMetrics.recordStarted(UUID.randomUUID().toString, UUID.randomUUID().toString, "q")
    StreamingMetrics.recordTerminated(
      UUID.randomUUID().toString, UUID.randomUUID().toString, Some("failure"))

    StreamingMetrics.value(StreamingMetrics.QueryFailed) shouldBe 1L
    StreamingMetrics.value(StreamingMetrics.ActiveQueries) shouldBe 0L
    StreamingMetrics.value(StreamingMetrics.QueryCompleted) shouldBe 0L
  }

}
