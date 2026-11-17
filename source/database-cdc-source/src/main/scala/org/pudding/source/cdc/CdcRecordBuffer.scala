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

import scala.collection.mutable

import org.pudding.core.PuddingException
import org.pudding.core.util.ConfigRedactor

/**
 * Bounded hand-off between a driver-side CDC poller and Spark's micro-batch planner.
 *
 * Records are retained until Spark commits the batch that contains them. A failed or retried
 * batch can therefore be planned again from the same start offset. Capacity applies to
 * uncommitted records, which back-pressures the connector instead of growing an unbounded queue.
 * The configured capacity is kept strictly above `maxRecordsPerBatch`: Spark commits the previous
 * source batch only while constructing a batch with new data, so a queue that can fill to exactly
 * one batch would block the poller and stop the commit that would free space.
 */
private[cdc] final class CdcRecordBuffer(
    label: String,
    capacity: Int,
    val maxRecordsPerBatch: Int,
    startOffset: CdcOffset) {

  require(capacity > 0, "cdc buffer capacity must be positive")
  require(this.maxRecordsPerBatch > 0, "cdc maxRecordsPerBatch must be positive")

  private case class Record(offset: CdcOffset, json: String)

  private val records = mutable.ArrayBuffer[Record]()

  private var lastCommitted: CdcOffset = startOffset

  private var lastAppendedSequence: Long = startOffset.sequence

  private var failure: Throwable = null

  private var stopped = false

  /**
   * Return the end offset for the next micro-batch, starting after `after`.
   *
   * Spark may construct the next batch before the previous batch has been committed (source
   * commits run one batch behind), so the cursor must follow the latest exposed offset rather
   * than the last committed buffer entry.
   */
  def latestOffset(after: CdcOffset): CdcOffset = this.synchronized {
    this.checkFailure()
    val start = this.indexAfter(after)
    if (start >= this.records.size) {
      after
    } else {
      val end = math.min(this.records.size, start + this.maxRecordsPerBatch) - 1
      this.records(end).offset
    }
  }

  def latestAvailable(after: CdcOffset): CdcOffset = this.synchronized {
    this.checkFailure()
    if (this.records.isEmpty) after else this.records.last.offset
  }

  private def indexAfter(offset: CdcOffset): Int = {
    val index = this.records.indexWhere(_.offset.sequence == offset.sequence)
    if (index >= 0) {
      index + 1
    } else if (offset.sequence == this.lastCommitted.sequence) {
      0
    } else if (this.records.isEmpty) {
      0
    } else if (offset.sequence < this.records.head.offset.sequence) {
      0
    } else {
      throw new PuddingException(
        s"$label can not locate the last exposed offset ${offset.toString}")
    }
  }

  def plan(start: CdcOffset, end: CdcOffset): Array[String] = this.synchronized {
    this.checkFailure()
    if (start.sequence == end.sequence) {
      Array.empty[String]
    } else {
      // On restart Spark can ask for a batch recorded in the checkpoint offset log before the
      // new poller has read its rows. Wait briefly for the connector to catch up instead of
      // losing the replay.
      val deadline = System.nanoTime() + CdcRecordBuffer.PlanWaitNanos
      while (this.records.lastIndexWhere(_.offset.sequence == end.sequence) < 0 &&
        this.failure == null && !this.stopped && System.nanoTime() < deadline) {
        try this.wait(1000L)
        catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            throw new PuddingException(s"$label was interrupted while preparing a micro-batch")
        }
      }
      this.checkFailure()
      val to = this.records.lastIndexWhere(_.offset.sequence == end.sequence)
      if (to < 0) {
        throw new PuddingException(
          s"$label can not find the micro-batch end offset ${end.toString}")
      }
      val from = this.records.indexWhere(_.offset.sequence == start.sequence) match {
        case -1 if start.sequence == this.lastCommitted.sequence => 0
        case -1 =>
          throw new PuddingException(
            s"$label can not find the micro-batch start offset ${start.toString}")
        case index => index + 1
      }
      if (from > to + 1) {
        throw new PuddingException(
          s"$label has an invalid micro-batch range: ${start.toString}..${end.toString}")
      }
      this.records.slice(from, to + 1).map(_.json).toArray
    }
  }

  def append(offset: CdcOffset, json: String): Unit = this.synchronized {
    while (!this.stopped && this.failure == null && this.records.size >= capacity) {
      try this.wait(1000L)
      catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new PuddingException(s"$label was interrupted while waiting for queue space")
      }
    }
    if (this.failure != null) {
      this.checkFailure()
    }
    if (this.stopped) {
      throw new PuddingException(s"$label was stopped")
    }
    if (offset.sequence <= this.lastAppendedSequence) {
      throw new PuddingException(
        s"$label produced a non-increasing CDC sequence: ${offset.sequence}")
    }
    this.records += Record(offset, json)
    this.lastAppendedSequence = offset.sequence
    this.notifyAll()
  }

  def commit(end: CdcOffset): Unit = this.synchronized {
    this.checkFailure()
    if (end.sequence == this.lastCommitted.sequence) {
      ()
    } else {
      val index = this.records.indexWhere(_.offset.sequence == end.sequence)
      if (index < 0) {
        throw new PuddingException(
          s"$label can not commit an unknown offset ${end.toString}")
      }
      this.records.remove(0, index + 1)
      this.lastCommitted = end
      this.notifyAll()
    }
  }

  def fail(error: Throwable): Unit = this.synchronized {
    if (this.failure == null) {
      this.failure = error
    }
    this.notifyAll()
  }

  def close(): Unit = this.synchronized {
    this.stopped = true
    this.notifyAll()
  }

  def committedOffset: CdcOffset = this.synchronized {
    this.lastCommitted
  }

  private def checkFailure(): Unit = {
    if (this.failure != null) {
      val detail = Option(this.failure.getMessage).filter(_.nonEmpty)
        .getOrElse(this.failure.getClass.getSimpleName)
      throw new PuddingException(
        s"$label failed: ${ConfigRedactor.redactLine(detail)}",
        this.failure)
    }
  }
}

private[cdc] object CdcRecordBuffer {

  private val PlanWaitNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(300L)
}
