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

import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import javax.management.{ObjectName, StandardMBean}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{
  StreamingQueryListener,
  StreamingQueryProgress
}

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

/**
 * Driver-side counters and gauges plus structured logs for Structured Streaming queries.
 *
 * Query names, ids and numeric progress are logged. Query configuration and checkpoint paths
 * are deliberately excluded so a log does not become a second configuration surface.
 */
object StreamingMetrics extends Logging {

  val QueryStarted = "streaming.query.started"

  val QueryCompleted = "streaming.query.completed"

  val QueryFailed = "streaming.query.failed"

  val ActiveQueries = "streaming.query.active"

  val Batches = "streaming.query.batches"

  val InputRows = "streaming.query.input.rows"

  val BatchDurationMs = "streaming.query.batch.duration.ms"

  val LastBatchId = "streaming.query.last.batch.id"

  val LastProgressMs = "streaming.query.last.progress.timestamp.ms"

  val LastWatermarkMs = "streaming.query.last.watermark.ms"

  val WatermarkLagMs = "streaming.query.watermark.lag.ms"

  val StateRowsTotal = "streaming.state.rows.total"

  val StateRowsUpdated = "streaming.state.rows.updated"

  val StateRowsRemoved = "streaming.state.rows.removed"

  val StateMemoryBytes = "streaming.state.memory.bytes"

  val StateRowsDroppedByWatermark = "streaming.state.rows.dropped.by.watermark"

  val SourceLagRecords = "streaming.source.lag.records"

  val SourceLagMax = "streaming.source.lag.max"

  val SourcePartitions = "streaming.source.partitions"

  val SourceLagUpdatedMs = "streaming.source.lag.updated.ms"

  val SourceHealthy = "streaming.source.healthy"

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val counters = new ConcurrentHashMap[String, AtomicLong]()

  private val registeredSessions = ConcurrentHashMap.newKeySet[String]()

  private val MBeanName = new ObjectName(
    "org.pudding.core.streaming:type=StreamingMetrics")

  @volatile private var registeredJmx = false

  /**
   * Register one listener per Spark application. Repeated calls for the same SparkSession are
   * ignored, which also keeps tests that reuse a session from accumulating listeners.
   */
  def register(session: SparkSession): Unit = this.synchronized {
    val applicationId = session.sparkContext.applicationId
    if (registeredSessions.add(applicationId)) {
      session.streams.addListener(new Listener)
      this.registerJmx()
    }
  }

  def value(name: String): Long = {
    val counter = this.counters.get(name)
    if (counter == null) 0L else counter.get()
  }

  def snapshot(): Map[String, Long] = {
    val builder = Map.newBuilder[String, Long]
    val iterator = this.counters.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      builder += entry.getKey -> entry.getValue.get()
    }
    builder.result()
  }

  def reset(): Unit = this.counters.clear()

  def registerJmx(): Unit = this.synchronized {
    if (!this.registeredJmx) {
      try {
        val server = ManagementFactory.getPlatformMBeanServer
        if (!server.isRegistered(this.MBeanName)) {
          server.registerMBean(
            new StandardMBean(new StreamingMetricsBean(), classOf[StreamingMetricsMBean]),
            this.MBeanName)
        }
        this.registeredJmx = true
      } catch {
        case NonFatal(_) => // Leave isJmxRegistered false so the failure is observable.
      }
    }
  }

  def isJmxRegistered: Boolean = this.registeredJmx

  private def counter(name: String): AtomicLong =
    this.counters.computeIfAbsent(name, _ => new AtomicLong())

  private[metric] def recordStarted(id: String, runId: String, name: String): Unit = {
    this.counter(QueryStarted).incrementAndGet()
    this.counter(ActiveQueries).incrementAndGet()
    logInfo(
      s"event=$QueryStarted id=$id runId=$runId name=${Option(name).getOrElse("")}")
  }

  private[metric] def recordProgress(
      progress: StreamingQueryProgress,
      nowMs: Long = System.currentTimeMillis()): Unit = {
    this.counter(LastProgressMs).set(nowMs)
    this.recordBatch(
      progress.id.toString,
      progress.runId.toString,
      progress.batchId,
      progress.numInputRows,
      progress.batchDuration)
    this.recordWatermark(progress.eventTime.asScala.toMap)
    this.recordStateOperators(Option(progress.stateOperators).getOrElse(Array.empty))
  }

  /**
   * Record source lag and health. A source that can measure lag updates these gauges; a failed
   * measurement sets health to zero but leaves the last known lag values in place.
   */
  private[pudding] def recordSourceLag(
      records: Long,
      maxLag: Long,
      partitions: Long,
      healthy: Boolean,
      updatedMs: Long = System.currentTimeMillis()): Unit = {
    this.counter(SourceLagRecords).set(records)
    this.counter(SourceLagMax).set(maxLag)
    this.counter(SourcePartitions).set(partitions)
    this.counter(SourceLagUpdatedMs).set(updatedMs)
    this.counter(SourceHealthy).set(if (healthy) 1L else 0L)
    logInfo(
      s"event=streaming.source.lag records=$records maxLag=$maxLag " +
        s"partitions=$partitions healthy=$healthy")
  }

  /** Mark a source lag measurement as failed without erasing the last known lag. */
  private[pudding] def recordSourceLagFailure(
      updatedMs: Long = System.currentTimeMillis()): Unit = {
    this.counter(SourceLagUpdatedMs).set(updatedMs)
    this.counter(SourceHealthy).set(0L)
    logWarning("event=streaming.source.lag.failed healthy=false")
  }

  /**
   * Aggregate Spark's state-operator progress across all stateful operators in the query.
   * Queries without state operators leave the previous values unchanged so a shared JMX
   * registry does not erase the stateful query's snapshot.
   */
  private[metric] def recordStateOperators(
      operators: Array[org.apache.spark.sql.streaming.StateOperatorProgress]): Unit = {
    if (operators != null && operators.nonEmpty) {
      val total = operators.map(_.numRowsTotal).sum
      val memoryBytes = operators.map(_.memoryUsedBytes).sum
      val droppedByWatermark = operators.map(_.numRowsDroppedByWatermark).sum
      this.recordStateValues(
        total = total,
        updated = operators.map(_.numRowsUpdated).sum,
        removed = operators.map(_.numRowsRemoved).sum,
        memoryBytes = memoryBytes,
        droppedByWatermark = droppedByWatermark)
      logInfo(
        s"event=streaming.query.state operators=${operators.length} " +
          s"rowsTotal=$total memoryBytes=$memoryBytes droppedByWatermark=$droppedByWatermark")
    }
  }

  private[metric] def recordStateValues(
      total: Long,
      updated: Long,
      removed: Long,
      memoryBytes: Long,
      droppedByWatermark: Long): Unit = {
    this.counter(StateRowsTotal).set(total)
    this.counter(StateRowsUpdated).set(updated)
    this.counter(StateRowsRemoved).set(removed)
    this.counter(StateMemoryBytes).set(memoryBytes)
    this.counter(StateRowsDroppedByWatermark).set(droppedByWatermark)
  }

  /**
   * Record the event-time watermark and its lag behind the driver clock. The watermark is
   * reported by Spark as an ISO-8601 timestamp string; "null" means the query has no watermark
   * yet and leaves the metric unchanged.
   */
  private[metric] def recordWatermark(
      eventTime: Map[String, String],
      nowMs: Long = System.currentTimeMillis()): Unit = {
    eventTime.get("watermark")
      .map(_.trim)
      .filter(value => value.nonEmpty && !value.equalsIgnoreCase("null"))
      .foreach { value =>
        try {
          val watermarkMs = java.time.Instant.parse(value).toEpochMilli
          val lagMs = math.max(0L, nowMs - watermarkMs)
          this.counter(LastWatermarkMs).set(watermarkMs)
          this.counter(WatermarkLagMs).set(lagMs)
          logInfo(
            s"event=streaming.query.watermark watermark=$value watermarkLagMs=$lagMs")
        } catch {
          case NonFatal(_) => ()
        }
      }
  }

  private[metric] def recordBatch(
      id: String,
      runId: String,
      batchId: Long,
      inputRows: Long,
      durationMs: Long): Unit = {
    this.counter(Batches).incrementAndGet()
    this.counter(InputRows).addAndGet(inputRows)
    this.counter(BatchDurationMs).addAndGet(durationMs)
    val last = this.counter(LastBatchId)
    var current = last.get()
    while (batchId > current && !last.compareAndSet(current, batchId)) {
      current = last.get()
    }
    logInfo(
      s"event=streaming.query.progress id=$id runId=$runId " +
        s"batch=$batchId inputRows=$inputRows batchDurationMs=$durationMs")
  }

  private[metric] def recordTerminated(
      id: String,
      runId: String,
      exception: Option[String]): Unit = {
    this.counter(ActiveQueries).updateAndGet(value => math.max(0L, value - 1L))
    val event = if (exception.isDefined) QueryFailed else QueryCompleted
    this.counter(event).incrementAndGet()
    // Do not log the exception text: it can contain a connection string or other configuration.
    logInfo(s"event=$event id=$id runId=$runId failed=${exception.isDefined}")
  }

  private class Listener extends StreamingQueryListener {

    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
      StreamingMetrics.this.recordStarted(
        event.id.toString, event.runId.toString, event.name)

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
      StreamingMetrics.this.recordProgress(event.progress)

    override def onQueryTerminated(
        event: StreamingQueryListener.QueryTerminatedEvent): Unit =
      StreamingMetrics.this.recordTerminated(
        event.id.toString, event.runId.toString, event.exception)
  }

  private class StreamingMetricsBean extends StreamingMetricsMBean {

    override def getSnapshot: String = Serialization.write(StreamingMetrics.snapshot())

    override def getCounter(name: String): Long = StreamingMetrics.value(name)

    override def resetMetrics(): Unit = StreamingMetrics.reset()
  }
}

/**
 * JMX view of the streaming counters and gauges.
 */
trait StreamingMetricsMBean {

  def getSnapshot: String

  def getCounter(name: String): Long

  def resetMetrics(): Unit
}
