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

import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import javax.management.{ObjectName, StandardMBean}

import scala.util.control.NonFatal

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

/**
 * Driver-side counters for the incremental state machine.
 *
 * Counters are aggregate-only: source ids and lock owners belong in the audit log, not in a
 * metrics registry that may have a different retention or access policy. `registerJmx` exposes
 * the snapshot and a counter lookup under
 * `org.pudding.core.incremental:type=IncrementalMetrics`; the job also logs the snapshot on
 * completion.
 */
object IncrementalMetrics {

  val LockAcquired = "incremental.lock.acquired"

  val LockRenewed = "incremental.lock.renewed"

  val LockTakeover = "incremental.lock.takeover"

  val LockLost = "incremental.lock.lost"

  val LockReleased = "incremental.lock.released"

  val LockForced = "incremental.lock.forced"

  val TransactionBegin = "incremental.transaction.begin"

  val TransactionCommit = "incremental.transaction.commit"

  val TransactionAbort = "incremental.transaction.abort"

  val TransactionReset = "incremental.transaction.reset"

  val TransactionAttempt = "incremental.transaction.attempt"

  val TransactionReplay = "incremental.transaction.replay"

  val TransactionFailure = "incremental.transaction.failure"

  val StateWriteFailure = "incremental.state.write.failure"

  val RunStarted = "incremental.run.started"

  val RunCompleted = "incremental.run.completed"

  val RunFailed = "incremental.run.failed"

  val SinkFencingRejected = "incremental.sink.fencing.rejected"

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val counters = new ConcurrentHashMap[String, AtomicLong]()

  private val MBeanName = new ObjectName(
    "org.pudding.core.incremental:type=IncrementalMetrics")

  @volatile private var registeredJmx = false

  def increment(name: String, delta: Long = 1L): Unit = {
    if (name != null && name.nonEmpty) {
      this.counter(name).addAndGet(delta)
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

  /** Clear counters; used by tests and long-lived embedding processes. */
  def reset(): Unit = this.counters.clear()

  /**
   * Register the JMX view once per process. A runtime without JMX must not fail a job; the
   * in-process counters and the completion log remain available.
   */
  def registerJmx(): Unit = this.synchronized {
    if (!this.registeredJmx) {
      try {
        val server = ManagementFactory.getPlatformMBeanServer
        if (!server.isRegistered(this.MBeanName)) {
          server.registerMBean(
            new StandardMBean(new IncrementalMetricsBean(), classOf[IncrementalMetricsMBean]),
            this.MBeanName)
        }
        this.registeredJmx = true
      } catch {
        case NonFatal(_) => // Leave isJmxRegistered false so the failure is observable.
      }
    }
  }

  def isJmxRegistered: Boolean = this.registeredJmx

  private def counter(name: String): AtomicLong = {
    this.counters.computeIfAbsent(name, _ => new AtomicLong())
  }

  private class IncrementalMetricsBean extends IncrementalMetricsMBean {

    override def getSnapshot: String = Serialization.write(IncrementalMetrics.snapshot())

    override def getCounter(name: String): Long = IncrementalMetrics.value(name)

    override def resetMetrics(): Unit = IncrementalMetrics.reset()
  }
}

/**
 * JMX view of the incremental counters.
 */
trait IncrementalMetricsMBean {

  def getSnapshot: String

  def getCounter(name: String): Long

  def resetMetrics(): Unit
}
