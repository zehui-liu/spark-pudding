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

package org.pudding.core.util

import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

import org.pudding.core.PuddingException

/**
 * Checkpoint lifecycle policy shared by every streaming sink.
 *
 * A streaming checkpoint holds source progress and query metadata. Deleting it makes the next
 * run start again from the source's configured starting position, so the default is to keep it.
 * `checkpointRetention=delete-on-success` is opt-in and only valid for a finite trigger
 * (`once` or `available-now`), where the query is expected to consume the available data and
 * terminate by itself. Because Spark does not distinguish a natural finite completion from an
 * external `stop()`, deletion requires completion evidence from query progress.
 */
object StreamingCheckpoint extends Logging {

  val LocationKey = "checkpointLocation"

  val RetentionKey = "checkpointRetention"

  val KeepValue = "keep"

  val DeleteOnSuccessValue = "delete-on-success"

  /**
   * Spark emits a final zero-row progress update when an `available-now` query observes that no
   * data remains. The default ten-second throttle can suppress that update for short finite
   * queries, so deletion temporarily disables the throttle. This configuration only affects
   * progress reporting frequency.
   */
  private val NoDataProgressEventIntervalKey =
    "spark.sql.streaming.noDataProgressEventInterval"

  private val DefaultNoDataProgressEventInterval = "10000"

  private val savedNoDataProgressEventInterval = new AtomicReference[String]()

  sealed trait RetentionPolicy {
    def name: String
  }

  case object KeepPolicy extends RetentionPolicy {
    override val name: String = KeepValue
  }

  case object DeleteOnSuccessPolicy extends RetentionPolicy {
    override val name: String = DeleteOnSuccessValue
  }

  /**
   * Resolve the checkpoint retention policy. An absent value means `keep`.
   */
  def retention(cfg: Map[String, Any]): RetentionPolicy = {
    cfg.get(RetentionKey).filter(_ != null) match {
      case None =>
        KeepPolicy
      case Some(value: String) =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case KeepValue => KeepPolicy
          case DeleteOnSuccessValue => DeleteOnSuccessPolicy
          case _ =>
            throw new PuddingException(
              s"$RetentionKey must be '$KeepValue' or '$DeleteOnSuccessValue'")
        }
      case Some(_) =>
        throw new PuddingException(
          s"$RetentionKey must be a string: '$KeepValue' or '$DeleteOnSuccessValue'")
    }
  }

  /**
   * Validate the policy before a streaming query starts. `location` is the checkpoint the sink
   * resolved from its configuration or options. When `stateContract` is present, the checkpoint
   * must have been created by the same source/transform configuration.
   *
   * `delete-on-success` also enables the zero-row progress update used to prove that an
   * `available-now` query drained its input before the checkpoint is removed.
   */
  def validate(
      cfg: Map[String, Any],
      trigger: Option[String],
      location: Option[String],
      stateContract: Option[String] = None): RetentionPolicy = {
    val policy = this.retention(cfg)
    if (policy == DeleteOnSuccessPolicy) {
      if (!StreamingTrigger.isFinite(trigger)) {
        throw new PuddingException(
          s"$RetentionKey=$DeleteOnSuccessValue requires a finite trigger " +
            "('once' or 'available-now')")
      }
      if (location.forall(_.trim.isEmpty)) {
        throw new PuddingException(
          s"$RetentionKey=$DeleteOnSuccessValue requires $LocationKey")
      }
    }
    stateContract.foreach { contract =>
      val checkpoint = location.map(_.trim).filter(_.nonEmpty).getOrElse {
        throw new PuddingException(
          s"streaming state contract requires $LocationKey")
      }
      StreamingStateContract.verify(
        checkpoint, contract, this.hadoopConfiguration)
    }
    if (policy == DeleteOnSuccessPolicy && this.isAvailableNow(trigger)) {
      this.enableDrainProgress()
    }
    policy
  }

  /**
   * Wait for a streaming query to terminate and apply the checkpoint policy.
   *
   * Spark reports an external `query.stop()` and a naturally finished finite trigger in the same
   * way: no exception and an inactive query. Deletion therefore also requires positive evidence
   * that the finite run drained its input. A failed, timed-out or externally stopped query keeps
   * its checkpoint.
   */
  def awaitTermination(
      query: StreamingQuery,
      cfg: Map[String, Any],
      trigger: Option[String],
      location: Option[String]): Unit = {
    this.awaitTermination(query, cfg, trigger, location, None)
  }

  /**
   * Wait up to `timeoutMs` and apply the checkpoint policy. A timeout is not a successful
   * termination, so the checkpoint is kept.
   */
  def awaitTermination(
      query: StreamingQuery,
      cfg: Map[String, Any],
      trigger: Option[String],
      location: Option[String],
      timeoutMs: Long): Unit = {
    this.awaitTermination(query, cfg, trigger, location, Some(timeoutMs))
  }

  private def awaitTermination(
      query: StreamingQuery,
      cfg: Map[String, Any],
      trigger: Option[String],
      location: Option[String],
      timeoutMs: Option[Long]): Unit = {
    val policy = this.validate(cfg, trigger, location)
    var completed = false
    try {
      timeoutMs match {
        case Some(value) => query.awaitTermination(value)
        case None => query.awaitTermination()
      }
      completed = query.exception.isEmpty && !query.isActive
    } finally {
      try {
        if (policy == DeleteOnSuccessPolicy) {
          if (completed && this.completedFiniteRun(query, trigger, location.get)) {
            this.deleteCheckpoint(location.get)
          } else {
            logWarning(
              s"streaming query for $LocationKey did not prove that its input was drained; " +
                "its checkpoint was kept")
          }
        }
      } finally {
        if (policy == DeleteOnSuccessPolicy) {
          this.restoreDrainProgress()
        }
      }
    }
  }

  /**
   * Return true only when the query itself produced evidence that its finite input was consumed.
   *
   * An `available-now` query terminates after a trigger that constructed no next batch. That
   * trigger emits a zero-row progress update with the next (uncommitted) batch id; the batch id is
   * therefore higher than the latest committed offset. A zero-row cleanup batch commits an offset
   * and keeps the ids equal, so it is not accepted as completion evidence. A `once` query runs a
   * single batch: a progress update means that batch committed.
   */
  private def completedFiniteRun(
      query: StreamingQuery,
      trigger: Option[String],
      location: String): Boolean = {
    val progress = Option(query.lastProgress).filter(_.batchId >= 0L)
    progress.exists { last =>
      if (this.isOnce(trigger)) {
        true
      } else if (this.isAvailableNow(trigger)) {
        if (last.numInputRows != 0L) {
          false
        } else {
          try {
            val path = new Path(location)
            val fileSystem = path.getFileSystem(this.hadoopConfiguration)
            CheckpointFiles.latestOffsetBatchId(fileSystem, path)
              .forall(last.batchId > _)
          } catch {
            case NonFatal(_) => false
          }
        }
      } else {
        false
      }
    }
  }

  private def isOnce(trigger: Option[String]): Boolean = {
    trigger.exists(_.trim.toLowerCase(Locale.ROOT) == "once")
  }

  private def isAvailableNow(trigger: Option[String]): Boolean = {
    trigger.exists { value =>
      val normalized = value.trim.toLowerCase(Locale.ROOT)
      normalized == "available-now" || normalized == "availablenow"
    }
  }

  private def enableDrainProgress(): Unit = {
    SparkSession.getActiveSession.foreach { session =>
      this.synchronized {
        if (savedNoDataProgressEventInterval.get() == null) {
          savedNoDataProgressEventInterval.set(
            session.conf.getOption(NoDataProgressEventIntervalKey)
              .getOrElse(DefaultNoDataProgressEventInterval))
        }
        session.conf.set(NoDataProgressEventIntervalKey, "0")
      }
    }
  }

  private def restoreDrainProgress(): Unit = {
    val previous = this.synchronized {
      savedNoDataProgressEventInterval.getAndSet(null)
    }
    if (previous != null) {
      SparkSession.getActiveSession.foreach { session =>
        session.conf.set(NoDataProgressEventIntervalKey, previous)
      }
    }
  }

  /**
   * Recursively delete one checkpoint directory using the active Spark session's Hadoop
   * configuration, so Spark-level HDFS/S3A settings and credential providers are available.
   */
  def deleteCheckpoint(location: String): Unit = {
    if (CheckpointFiles.deleteCheckpoint(location, this.hadoopConfiguration)) {
      logInfo(s"deleted streaming checkpoint at $location")
    }
  }

  private def hadoopConfiguration: Configuration = {
    SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf)
      .getOrElse(new Configuration())
  }
}
