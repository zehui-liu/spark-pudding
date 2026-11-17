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

package org.pudding.source

import java.util.{Locale, Properties}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.admin.{Admin, OffsetSpec}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{
  StreamingQueryListener,
  StreamingQueryProgress
}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.pudding.core.PuddingException
import org.pudding.core.metric.StreamingMetrics

/**
 * Driver-side Kafka consumer-lag and health metrics.
 *
 * Spark 3.4 does not include consumer lag in `StreamingQueryProgress`. The source end offset
 * from progress is the offset Spark has consumed up to (exclusive); an AdminClient lookup of
 * the latest offsets for the same topic-partitions yields the number of records still pending.
 * The lookup is throttled and is never allowed to fail the streaming query.
 *
 * Only aggregate lag is exposed as metrics. Per-partition values are logged at DEBUG so a large
 * topic does not create an unbounded metric registry.
 */
private[source] object KafkaLagMetrics extends Logging {

  val IntervalKey = "lagMetricsIntervalMs"

  val DefaultIntervalMs = 30000L

  private val AdminTimeoutSeconds = 10L

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val sources = new ConcurrentHashMap[String, KafkaLagSource]()

  private val registeredSessions = ConcurrentHashMap.newKeySet[String]()

  case class LagSnapshot(records: Long, maxLag: Long, partitions: Long)

  private class KafkaLagSource(
      options: Map[String, String],
      intervalMs: Long) {

    private val lastEvaluationMs = new java.util.concurrent.atomic.AtomicLong(0L)

    def due(nowMs: Long): Boolean =
      nowMs - this.lastEvaluationMs.get() >= intervalMs

    def mark(nowMs: Long): Unit = this.lastEvaluationMs.set(nowMs)

    def measure(consumed: Map[TopicPartition, Long]): LagSnapshot = {
      if (consumed.isEmpty) {
        LagSnapshot(0L, 0L, 0L)
      } else {
        val admin = Admin.create(KafkaLagMetrics.adminProperties(options))
        try {
          val request = consumed.keys.map(partition => partition -> OffsetSpec.latest).toMap
          val latest = admin.listOffsets(request.asJava).all()
            .get(AdminTimeoutSeconds, TimeUnit.SECONDS).asScala.toMap
          val latestOffsets = latest.map { case (partition, info) =>
            partition -> info.offset()
          }
          KafkaLagMetrics.lagSnapshot(consumed, latestOffsets)
        } finally {
          admin.close()
        }
      }
    }
  }

  private class Listener(applicationId: String) extends StreamingQueryListener {

    override def onQueryStarted(
        event: StreamingQueryListener.QueryStartedEvent): Unit = ()

    override def onQueryProgress(
        event: StreamingQueryListener.QueryProgressEvent): Unit =
      KafkaLagMetrics.record(applicationId, event.progress)

    override def onQueryTerminated(
        event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
  }

  /**
   * Register the Kafka reader options for this Spark application. A streaming job has one
   * source, so the latest registration for an application is authoritative.
   */
  def register(
      session: SparkSession,
      options: Map[String, String],
      intervalMs: Long): Unit = this.synchronized {
    if (intervalMs <= 0L) {
      throw new PuddingException(s"$IntervalKey must be positive")
    }
    val applicationId = session.sparkContext.applicationId
    sources.put(applicationId, new KafkaLagSource(options, intervalMs))
    if (registeredSessions.add(applicationId)) {
      session.streams.addListener(new Listener(applicationId))
    }
  }

  /**
   * Parse the `lagMetricsIntervalMs` source setting.
   */
  def interval(config: Map[String, Any]): Long = {
    config.get(IntervalKey).filter(_ != null) match {
      case None => DefaultIntervalMs
      case Some(value: java.lang.Number) =>
        validateInterval(value.longValue())
      case Some(value: String) =>
        try {
          validateInterval(value.trim.toLong)
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$IntervalKey must be a positive integer")
        }
      case Some(_) =>
        throw new PuddingException(s"$IntervalKey must be a positive integer")
    }
  }

  private def validateInterval(value: Long): Long = {
    if (value <= 0L) {
      throw new PuddingException(s"$IntervalKey must be positive")
    }
    value
  }

  private[source] def record(
      applicationId: String,
      progress: StreamingQueryProgress): Unit = this.synchronized {
    val source = sources.get(applicationId)
    if (source != null && progress != null) {
      val now = System.currentTimeMillis()
      if (source.due(now)) {
        val kafkaSources = Option(progress.sources).getOrElse(Array.empty)
          .filter(sourceProgress =>
            Option(sourceProgress.description)
              .exists(_.trim.toLowerCase(Locale.ROOT).startsWith("kafka")))
        if (kafkaSources.nonEmpty) {
          source.mark(now)
          try {
            val consumed = kafkaSources.flatMap { sourceProgress =>
              this.parseOffsets(sourceProgress.endOffset)
            }.toMap
            val snapshot = source.measure(consumed)
            StreamingMetrics.recordSourceLag(
              snapshot.records,
              snapshot.maxLag,
              snapshot.partitions,
              healthy = true,
              updatedMs = now)
          } catch {
            case NonFatal(error) =>
              StreamingMetrics.recordSourceLagFailure(updatedMs = now)
              // Kafka errors can contain broker configuration; log only the class name.
              logWarning(
                s"event=streaming.source.lag.failed source=kafka " +
                  s"error=${error.getClass.getSimpleName}")
          }
        }
      }
    }
  }

  private[source] def parseOffsets(value: String): Map[TopicPartition, Long] = {
    if (value == null || value.trim.isEmpty) {
      Map.empty
    } else {
      try {
        Serialization.read[Map[String, Map[String, Long]]](value).flatMap {
          case (topic, partitions) =>
            partitions.map { case (partition, offset) =>
              new TopicPartition(topic, partition.toInt) -> offset
            }
        }
      } catch {
        case NonFatal(_) =>
          throw new PuddingException("invalid kafka source offset JSON in query progress")
      }
    }
  }

  private[source] def lagSnapshot(
      consumed: Map[TopicPartition, Long],
      latest: Map[TopicPartition, Long]): LagSnapshot = {
    val lags = consumed.iterator
      .filter { case (_, offset) => offset >= 0L }
      .map { case (partition, offset) =>
        val latestOffset = latest.getOrElse(partition, offset)
        val lag = math.max(0L, latestOffset - offset)
        logDebug(
          s"event=streaming.source.lag.partition topic=${partition.topic} " +
            s"partition=${partition.partition} lag=$lag")
        lag
      }
      .toVector
    if (lags.isEmpty) {
      LagSnapshot(0L, 0L, 0L)
    } else {
      LagSnapshot(lags.sum, lags.max, lags.size.toLong)
    }
  }

  private[source] def adminProperties(
      options: Map[String, String]): Properties = {
    val properties = new Properties()
    options.foreach { case (key, value) =>
      if (key.startsWith("kafka.")) {
        properties.put(key.substring("kafka.".length), value)
      }
    }
    if (!properties.containsKey("bootstrap.servers")) {
      throw new PuddingException(
        "kafka lag metrics require kafka.bootstrap.servers")
    }
    properties
  }
}
