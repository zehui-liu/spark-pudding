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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.TimestampType

import org.pudding.core.PuddingException

/**
 * Event-time window settings for a stateful streaming transform.
 */
private[transform] final case class StreamingWindowConfig(
    duration: String,
    slide: Option[String])

/**
 * Event-time and watermark settings shared by the stateful streaming transforms.
 *
 * A watermark is mandatory for every stateful operator in this module. Spark uses it to close
 * event-time windows and to evict deduplication state; without it the state store would grow
 * without an upper bound. The watermark delay is therefore also the state-retention boundary.
 */
private[transform] final case class StreamingStateConfig(
    eventTime: String,
    watermark: String,
    window: Option[StreamingWindowConfig])

private[transform] object StreamingStateSupport {

  val EventTimeKey = "eventTime"

  val WatermarkKey = "watermark"

  val WindowKey = "window"

  val SlideKey = "slide"

  val WindowStartColumn = "window_start"

  val WindowEndColumn = "window_end"

  private val StreamingKeys = Seq(EventTimeKey, WatermarkKey, WindowKey, SlideKey)

  private val WindowKeys = Set("duration", "slide")

  def rejectStreamingKeys(
      cfg: Map[String, Any],
      transform: String): Unit = {
    val configured = StreamingKeys.filter(key => this.configured(cfg, key))
    if (configured.nonEmpty) {
      throw new PuddingException(
        s"$transform ${configured.mkString(", ")} can only be used in streaming execution")
    }
  }

  def validate(
      cfg: Map[String, Any],
      transform: String,
      requireWindow: Boolean): StreamingStateConfig = {
    val eventTime = TransformSupport.optionalString(cfg, EventTimeKey).getOrElse {
      throw new PuddingException(
        s"$transform streaming requires $EventTimeKey for event-time processing")
    }
    val watermark = TransformSupport.optionalString(cfg, WatermarkKey).getOrElse {
      throw new PuddingException(
        s"$transform streaming requires $WatermarkKey; state is otherwise unbounded")
    }
    val window = if (requireWindow) {
      Some(this.window(cfg, transform))
    } else {
      if (this.configured(cfg, WindowKey) || this.configured(cfg, SlideKey)) {
        throw new PuddingException(
          s"$transform does not support $WindowKey or $SlideKey; they are only valid for " +
            "aggregate-transform")
      }
      None
    }
    StreamingStateConfig(eventTime, watermark, window)
  }

  /**
   * Resolve a column name case-insensitively and return its schema spelling. The caller uses the
   * returned name for watermark and grouping references so Spark does not have to guess.
   */
  def resolveColumns(
      dataFrame: DataFrame,
      names: Seq[String],
      key: String): Seq[String] = {
    val resolved = names.map { name =>
      dataFrame.schema.fields
        .find(_.name.equalsIgnoreCase(name))
        .map(_.name)
        .getOrElse(throw new PuddingException(s"$key column not found: $name"))
    }
    val duplicates = resolved
      .groupBy(_.toLowerCase(Locale.ROOT))
      .collect { case (_, values) if values.size > 1 => values.head }
      .toSeq
      .sorted
    if (duplicates.nonEmpty) {
      throw new PuddingException(
        s"$key contains duplicate columns: ${duplicates.mkString(", ")}")
    }
    resolved
  }

  /**
   * Resolve the event-time column and ensure it is a timestamp. Spark watermarks are only
   * defined for event-time columns of timestamp type.
   */
  def timestampColumn(dataFrame: DataFrame, name: String): String = {
    val field = dataFrame.schema.fields
      .find(_.name.equalsIgnoreCase(name))
      .getOrElse(throw new PuddingException(s"$EventTimeKey column not found: $name"))
    field.dataType match {
      case TimestampType => field.name
      case other =>
        throw new PuddingException(
          s"$EventTimeKey column '$name' must be a timestamp, but got: ${other.simpleString}")
    }
  }

  def validateWindowOutputNames(
      names: Seq[String],
      transform: String): Unit = {
    val reserved = Set(
      WindowStartColumn.toLowerCase(Locale.ROOT),
      WindowEndColumn.toLowerCase(Locale.ROOT))
    val conflicts = names
      .filter(name => reserved.contains(name.toLowerCase(Locale.ROOT)))
      .distinct
    if (conflicts.nonEmpty) {
      throw new PuddingException(
        s"$transform output columns conflict with the window columns: " +
          conflicts.mkString(", "))
    }
  }

  private def window(
      cfg: Map[String, Any],
      transform: String): StreamingWindowConfig = {
    cfg.get(WindowKey).filter(_ != null) match {
      case None =>
        throw new PuddingException(
          s"$transform streaming requires $WindowKey for a bounded aggregation window")
      case Some(value: String) =>
        StreamingWindowConfig(
          this.text(value, WindowKey),
          TransformSupport.optionalString(cfg, SlideKey))
      case Some(value: Map[_, _]) =>
        this.windowMap(
          value.map { case (key, item) => key.toString -> item }.toMap,
          cfg,
          transform)
      case Some(value: java.util.Map[_, _]) =>
        this.windowMap(
          value.asScala.map { case (key, item) => key.toString -> item }.toMap,
          cfg,
          transform)
      case Some(other) =>
        throw new PuddingException(
          s"$WindowKey must be a duration string or a map with duration and slide, " +
            s"but got: ${other.getClass.getSimpleName}")
    }
  }

  private def windowMap(
      values: Map[String, Any],
      cfg: Map[String, Any],
      transform: String): StreamingWindowConfig = {
    val unknown = values.keySet.diff(WindowKeys)
    if (unknown.nonEmpty) {
      throw new PuddingException(
        s"$transform $WindowKey contains unknown keys: " +
          unknown.toSeq.sorted.mkString(", "))
    }
    if (this.configured(cfg, SlideKey)) {
      throw new PuddingException(
        s"$transform $SlideKey can not be used together with a $WindowKey map")
    }
    val duration = values.get("duration").map(value => this.text(value, s"$WindowKey.duration"))
      .getOrElse(throw new PuddingException(
        s"$transform $WindowKey map requires duration"))
    val slide = values.get("slide").map(value => this.text(value, s"$WindowKey.slide"))
    StreamingWindowConfig(duration, slide)
  }

  private def configured(cfg: Map[String, Any], key: String): Boolean = {
    cfg.get(key).exists(_ != null)
  }

  private def text(value: Any, key: String): String = value match {
    case item: String if item.trim.nonEmpty => item.trim
    case _ => throw new PuddingException(s"$key must be a non-empty string")
  }
}
