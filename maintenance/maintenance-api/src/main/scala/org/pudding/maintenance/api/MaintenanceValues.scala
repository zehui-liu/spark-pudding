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

package org.pudding.maintenance.api

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneOffset}
import java.util.Locale

/**
 * Parsing and normalization helpers shared by the CLI, validation and Spark adapters.
 */
object MaintenanceValues {

  private val DurationPattern = "^([0-9]+)(ms|s|m|h|d|w)$".r

  private val SizePattern = "^([0-9]+)([a-zA-Z]*)$".r

  private val TimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT)
      .withZone(ZoneOffset.UTC)

  def normalize(value: String): String = {
    Option(value).getOrElse("").trim.toLowerCase(Locale.ROOT)
  }

  def trimmed(value: Option[String]): Option[String] = {
    value.map(_.trim).filter(_.nonEmpty)
  }

  def duration(value: String): Duration = {
    val text = Option(value).getOrElse("").trim.toLowerCase(Locale.ROOT)
    text match {
      case DurationPattern(amount, unit) =>
        val count = try amount.toLong
        catch {
          case _: NumberFormatException =>
            throw new MaintenanceUsageException(s"invalid duration: $value")
        }
        if (count <= 0L) {
          throw new MaintenanceUsageException(s"duration must be greater than zero: $value")
        }
        try {
          unit match {
            case "ms" => Duration.ofMillis(count)
            case "s" => Duration.ofSeconds(count)
            case "m" => Duration.ofMinutes(count)
            case "h" => Duration.ofHours(count)
            case "d" => Duration.ofDays(count)
            case "w" => Duration.ofDays(Math.multiplyExact(count, 7L))
            case _ => throw new MaintenanceUsageException(s"invalid duration unit: $value")
          }
        } catch {
          case _: ArithmeticException =>
            throw new MaintenanceUsageException(s"duration is out of range: $value")
        }
      case _ =>
        throw new MaintenanceUsageException(
          s"invalid duration '$value'; use a positive number with ms, s, m, h, d or w")
    }
  }

  /**
   * Format a duration as a UTC timestamp string accepted by Spark timestamp literals and the
   * Paimon expiry procedures.
   */
  def olderThanTimestamp(value: String, now: Instant = Instant.now()): String = {
    TimestampFormat.format(now.minus(this.duration(value)))
  }

  /**
   * Parse a byte size. The suffix is binary (KB=1024, MB=1024^2, ...); a plain number is bytes.
   */
  def size(value: String): Long = {
    val text = Option(value).getOrElse("").trim.toUpperCase(Locale.ROOT)
    text match {
      case SizePattern(amount, suffix) =>
        val count = try amount.toLong
        catch {
          case _: NumberFormatException =>
            throw new MaintenanceUsageException(s"invalid size: $value")
        }
        val multiplier = suffix match {
          case "" | "B" => 1L
          case "K" | "KB" => 1024L
          case "M" | "MB" => 1024L * 1024L
          case "G" | "GB" => 1024L * 1024L * 1024L
          case "T" | "TB" => 1024L * 1024L * 1024L * 1024L
          case _ =>
            throw new MaintenanceUsageException(
              s"invalid size '$value'; use B, KB, MB, GB or TB")
        }
        if (count <= 0L) {
          throw new MaintenanceUsageException(s"size must be greater than zero: $value")
        }
        try {
          Math.multiplyExact(count, multiplier)
        } catch {
          case _: ArithmeticException =>
            throw new MaintenanceUsageException(s"size is out of range: $value")
        }
      case _ =>
        throw new MaintenanceUsageException(
          s"invalid size '$value'; use a positive number with B, KB, MB, GB or TB")
    }
  }
}
