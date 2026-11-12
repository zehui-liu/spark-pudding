/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

import org.pudding.core.PuddingException

/**
 * Parses the job-level structured-streaming trigger.
 *
 * Supported forms:
 *  - `once`
 *  - `available-now` / `availableNow`
 *  - `processing-time: 30 seconds` (or a bare Spark duration such as `30 seconds`)
 *  - `continuous: 1 second`
 */
object StreamingTrigger {

  def parse(value: String): Trigger = {
    val text = Option(value).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("streaming trigger can not be empty")
    }
    val normalized = text.toLowerCase(Locale.ROOT)
    try {
      if (normalized == "once") {
        // Spark 3.4 marks Trigger.Once deprecated in favour of AvailableNow. Keep the existing
        // job spelling for compatibility; AvailableNow processes the same finite input in
        // micro-batches and additionally supports rate limits.
        Trigger.Once()
      } else if (normalized == "available-now" || normalized == "availablenow") {
        Trigger.AvailableNow()
      } else if (normalized.startsWith("continuous:")) {
        Trigger.Continuous(this.afterColon(text))
      } else if (normalized.startsWith("processing-time:")) {
        Trigger.ProcessingTime(this.afterColon(text))
      } else if (normalized.startsWith("processingtime:")) {
        Trigger.ProcessingTime(this.afterColon(text))
      } else {
        Trigger.ProcessingTime(text)
      }
    } catch {
      case error: IllegalArgumentException =>
        throw new PuddingException(
          s"invalid streaming trigger: $value: ${error.getMessage}", error)
    }
  }

  def validate(value: String): Unit = {
    this.parse(value)
    ()
  }

  /**
   * A finite trigger stops by itself after consuming the data available at start. Checkpoint
   * cleanup may only run after such a query terminates; a processing-time or continuous query
   * is expected to run until it is stopped.
   */
  def isFinite(trigger: Option[String]): Boolean = trigger.exists { value =>
    val normalized = value.trim.toLowerCase(Locale.ROOT)
    normalized == "once" || normalized == "available-now" || normalized == "availablenow"
  }

  def applyTo(writer: DataStreamWriter[_], trigger: Option[String]): DataStreamWriter[_] = {
    trigger match {
      case Some(value) => writer.trigger(this.parse(value))
      case None => writer
    }
  }

  private def afterColon(value: String): String = {
    val duration = value.substring(value.indexOf(':') + 1).trim
    if (duration.isEmpty) {
      throw new PuddingException(s"streaming trigger is missing a duration: $value")
    }
    duration
  }
}
