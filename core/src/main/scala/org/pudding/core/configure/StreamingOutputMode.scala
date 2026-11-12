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

package org.pudding.core.configure

import java.util.Locale

import org.pudding.core.PuddingException

/**
 * Structured Streaming output modes accepted by sinks.
 *
 * `append` emits only new rows. `update` emits rows changed since the previous batch, which a
 * sink must merge by key to avoid duplicates. `complete` emits the whole result table each
 * batch, which is only safe for display sinks or a sink that replaces the entire target.
 */
object StreamingOutputMode {

  val Key = "outputMode"

  val Append = "append"

  val Update = "update"

  val Complete = "complete"

  val All: Set[String] = Set(Append, Update, Complete)

  /**
   * Read and normalize the configured output mode. An absent or null value means the sink's
   * default. Invalid values are rejected before a streaming query starts.
   */
  def configured(config: Option[Map[String, Any]]): Option[String] = {
    config.flatMap(_.get(Key).filter(_ != null)).map {
      case value: String => normalize(value)
      case _ =>
        throw new PuddingException(s"$Key must be a string!")
    }
  }

  /**
   * Resolve a configured mode or use append as the default. Also verifies the sink declaration
   * so a sink can not silently accept a mode that Spark or its writer does not support.
   */
  def resolve(
      config: Option[Map[String, Any]],
      sinkType: String,
      supported: Set[String]): String = {
    val mode = this.configured(config).getOrElse(Append)
    this.requireSupported(mode, sinkType, supported)
  }

  /**
   * Validate one already-normalized mode against a sink declaration.
   */
  def requireSupported(
      mode: String,
      sinkType: String,
      supported: Set[String]): String = {
    val normalized = this.normalize(mode)
    if (!supported.contains(normalized)) {
      throw new PuddingException(
        s"sink '$sinkType' does not support streaming $Key=$normalized; supported: " +
          supported.toSeq.sorted.mkString(", "))
    }
    normalized
  }

  def normalize(value: String): String = {
    if (value == null) {
      throw new PuddingException(s"$Key can not be null")
    }
    val normalized = value.trim.toLowerCase(Locale.ROOT)
    if (normalized.isEmpty) {
      throw new PuddingException(s"$Key can not be empty")
    }
    if (!All.contains(normalized)) {
      throw new PuddingException(
        s"unsupported streaming $Key; expected one of " +
          All.toSeq.sorted.mkString(", "))
    }
    normalized
  }
}
