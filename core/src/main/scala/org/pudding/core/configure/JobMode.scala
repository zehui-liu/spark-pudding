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
 * User-facing job mode. The mode selects the processing contract and the execution engine.
 *
 * - `batch` is general bounded processing with arbitrary sources and transforms, and one sink.
 * - `sync` is a bounded one-source/one-sink synchronization; its strategy is `full` or
 *   `incremental`.
 * - `streaming` is continuous Structured Streaming processing with one source and one sink.
 */
sealed trait JobMode {
  def name: String

  def executionMode: ExecutionMode
}

object JobMode {

  case object Batch extends JobMode {
    override val name: String = "batch"

    override val executionMode: ExecutionMode = ExecutionMode.Batch
  }

  case object Sync extends JobMode {
    override val name: String = "sync"

    override val executionMode: ExecutionMode = ExecutionMode.Batch
  }

  case object Streaming extends JobMode {
    override val name: String = "streaming"

    override val executionMode: ExecutionMode = ExecutionMode.Streaming
  }

  val values: Set[JobMode] = Set(Batch, Sync, Streaming)

  def parse(value: String, field: String = "job.mode"): JobMode = {
    val normalized = Option(value).map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .getOrElse(throw new PuddingException(s"$field can not be empty!"))
    this.values.find(_.name == normalized).getOrElse {
      normalized match {
        case "full" | "incremental" =>
          throw new PuddingException(
            s"$field '$value' is no longer supported; use job.mode: sync with " +
              s"job.strategy: $normalized")
        case "etl" =>
          throw new PuddingException(
            s"$field 'etl' is no longer supported; use job.mode: batch for bounded " +
              "processing or job.mode: streaming for continuous processing")
        case _ =>
          throw new PuddingException(
            s"unsupported $field: $value, supported: " +
              this.values.map(_.name).toSeq.sorted.mkString(", "))
      }
    }
  }
}

/**
 * Sync strategy. It is required only for `job.mode: sync` and is never combined with streaming.
 */
sealed trait SyncStrategy {
  def name: String
}

object SyncStrategy {

  case object Full extends SyncStrategy {
    override val name: String = "full"
  }

  case object Incremental extends SyncStrategy {
    override val name: String = "incremental"
  }

  val values: Set[SyncStrategy] = Set(Full, Incremental)

  def parse(value: String, field: String = "job.strategy"): SyncStrategy = {
    val normalized = Option(value).map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .getOrElse(throw new PuddingException(s"$field can not be empty!"))
    this.values.find(_.name == normalized).getOrElse {
      throw new PuddingException(
        s"unsupported $field: $value, supported: " +
          this.values.map(_.name).toSeq.sorted.mkString(", "))
    }
  }
}

/**
 * Internal execution engine selected by [[JobMode]]. Sources, sinks and transforms declare
 * support for these values; users select a job mode instead of setting this directly.
 */
sealed trait ExecutionMode {
  def name: String
}

object ExecutionMode {

  case object Batch extends ExecutionMode {
    override val name: String = "batch"
  }

  case object Streaming extends ExecutionMode {
    override val name: String = "streaming"
  }
}
