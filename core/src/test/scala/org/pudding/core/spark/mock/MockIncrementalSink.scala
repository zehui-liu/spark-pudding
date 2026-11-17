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

package org.pudding.core.spark.mock

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame

import org.pudding.core.definition.{DataSink, IncrementalSemantics, IncrementalWrite, SinkContext}

/**
 * Sink that advertises every incremental semantic without writing data.
 *
 * The optional `markerDir` config writes one marker per transaction id, so tests can verify
 * exactly-once sink application across a process crash. `crashAfterWrite` halts the JVM after
 * the marker is durable, simulating a crash between the sink commit and the cursor commit.
 */
class MockIncrementalSink extends DataSink {

  override def identifier: String = "mock_incremental_sink"

  override def supportsLakeIncremental: Boolean = true

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    IncrementalSemantics.values

  override def isIdempotentIncremental(semantics: IncrementalSemantics): Boolean =
    semantics != IncrementalSemantics.Append

  override def writeIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: IncrementalWrite): Unit = {
    val config = context.config.getOrElse(Map.empty)
    if (config.get("fail").contains(true)) {
      throw new org.pudding.core.PuddingException("mock incremental sink failure")
    }
    config.get("markerDir").foreach { value =>
      val directory = Paths.get(value.toString)
      Files.createDirectories(directory)
      val marker = directory.resolve(incremental.transactionId + ".marker")
      if (!Files.exists(marker)) {
        val content =
          s"source=${incremental.sourceId}\n" +
            s"transaction=${incremental.transactionId}\n" +
            s"from=${incremental.fromCursor.getOrElse("")}\n" +
            s"to=${incremental.toCursor}\n" +
            s"fencingToken=${incremental.fencingToken}\n"
        Files.write(marker, content.getBytes(StandardCharsets.UTF_8))
      }
    }
    val crash = config.get("crashAfterWrite").contains(true) ||
      sys.env.get("PUDDING_TEST_CRASH_AFTER_SINK").contains("true")
    if (crash) {
      Runtime.getRuntime.halt(17)
    }
  }

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {}
}
