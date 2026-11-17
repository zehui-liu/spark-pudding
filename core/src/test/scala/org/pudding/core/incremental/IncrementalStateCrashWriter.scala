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

import org.apache.hadoop.conf.Configuration

import org.pudding.core.definition.IncrementalSemantics

/**
 * Test helper that writes a pending transaction and then dies without releasing the lock. It is
 * launched in a separate JVM by [[IncrementalStateCrashRecoveryTest]].
 */
object IncrementalStateCrashWriter {

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val ttlMillis = args(1).toLong
    val sourceId = "source"
    val transactionId = "txn-crash"
    val store = new IncrementalStateStore(
      new FileSystemStateBackend(path, new Configuration(), ttlMillis))
    store.acquire(sourceId)
    val plan = IncrementalPlanDescriptor.create(
      sourceType = "test-source",
      sourceId = sourceId,
      sinkType = "test-sink",
      sinkId = "sink",
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      sourceConfig = Some(Map("path" -> path)),
      sinkConfig = Some(Map("table" -> "target")),
      stateBackend = "filesystem")
    store.begin(sourceId, None, "42", transactionId, plan)
    System.out.println("pending transaction written")
    System.out.flush()
    // Simulate an abrupt process death: no shutdown hook, no lock release.
    Runtime.getRuntime.halt(17)
  }
}
