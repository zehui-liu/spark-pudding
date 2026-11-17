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

import javax.management.ObjectName

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IncrementalMetricsTest extends AnyFunSuite with Matchers {

  test("count and snapshot incremental metrics") {
    IncrementalMetrics.reset()
    IncrementalMetrics.value(IncrementalMetrics.LockAcquired) shouldBe 0L
    IncrementalMetrics.increment(IncrementalMetrics.LockAcquired)
    IncrementalMetrics.increment(IncrementalMetrics.LockAcquired, 2L)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionCommit)

    IncrementalMetrics.value(IncrementalMetrics.LockAcquired) shouldBe 3L
    IncrementalMetrics.snapshot() shouldBe Map(
      IncrementalMetrics.LockAcquired -> 3L,
      IncrementalMetrics.TransactionCommit -> 1L)
    IncrementalMetrics.value("missing") shouldBe 0L
    IncrementalMetrics.increment("", 5L)
    IncrementalMetrics.snapshot().keySet should not contain ""
    IncrementalMetrics.reset()
  }

  test("expose counters through JMX and register idempotently") {
    IncrementalMetrics.reset()
    IncrementalMetrics.increment(IncrementalMetrics.LockLost, 2L)
    IncrementalMetrics.registerJmx()
    IncrementalMetrics.registerJmx()
    IncrementalMetrics.isJmxRegistered shouldBe true

    val server = ManagementFactory.getPlatformMBeanServer
    val name = new ObjectName("org.pudding.core.incremental:type=IncrementalMetrics")
    server.isRegistered(name) shouldBe true
    val snapshot = server.getAttribute(name, "Snapshot").toString
    snapshot should include ("incremental.lock.lost")
    snapshot should include ("2")
    val count = server.invoke(
      name,
      "getCounter",
      Array[AnyRef](IncrementalMetrics.LockLost),
      Array[String](classOf[String].getName))
    count shouldBe 2L

    server.invoke(
      name,
      "resetMetrics",
      Array.empty[AnyRef],
      Array.empty[String])
    IncrementalMetrics.value(IncrementalMetrics.LockLost) shouldBe 0L
  }
}
