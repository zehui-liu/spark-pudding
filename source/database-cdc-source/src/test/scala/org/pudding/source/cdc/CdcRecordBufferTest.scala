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

package org.pudding.source.cdc

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CdcRecordBufferTest extends AnyFunSuite with Matchers {

  test("checkpoint replay matches by sequence even when the connector cursor differs") {
    val committed = CdcOffset.debezium(Map("file" -> "binlog", "pos" -> 4L), 4L)
    val buffer = new CdcRecordBuffer("test", 10, 2, committed)
    // A connector can re-emit the committed cursor after a restart. The synthetic sequence is
    // what Spark recorded as the batch end, so replay must not depend on cursor equality.
    val replayed = CdcOffset.debezium(Map("file" -> "binlog", "pos" -> 4L), 5L)
    val checkpointEnd = CdcOffset.debezium(Map("file" -> "binlog", "pos" -> 5L), 5L)
    buffer.append(replayed, """{"id":1}""")

    buffer.plan(committed, checkpointEnd).toSeq shouldBe Seq("""{"id":1}""")
    buffer.commit(checkpointEnd)
    buffer.committedOffset.sequence shouldBe 5L
  }

  test("latest offset follows the last exposed cursor and caps the batch") {
    val committed = CdcOffset.debezium(Map("pos" -> 0L), 0L)
    val buffer = new CdcRecordBuffer("test", 10, 2, committed)
    val first = CdcOffset.debezium(Map("pos" -> 1L), 1L)
    val second = CdcOffset.debezium(Map("pos" -> 2L), 2L)
    val third = CdcOffset.debezium(Map("pos" -> 3L), 3L)
    buffer.append(first, "1")
    buffer.append(second, "2")
    buffer.append(third, "3")

    buffer.latestOffset(committed) shouldBe second
    buffer.latestOffset(second) shouldBe third
    buffer.latestOffset(third) shouldBe third
  }
}
