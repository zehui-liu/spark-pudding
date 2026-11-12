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

package org.pudding.source.cdc

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException

class CdcOffsetTest extends AnyFunSuite with Matchers {

  test("initial and cursor offsets round-trip through their JSON form") {
    val initial = CdcOffset.initial()
    CdcOffset.parse(initial.json()) shouldBe initial

    val cursor = CdcOffset.debezium(
      Map("file" -> "mysql-bin.000001", "pos" -> 123L, "snapshot" -> false), 9L)
    val parsed = CdcOffset.parse(cursor.json())
    parsed shouldBe cursor
    parsed.kind shouldBe CdcOffset.DebeziumKind
    parsed.sequence shouldBe 9L
    parsed.data("pos") shouldBe 123L
    parsed.javaData.get("pos") shouldBe java.lang.Long.valueOf(123L)
    parsed.json() should not include ("password")
    parsed.json() should not include ("table")
  }

  test("MongoDB snapshot and resume-token payloads round-trip") {
    val snapshot = CdcOffset.mongodb(
      Map(
        "mode" -> "snapshot",
        "startAt" -> """{"v": {"$timestamp": {"t": 10, "i": 1}}}""",
        "lastId" -> """{"_id": "a"}"""), 3L)
    CdcOffset.parse(snapshot.json()) shouldBe snapshot

    val stream = CdcOffset.mongodb(
      Map("mode" -> "stream", "resumeToken" -> """{"_data": "abc"}"""), 4L)
    CdcOffset.parse(stream.json()) shouldBe stream
  }

  test("reject malformed and non-scalar cursor data") {
    an[PuddingException] should be thrownBy CdcOffset.parse("")
    an[PuddingException] should be thrownBy CdcOffset.parse("""{"v":1,"kind":"debezium"}""")
    an[PuddingException] should be thrownBy {
      CdcOffset.debezium(Map("nested" -> Map("value" -> 1)), 1L)
    }
  }

  test("sequence makes offsets unique within a database cursor") {
    val first = CdcOffset.debezium(Map("pos" -> 100L), 1L)
    val second = CdcOffset.debezium(Map("pos" -> 100L), 2L)
    first should not be second
    first.json() should not be second.json()
  }
}
