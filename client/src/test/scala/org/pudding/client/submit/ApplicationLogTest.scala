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

package org.pudding.client.submit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Tests bounded log buffering and snapshots.
 */
class ApplicationLogTest extends AnyFunSuite with Matchers {

  test("keeps only recent lines and reports truncation") {
    val buffer = new BoundedLogBuffer(3)
    (1 to 5).foreach(line => buffer.append("line-" + line))

    val all = buffer.snapshot(10)
    all.getLines.asScala shouldBe List("line-3", "line-4", "line-5")
    all.isTruncated shouldBe true

    val recent = buffer.snapshot(2)
    recent.getLines.asScala shouldBe List("line-4", "line-5")
    recent.getText shouldBe "line-4\nline-5"
  }

  test("does not report truncation while all lines are buffered") {
    val buffer = new BoundedLogBuffer(3)
    buffer.append("one")
    buffer.append("two")

    val log = buffer.snapshot(5)
    log.getLines.asScala shouldBe List("one", "two")
    log.isTruncated shouldBe false
  }

  test("tail keeps trailing non-empty lines") {
    val log = ApplicationLog.tail("one\r\ntwo\nthree\n", 2)

    log.getLines.asScala shouldBe List("two", "three")
    log.isTruncated shouldBe true
    log.getText shouldBe "two\nthree"
  }

  test("empty log text produces an empty snapshot") {
    ApplicationLog.tail(null, 10).isEmpty shouldBe true
    ApplicationLog.tail("", 10).isEmpty shouldBe true
    ApplicationLog.tail("\n", 10).getLines.asScala shouldBe List("")
  }

  test("requires a positive line limit") {
    val error = intercept[SubmissionException] {
      ApplicationLog.tail("line", 0)
    }
    error.getMessage should include ("must be positive")
    an[SubmissionException] should be thrownBy new BoundedLogBuffer(2).snapshot(0)
  }

  test("toString does not expose log content") {
    val log = ApplicationLog.of(java.util.Arrays.asList("secret-value"), false)
    log.toString should not include ("secret-value")
  }
}
