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

/**
 * Tests the Java-facing log redaction facade.
 */
class LogRedactionTest extends AnyFunSuite with Matchers {

  test("redacts each line of a diagnostic text") {
    val text =
      "launcher started\n" +
        "password=first-secret\n" +
        "jdbc:mysql://alice:url-secret@localhost/db\n" +
        "loaded 12 rows"

    val redacted = LogRedaction.redactText(text)

    redacted should not include ("first-secret")
    redacted should not include ("url-secret")
    redacted should include ("loaded 12 rows")
    redacted.split('\n').length shouldBe 4
  }

  test("preserves null and ordinary lines") {
    LogRedaction.redactLine(null) shouldBe null
    LogRedaction.redactText(null) shouldBe null
    LogRedaction.redactLine("INFO healthy") shouldBe "INFO healthy"
  }
}
