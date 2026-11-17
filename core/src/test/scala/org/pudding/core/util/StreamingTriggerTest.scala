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

package org.pudding.core.util

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the job-level structured-streaming trigger parser.
 */
class StreamingTriggerTest extends AnyFunSuite with Matchers {

  test("parse available-now, once and processing-time triggers") {
    StreamingTrigger.parse("available-now").getClass.getName should include ("AvailableNowTrigger")
    StreamingTrigger.parse("availableNow").getClass.getName should include ("AvailableNowTrigger")
    StreamingTrigger.parse("once").getClass.getName should include ("OneTimeTrigger")
    StreamingTrigger.parse("30 seconds").getClass.getName should include ("ProcessingTimeTrigger")
    StreamingTrigger.parse("processing-time: 10 seconds").getClass.getName should
      include ("ProcessingTimeTrigger")
  }

  test("recognize finite triggers") {
    StreamingTrigger.isFinite(Some("available-now")) shouldBe true
    StreamingTrigger.isFinite(Some(" AvailableNow ")) shouldBe true
    StreamingTrigger.isFinite(Some("once")) shouldBe true
    StreamingTrigger.isFinite(Some("30 seconds")) shouldBe false
    StreamingTrigger.isFinite(Some("processing-time: 10 seconds")) shouldBe false
    StreamingTrigger.isFinite(Some("continuous: 1 second")) shouldBe false
    StreamingTrigger.isFinite(None) shouldBe false
  }

  test("reject empty, invalid and malformed triggers") {
    Seq("", "   ", "processing-time:", "continuous:").foreach { value =>
      intercept[PuddingException] {
        StreamingTrigger.validate(value)
      }
    }
    intercept[PuddingException] {
      StreamingTrigger.parse("not-a-duration")
    }
  }
}
