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

package org.pudding.core.configure

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamingOutputModeTest extends AnyFunSuite with Matchers {

  test("default to append and normalize configured values") {
    StreamingOutputMode.configured(None) shouldBe None
    StreamingOutputMode.configured(Some(Map.empty)) shouldBe None
    StreamingOutputMode.resolve(None, "test-sink", Set("append")) shouldBe "append"
    StreamingOutputMode.configured(
      Some(Map("outputMode" -> " UPDATE "))) shouldBe Some("update")
  }

  test("reject invalid values and types") {
    intercept[PuddingException] {
      StreamingOutputMode.configured(Some(Map("outputMode" -> "replace")))
    }.getMessage should include ("unsupported streaming outputMode")

    intercept[PuddingException] {
      StreamingOutputMode.configured(Some(Map("outputMode" -> 1)))
    }.getMessage should include ("outputMode must be a string")
  }

  test("reject a mode the sink does not declare") {
    val error = intercept[PuddingException] {
      StreamingOutputMode.resolve(
        Some(Map("outputMode" -> "update")), "test-sink", Set("append"))
    }
    error.getMessage should include ("does not support streaming outputMode=update")
    error.getMessage should include ("supported: append")
  }
}
