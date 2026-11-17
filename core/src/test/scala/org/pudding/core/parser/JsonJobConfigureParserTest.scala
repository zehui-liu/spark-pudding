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

package org.pudding.core.parser

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the JSON job configuration parser.
 */
class JsonJobConfigureParserTest extends AnyFunSuite with Matchers {

  private val parser = new JsonJobConfigureParser()

  test("parse a JSON pipeline and normalize typed job config values") {
    val pipeline = parser.parseFromString(
      """
        |{
        |  "job": {
        |    "mode": "batch",
        |    "appName": "json job",
        |    "config": {
        |      "spark.executor.instances": 2,
        |      "spark.sql.adaptive.enabled": true
        |    }
        |  },
        |  "source": [
        |    {"type": "file-common-source", "id": "source-1"}
        |  ],
        |  "sink": [
        |    {"type": "console-sink", "id": "sink-1", "dependency": "source-1"}
        |  ]
        |}
        |""".stripMargin)

    pipeline.job.appName shouldBe "json job"
    pipeline.job.config shouldBe Some(Map(
      "spark.executor.instances" -> "2",
      "spark.sql.adaptive.enabled" -> "true"))
    pipeline.sources.head.id shouldBe "source-1"
    pipeline.sinks.head.dependency shouldBe "source-1"
  }

  test("reject malformed JSON with a PuddingException") {
    val error = intercept[PuddingException] {
      parser.parseFromString("{ not-json")
    }
    error.getMessage should include ("parser json file error")
  }

  test("reject a null transform section") {
    val error = intercept[PuddingException] {
      parser.parseFromString(
        """
          |{
          |  "job": {"mode": "batch", "appName": "json job"},
          |  "source": [{"type": "file-common-source", "id": "source-1"}],
          |  "transform": null,
          |  "sink": [{"type": "console-sink", "id": "sink-1", "dependency": "source-1"}]
          |}
          |""".stripMargin)
    }
    error.getMessage should include ("transform value must exist")
  }
}
