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

package org.pudding.sink

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.{CommonTestUtils, PuddingException}

/**
 * test for ConsoleSink
 */
class ShowSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for console sink")

  private var sparkSession: SparkSession = _

  private var dataFrame: DataFrame = _

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession
    dataFrame = sparkSession.range(2).toDF("id")
  }

  override def afterAll(): Unit = {
    utils.closeSparkSession()
  }

  test("batch writer works with default options") {
    noException should be thrownBy new ConsoleSink().writer(
      dataFrame, Some(Map.empty[String, Any]))
  }

  test("batch writer accepts typed and string options") {
    val config = Some(Map(
      "numRows" -> 1,
      "truncate" -> "1",
      "vertical" -> true,
      "printSchema" -> "true"
    ))

    noException should be thrownBy new ConsoleSink().writer(dataFrame, config)
  }

  test("streaming flag supports the canonical and legacy keys") {
    val sink = new ConsoleSink()

    sink.streamingEnabled(Map("isStreaming" -> true)) shouldBe true
    sink.streamingEnabled(Map("isStreaming" -> "true")) shouldBe true
    sink.streamingEnabled(Map("isStream" -> true)) shouldBe true
    sink.streamingEnabled(Map.empty) shouldBe false
    // the canonical key takes precedence when both are present
    sink.streamingEnabled(Map("isStreaming" -> false, "isStream" -> true)) shouldBe false
  }

  test("invalid option types are rejected") {
    val sink = new ConsoleSink()

    intercept[PuddingException] {
      sink.streamingEnabled(Map("isStreaming" -> "yes"))
    }
    intercept[PuddingException] {
      sink.writer(dataFrame, Some(Map("numRows" -> "many")))
    }
  }
}
