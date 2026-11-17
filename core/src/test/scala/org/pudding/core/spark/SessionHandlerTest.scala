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

package org.pudding.core.spark

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.pudding.core.configure.{JobConf, JobMode}

/**
 * Tests Spark session configuration, catalog installation and conflict handling.
 */
class SessionHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val mockJobConf: JobConf = JobConf(
    "",
    Option.empty[String],
    "local test",
    "local[2]",
    "",
    Option("describe"),
    Option(Map("spark.master" -> "local[2]")),
    JobMode.Batch
  )

  test("test local sparkSession") {
    val sparkSession = SessionHandler.apply().sparkSessionBuilder(mockJobConf)
    assert(sparkSession.version == org.apache.spark.SPARK_VERSION)
    sparkSession.close()
  }

  test("use job master when no runtime master is configured") {
    val jobConf = mockJobConf.copy(
      master = "local[2]",
      config = Option(Map("spark.ui.enabled" -> "false"))
    )

    val sparkSession = SessionHandler.apply().sparkSessionBuilder(jobConf)
    try {
      sparkSession.sparkContext.master shouldBe "local[2]"
    } finally {
      sparkSession.close()
    }
  }
}
