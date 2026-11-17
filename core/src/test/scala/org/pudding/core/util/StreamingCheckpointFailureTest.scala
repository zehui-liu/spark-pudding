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

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Verifies that a failed finite query never triggers delete-on-success.
 */
class StreamingCheckpointFailureTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("streaming checkpoint failure test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("keep a checkpoint when a delete-on-success query fails") {
    val checkpoint = Files.createTempDirectory("pudding-checkpoint-failure")
    val query = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()
      .writeStream
      .foreachBatch { (_: org.apache.spark.sql.DataFrame, _: Long) =>
        throw new RuntimeException("simulated sink failure")
      }
      .option("checkpointLocation", checkpoint.toString)
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .start()

    try {
      intercept[Exception] {
        StreamingCheckpoint.awaitTermination(
          query,
          Map("checkpointRetention" -> "delete-on-success"),
          Some("available-now"),
          Some(checkpoint.toString))
      }
      Files.exists(checkpoint) shouldBe true
    } finally {
      query.stop()
      org.apache.hadoop.fs.FileSystem.get(
        new org.apache.hadoop.conf.Configuration())
        .delete(new org.apache.hadoop.fs.Path(checkpoint.toUri), true)
    }
  }
}
