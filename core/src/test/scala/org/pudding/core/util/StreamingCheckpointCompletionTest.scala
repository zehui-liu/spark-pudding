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

package org.pudding.core.util

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Verifies that `delete-on-success` distinguishes a drained finite query from an external stop.
 */
class StreamingCheckpointCompletionTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private val cfg = Map[String, Any]("checkpointRetention" -> "delete-on-success")

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("streaming checkpoint completion test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("delete a checkpoint after an available-now query drains its input") {
    val (input, checkpoint) = this.fixture()
    val query = this.startTextQuery(input, checkpoint, "available-now", Trigger.AvailableNow()) {
      batch =>
        batch.count()
        ()
    }
    try {
      StreamingCheckpoint.awaitTermination(
        query, cfg, Some("available-now"), Some(checkpoint.toString))
      Files.exists(checkpoint) shouldBe false
    } finally {
      stopQuietly(query)
      this.delete(input)
    }
  }

  test("delete a checkpoint after a once query commits its batch") {
    val (input, checkpoint) = this.fixture()
    val query = this.startTextQuery(input, checkpoint, "once", Trigger.Once()) { batch =>
      batch.count()
      ()
    }
    try {
      StreamingCheckpoint.awaitTermination(
        query, cfg, Some("once"), Some(checkpoint.toString))
      Files.exists(checkpoint) shouldBe false
    } finally {
      stopQuietly(query)
      this.delete(input)
    }
  }

  test("keep the checkpoint when a query is stopped before its first batch commits") {
    val (input, checkpoint) = this.fixture()
    val batchStarted = new CountDownLatch(1)
    val releaseBatch = new CountDownLatch(1)
    val query = this.startTextQuery(input, checkpoint, "available-now", Trigger.AvailableNow()) {
      batch =>
        batchStarted.countDown()
        try {
          releaseBatch.await(30, TimeUnit.SECONDS)
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt()
        }
        ()
    }
    try {
      batchStarted.await(30, TimeUnit.SECONDS) shouldBe true
      query.stop()
      releaseBatch.countDown()

      StreamingCheckpoint.awaitTermination(
        query, cfg, Some("available-now"), Some(checkpoint.toString))
      Files.exists(checkpoint) shouldBe true
    } finally {
      releaseBatch.countDown()
      stopQuietly(query)
      this.delete(input)
      this.delete(checkpoint)
    }
  }

  private def fixture(): (java.nio.file.Path, java.nio.file.Path) = {
    val input = Files.createTempDirectory("pudding-checkpoint-completion-input")
    Files.write(
      input.resolve("input.txt"),
      "row".getBytes(StandardCharsets.UTF_8))
    val parent = Files.createTempDirectory("pudding-checkpoint-completion")
    (input, parent.resolve("checkpoint"))
  }

  private def startTextQuery(
      input: java.nio.file.Path,
      checkpoint: java.nio.file.Path,
      triggerName: String,
      trigger: Trigger)(
      process: org.apache.spark.sql.DataFrame => Unit): StreamingQuery = {
    StreamingCheckpoint.validate(
      cfg, Some(triggerName), Some(checkpoint.toString))
    sparkSession.readStream
      .format("text")
      .load(input.toString)
      .writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, _: Long) =>
        process(batch)
      }
      .option("checkpointLocation", checkpoint.toString)
      .outputMode("append")
      .trigger(trigger)
      .start()
  }

  private def stopQuietly(query: StreamingQuery): Unit = {
    if (query != null && query.isActive) {
      query.stop()
    }
  }

  private def delete(path: java.nio.file.Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toVector.reverse.foreach(value => Files.deleteIfExists(value))
      } finally {
        stream.close()
      }
    }
  }
}
