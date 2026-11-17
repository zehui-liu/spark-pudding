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

import java.nio.file.Files

import jodd.io.FileUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.CommonTestUtils
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.SinkContext

/**
 * Writes files through `file-sink`, including streaming and atomic full refresh.
 */
class FileSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for file sink")
  var sparkSession: SparkSession = _

  var mockDataFrame :DataFrame = _

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession
    mockDataFrame = sparkSession.read.json(getClass.getClassLoader.getResource("test.json").getFile)
  }

  override def afterAll(): Unit = {
    utils.closeSparkSession()
  }

  test("test file sink") {
    val outputDir = Files.createTempDirectory("pudding-file-sink-")
    val config = Some(Map(
      "path" -> outputDir.toString,
      "fileType" -> "parquet",
      "saveMode" -> "Overwrite"
    ))
    try {
      new FileSink().writer(mockDataFrame, config)
      sparkSession.read.parquet(outputDir.toString).count() shouldBe mockDataFrame.count()
    } finally {
      FileUtil.deleteDir(outputDir.toFile)
    }
  }

  test("replace a file target with a staged swap") {
    val root = Files.createTempDirectory("pudding-file-sink-atomic")
    val output = root.resolve("out")
    try {
      sparkSession.range(1, 5).toDF("id").write
        .mode("overwrite").parquet(output.toString)
      new FileSink().writeFullRefresh(
        sparkSession.range(10, 15).toDF("id"),
        SinkContext(Some(Map(
          "path" -> output.toString,
          "fileType" -> "parquet",
          "saveMode" -> "overwrite",
          "allowNonAtomicReplace" -> true))))

      val result = sparkSession.read.parquet(output.toString)
      result.count() shouldBe 5L
      result.select("id").collect().map(_.getLong(0)).toSet shouldBe Set(10L, 11L, 12L, 13L, 14L)
      val entries = Files.list(root)
      try {
        entries.filter(_.getFileName.toString.contains(".pudding-")).count() shouldBe 0L
      } finally {
        entries.close()
      }
    } finally {
      FileUtil.deleteDir(root.toFile)
    }
  }

  test("recover a stale file backup before the next full refresh") {
    val root = Files.createTempDirectory("pudding-file-sink-recovery")
    val output = root.resolve("out")
    val backup = root.resolve(".out.pudding-backup")
    try {
      sparkSession.range(1, 3).toDF("id").write
        .mode("overwrite").parquet(output.toString)
      Files.move(output, backup)

      new FileSink().writeFullRefresh(
        sparkSession.range(10, 13).toDF("id"),
        SinkContext(Some(Map(
          "path" -> output.toString,
          "fileType" -> "parquet",
          "saveMode" -> "overwrite",
          "allowNonAtomicReplace" -> true))))

      sparkSession.read.parquet(output.toString).count() shouldBe 3L
      Files.exists(backup) shouldBe false
    } finally {
      FileUtil.deleteDir(root.toFile)
    }
  }

  test("explicit batch execution overrides connector streaming flags") {
    val outputDir = Files.createTempDirectory("pudding-file-sink-explicit-batch")
    val config = Some(Map(
      "path" -> outputDir.toString,
      "fileType" -> "parquet",
      "saveMode" -> "Overwrite",
      "isStreaming" -> true))
    try {
      new FileSink().writer(
        mockDataFrame,
        SinkContext(config, executionMode = Some(ExecutionMode.Batch)))
      sparkSession.read.parquet(outputDir.toString).count() shouldBe mockDataFrame.count()
    } finally {
      FileUtil.deleteDir(outputDir.toFile)
    }
  }

  test("streaming flag supports the canonical key and typed timeout") {
    val tempDir = Files.createTempDirectory("pudding-file-sink-stream-")
    val outputDir = tempDir.resolve("out")
    val streamingDataFrame = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val streamingConfig = Some(Map(
      "path" -> outputDir.toString,
      "fileType" -> "json",
      "isStreaming" -> true,
      "timeoutMs" -> 1500
    ))

    try {
      // a batch write would fail because saveMode is absent, so success proves
      // that the streaming branch was selected and that timeoutMs was parsed
      noException should be thrownBy new FileSink().writer(streamingDataFrame, streamingConfig)
    } finally {
      FileUtil.deleteDir(outputDir.toFile)
      FileUtil.deleteDir(tempDir.toFile)
    }
  }

  test("keep or delete the checkpoint after a finite streaming query") {
    val tempDir = Files.createTempDirectory("pudding-file-sink-retention")

    def write(name: String, retention: String): java.nio.file.Path = {
      val checkpoint = tempDir.resolve(name + "-checkpoint")
      val streamingDataFrame = sparkSession.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()
      new FileSink().writer(
        streamingDataFrame,
        SinkContext(
          Some(Map(
            "path" -> tempDir.resolve(name).toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "checkpointLocation" -> checkpoint.toString,
            "checkpointRetention" -> retention)),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
      checkpoint
    }

    try {
      Files.exists(write("keep", "keep")) shouldBe true
      Files.exists(write("delete", "delete-on-success")) shouldBe false
    } finally {
      FileUtil.deleteDir(tempDir.toFile)
    }
  }

  test("honor a checkpoint location supplied through writer options") {
    val tempDir = Files.createTempDirectory("pudding-file-sink-options-checkpoint")
    val outputDir = tempDir.resolve("out")
    val optionCheckpoint = tempDir.resolve("option-checkpoint")
    val derivedCheckpoint = java.nio.file.Paths.get(outputDir.toString + "_checkpoint")
    val streamingDataFrame = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    try {
      new FileSink().writer(
        streamingDataFrame,
        SinkContext(
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "options" -> Map("checkpointLocation" -> optionCheckpoint.toString),
            "checkpointRetention" -> "keep")),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))

      Files.exists(optionCheckpoint) shouldBe true
      Files.exists(derivedCheckpoint) shouldBe false
    } finally {
      FileUtil.deleteDir(tempDir.toFile)
    }
  }

  test("treat checkpoint paths with and without a trailing slash as equal") {
    val tempDir = Files.createTempDirectory("pudding-file-sink-checkpoint-slash")
    val outputDir = tempDir.resolve("out")
    val checkpoint = tempDir.resolve("checkpoint")
    val streamingDataFrame = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    try {
      new FileSink().writer(
        streamingDataFrame,
        SinkContext(
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "checkpointLocation" -> (checkpoint.toString + "/"),
            "options" -> Map("checkpointLocation" -> checkpoint.toString),
            "checkpointRetention" -> "keep")),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))

      Files.exists(checkpoint) shouldBe true
    } finally {
      FileUtil.deleteDir(tempDir.toFile)
    }
  }

  test("reject conflicting checkpoint locations") {
    val tempDir = Files.createTempDirectory("pudding-file-sink-checkpoint-conflict")
    val streamingDataFrame = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()

    try {
      val error = intercept[org.pudding.core.PuddingException] {
        new FileSink().writer(
          streamingDataFrame,
          Some(Map(
            "path" -> tempDir.resolve("out").toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "checkpointLocation" -> tempDir.resolve("configured").toString,
            "options" -> Map(
              "checkpointLocation" -> tempDir.resolve("option").toString))))
      }
      error.getMessage should include ("checkpointLocation")
      error.getMessage should include ("configured twice")
    } finally {
      FileUtil.deleteDir(tempDir.toFile)
    }
  }

  test("reject a non-positive streaming timeout") {
    val streamingDataFrame = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()
    val outputDir = Files.createTempDirectory("pudding-file-sink-timeout")
    try {
      val error = intercept[org.pudding.core.PuddingException] {
        new FileSink().writer(
          streamingDataFrame,
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "timeoutMs" -> 0)))
      }
      error.getMessage should include ("greater than zero")
    } finally {
      FileUtil.deleteDir(outputDir.toFile)
    }
  }

  test("streaming flag accepts the legacy keys") {
    val sink = new FileSink()

    sink.streamingEnabled(Map("isStreaming" -> true)) shouldBe true
    sink.streamingEnabled(Map("streamWrite" -> "true")) shouldBe true
    sink.streamingEnabled(Map("streamWriter" -> "true")) shouldBe true
    sink.streamingEnabled(Map("streamingWriter" -> true)) shouldBe true
    sink.streamingEnabled(Map(
      "isStreaming" -> false,
      "streamWrite" -> true)) shouldBe true
    sink.streamingEnabled(Map.empty) shouldBe false
  }

  test("reject invalid writer config before starting a streaming query") {
    val streamingDataFrame = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()
    val outputDir = Files.createTempDirectory("pudding-file-sink-invalid")

    try {
      val invalidMode = intercept[org.pudding.core.PuddingException] {
        new FileSink().writer(
          streamingDataFrame,
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "outputMode" -> "replace")))
      }
      invalidMode.getMessage should include ("unsupported outputMode")

      val updateMode = intercept[org.pudding.core.PuddingException] {
        new FileSink().writer(
          streamingDataFrame,
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "outputMode" -> "update")))
      }
      updateMode.getMessage should include ("supports only append")

      val invalidCheckpoint = intercept[org.pudding.core.PuddingException] {
        new FileSink().writer(
          streamingDataFrame,
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "checkpointLocation" -> 42)))
      }
      invalidCheckpoint.getMessage should include ("checkpointLocation must be a string")

      val retainCheckpoint = intercept[org.pudding.core.PuddingException] {
        new FileSink().writer(
          streamingDataFrame,
          Some(Map(
            "path" -> outputDir.toString,
            "fileType" -> "json",
            "isStreaming" -> true,
            "checkpointRetention" -> "delete-on-success")))
      }
      retainCheckpoint.getMessage should include ("requires a finite trigger")
    } finally {
      FileUtil.deleteDir(outputDir.toFile)
    }
  }

  test("reject non-string required batch values") {
    intercept[org.pudding.core.PuddingException] {
      new FileSink().writer(
        mockDataFrame,
        Some(Map("path" -> 1, "fileType" -> "json", "saveMode" -> "overwrite")))
    }
    intercept[org.pudding.core.PuddingException] {
      new FileSink().writer(
        mockDataFrame,
        Some(Map("path" -> "/tmp/pudding-invalid", "fileType" -> "json", "saveMode" -> true)))
    }
  }

}
