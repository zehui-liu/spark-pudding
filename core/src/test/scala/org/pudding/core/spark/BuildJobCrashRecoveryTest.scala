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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.incremental.{FileSystemStateBackend, IncrementalStateStore, ProcessTestSupport}

/**
 * End-to-end crash recovery: a separate JVM runs the real BuildJob with a sink that records its
 * transaction and then halts. The second JVM replays the pending transaction, the sink skips the
 * already-written transaction and the cursor is committed.
 */
class BuildJobCrashRecoveryTest extends AnyFunSuite with Matchers {

  test("replay a BuildJob after the sink commit and an abrupt process exit") {
    val root = Files.createTempDirectory("pudding-buildjob-crash")
    val statePath = Files.createDirectories(root.resolve("state"))
    val markerPath = Files.createDirectories(root.resolve("markers"))
    val jobFile = root.resolve("job.yaml")
    try {
      this.writeJob(jobFile, statePath, markerPath)
      val first = this.runJob(root, jobFile, root.resolve("first.log"), crash = true)
      withClue(ProcessTestSupport.readLog(root.resolve("first.log"))) {
        first shouldBe 17
      }

      val stateStore = this.store(statePath)
      val crashed = stateStore.load("source-1")
      crashed.committedCursor shouldBe None
      crashed.pending.map(_.toCursor) shouldBe Some("10")
      crashed.pending.map(_.transactionId) should not be empty
      stateStore.lockInfo("source-1") should not be empty
      this.markers(markerPath) shouldBe 1
      val markerText = {
        val stream = Files.list(markerPath)
        try new String(
          Files.readAllBytes(stream.iterator().next()),
          StandardCharsets.UTF_8)
        finally stream.close()
      }
      markerText should include ("source=source-1")
      markerText should include ("fencingToken=1")

      // The test uses a non-expiring lock, so crash recovery starts with the documented
      // operator unlock step.
      stateStore.forceUnlock("source-1") shouldBe true

      this.writeJob(jobFile, statePath, markerPath)
      val second = this.runJob(root, jobFile, root.resolve("second.log"), crash = false)
      withClue(ProcessTestSupport.readLog(root.resolve("second.log"))) {
        second shouldBe 0
      }

      val recovered = stateStore.load("source-1")
      recovered.committedCursor shouldBe Some("10")
      recovered.pending shouldBe None
      recovered.lastTransactionId should not be empty
      this.markers(markerPath) shouldBe 1
      stateStore.lockInfo("source-1") shouldBe None
    } finally {
      ProcessTestSupport.delete(root)
    }
  }

  private def writeJob(
      file: Path,
      statePath: Path,
      markerPath: Path): Unit = {
    val yaml =
      s"""
         |job:
         |  mode: sync
         |  strategy: incremental
         |  appName: "build job crash recovery"
         |  master: "local[2]"
         |  deployMode: "client"
         |  config:
         |    spark.master: "local[2]"
         |    spark.ui.enabled: "false"
         |    spark.sql.shuffle.partitions: "2"
         |source:
         |  - type: "mock_incremental_source"
         |    id: "source-1"
         |    config:
         |      incremental:
         |        semantics: "upsert"
         |        keyFields: ["id"]
         |        initial: "latest"
         |        statePath: "${statePath.toString}"
         |sink:
         |  - type: "mock_incremental_sink"
         |    id: "sink-1"
         |    dependency: "source-1"
         |    config:
         |      markerDir: "${markerPath.toString}"
         |""".stripMargin
    Files.write(file, yaml.getBytes(StandardCharsets.UTF_8))
  }

  private def runJob(root: Path, jobFile: Path, log: Path, crash: Boolean): Int = {
    val builder = new ProcessBuilder(
      ProcessTestSupport.javaCommand(
        "-cp",
        ProcessTestSupport.testClasspath,
        "org.pudding.core.PuddingSpoon",
        "--confType",
        "yaml",
        "--jobFile",
        jobFile.toString))
      .directory(root.toFile)
      .redirectErrorStream(true)
      .redirectOutput(log.toFile)
    if (crash) {
      builder.environment().put("PUDDING_TEST_CRASH_AFTER_SINK", "true")
    } else {
      builder.environment().remove("PUDDING_TEST_CRASH_AFTER_SINK")
    }
    val process = builder.start()
    val finished = process.waitFor(180L, TimeUnit.SECONDS)
    if (!finished) {
      process.destroyForcibly()
      fail("BuildJob process did not exit: " + ProcessTestSupport.readLog(log))
    }
    process.exitValue()
  }

  private def store(statePath: Path): IncrementalStateStore = {
    new IncrementalStateStore(
      new FileSystemStateBackend(statePath.toString, new Configuration()))
  }

  private def markers(path: Path): Int = {
    if (!Files.exists(path)) {
      0
    } else {
      val stream = Files.list(path)
      try stream.iterator().asScala.count(_.getFileName.toString.endsWith(".marker"))
      finally stream.close()
    }
  }
}
