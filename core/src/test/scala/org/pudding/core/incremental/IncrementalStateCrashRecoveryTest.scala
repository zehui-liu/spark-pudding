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

package org.pudding.core.incremental

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException

/**
 * Recovery tests that kill a separate writer JVM after it records a pending transaction. The
 * surviving process must see the pending journal, wait for (or explicitly remove) the stale lock,
 * and finish the transaction.
 */
class IncrementalStateCrashRecoveryTest extends AnyFunSuite with Matchers {

  test("recover a pending transaction after the writer process is killed") {
    val path = Files.createTempDirectory("pudding-crash-lock")
    try {
      val writer = this.startWriter(path)
      this.awaitExit(writer, path)

      val store = this.store(path)
      val crashed = store.load("source")
      crashed.pending.map(_.transactionId) shouldBe Some("txn-crash")
      crashed.pending.map(_.toCursor) shouldBe Some("42")
      crashed.committedCursor shouldBe None
      store.lockInfo("source").map(_.fencingToken) shouldBe Some(1L)
      intercept[PuddingException] {
        store.acquire("source")
      }.getMessage should include ("locked")

      // The filesystem backend has no lease; an operator removes the lock after confirming the
      // previous writer is gone, and the pending journal then completes.
      store.forceUnlock("source") shouldBe true
      val recovered = store.acquire("source")
      recovered.fencingToken shouldBe 2L
      try {
        store.load("source").pending.map(_.transactionId) shouldBe Some("txn-crash")
        store.commit("source", "txn-crash")
        store.load("source").committedCursor shouldBe Some("42")
        store.load("source").pending shouldBe None
      } finally {
        store.release("source") shouldBe true
      }
    } finally {
      ProcessTestSupport.delete(path)
    }
  }

  private def store(path: Path): IncrementalStateStore = {
    new IncrementalStateStore(
      new FileSystemStateBackend(path.toString, new Configuration()))
  }

  private def startWriter(path: Path): Process = {
    val log = path.resolve("writer.log").toFile
    new ProcessBuilder(
      ProcessTestSupport.javaCommand(
        "-cp",
        ProcessTestSupport.testClasspath,
        "org.pudding.core.incremental.IncrementalStateCrashWriter",
        path.toString,
        "0"))
      .redirectErrorStream(true)
      .redirectOutput(log)
      .start()
  }

  private def awaitExit(writer: Process, path: Path): Unit = {
    val exited = writer.waitFor(30L, TimeUnit.SECONDS)
    if (!exited) {
      writer.destroyForcibly()
      fail("crash writer did not exit")
    }
    val log = new String(
      Files.readAllBytes(path.resolve("writer.log")),
      StandardCharsets.UTF_8)
    withClue(log) {
      writer.exitValue() shouldBe 17
    }
  }

}
