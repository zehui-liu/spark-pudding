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

package org.pudding.core.incremental

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the state inspection and repair CLI.
 */
class IncrementalStateToolTest extends AnyFunSuite with Matchers {

  private def run(args: String*): (Int, String, String) = {
    val out = new ByteArrayOutputStream()
    val err = new ByteArrayOutputStream()
    val code = IncrementalStateTool.run(
      args.toArray,
      new PrintStream(out, true, StandardCharsets.UTF_8.name()),
      new PrintStream(err, true, StandardCharsets.UTF_8.name()))
    (code, out.toString(StandardCharsets.UTF_8.name()), err.toString(StandardCharsets.UTF_8.name()))
  }

  private val plan = IncrementalPlanDescriptor.create(
    sourceType = "test-source",
    sourceId = "source",
    sinkType = "test-sink",
    sinkId = "sink",
    semantics = org.pudding.core.definition.IncrementalSemantics.Upsert,
    keyFields = Seq("id"),
    opColumn = "__op",
    sourceConfig = None,
    sinkConfig = None,
    stateBackend = "filesystem")

  private def committed(path: java.nio.file.Path, cursor: String): Unit = {
    val store = new IncrementalStateStore(path.toString, new Configuration())
    store.acquire("source")
    try {
      store.begin("source", None, cursor, s"txn-$cursor", this.plan)
      store.commit("source", s"txn-$cursor")
    } finally {
      store.release("source")
    }
  }

  private def pending(path: java.nio.file.Path, cursor: String): Unit = {
    val store = new IncrementalStateStore(path.toString, new Configuration())
    store.acquire("source")
    try {
      store.begin("source", None, cursor, s"txn-$cursor", this.plan)
    } finally {
      store.release("source")
    }
  }

  test("show state, pending transaction and lock") {
    val path = Files.createTempDirectory("pudding-state-tool-show")
    this.pending(path, "10")

    val (code, output, error) = this.run("show", path.toString, "source")
    code shouldBe 0
    error shouldBe empty
    output should include ("source: source")
    output should include ("cursor: <none>")
    output should include ("pending: txn-10")
    output should include ("lock: none")
  }

  test("list all sources and show an empty state directory") {
    val path = Files.createTempDirectory("pudding-state-tool-list")
    this.committed(path, "10")

    val (code, output, _) = this.run("list", path.toString)
    code shouldBe 0
    output should include ("source: source")
    output should include ("cursor: 10")

    val empty = Files.createTempDirectory("pudding-state-tool-empty")
    val (emptyCode, emptyOutput, _) = this.run("show", empty.toString)
    emptyCode shouldBe 0
    emptyOutput should include ("no incremental state found")
  }

  test("reset a cursor and clear the state") {
    val path = Files.createTempDirectory("pudding-state-tool-reset")
    this.committed(path, "10")

    val (resetCode, resetOutput, _) =
      this.run("reset", path.toString, "source", "--cursor", "5")
    resetCode shouldBe 0
    resetOutput should include ("reset incremental cursor")

    val store = new IncrementalStateStore(path.toString, new Configuration())
    store.load("source").committedCursor shouldBe Some("5")

    val (clearCode, clearOutput, _) =
      this.run("reset", path.toString, "source", "--clear")
    clearCode shouldBe 0
    clearOutput should include ("cleared incremental state")
    store.load("source").isEmpty shouldBe true
  }

  test("commit or abort a pending transaction") {
    val commitPath = Files.createTempDirectory("pudding-state-tool-commit")
    this.pending(commitPath, "10")
    val (commitCode, commitOutput, _) =
      this.run("resolve", commitPath.toString, "source", "--commit")
    commitCode shouldBe 0
    commitOutput should include ("committed transaction")
    val commitStore = new IncrementalStateStore(commitPath.toString, new Configuration())
    commitStore.load("source").committedCursor shouldBe Some("10")
    commitStore.load("source").pending shouldBe None

    val abortPath = Files.createTempDirectory("pudding-state-tool-abort")
    this.pending(abortPath, "10")
    val (abortCode, abortOutput, _) =
      this.run("resolve", abortPath.toString, "source", "--abort")
    abortCode shouldBe 0
    abortOutput should include ("aborted transaction")
    val abortStore = new IncrementalStateStore(abortPath.toString, new Configuration())
    abortStore.load("source").pending shouldBe None
  }

  test("require an explicit pending transaction to resolve") {
    val path = Files.createTempDirectory("pudding-state-tool-no-pending")
    this.committed(path, "10")

    val (code, _, error) = this.run("resolve", path.toString, "source", "--commit")
    code shouldBe 1
    error should include ("no pending")
  }

  test("emit machine-readable status with plan and recovery metadata") {
    val path = Files.createTempDirectory("pudding-state-tool-status")
    this.pending(path, "10")

    val (code, output, error) = this.run("status", path.toString, "source", "--json")
    code shouldBe 0
    error shouldBe empty
    output should include ("\"sourceId\":\"source\"")
    output should include ("\"pending\":{")
    output should include ("\"planFingerprint\":\"")
    output should include ("\"attempts\":1")
    output should include ("\"lock\":null")

    val (allCode, allOutput, _) = this.run("status", path.toString, "--json")
    allCode shouldBe 0
    allOutput.trim should startWith ("[")
  }

  test("force a reset past a stale lock and remove the lock explicitly") {
    val path = Files.createTempDirectory("pudding-state-tool-force")
    val stale = new IncrementalStateStore(path.toString, new Configuration())
    stale.acquire("source")

    val (resetCode, _, _) =
      this.run("reset", path.toString, "source", "--cursor", "3", "--force")
    resetCode shouldBe 0
    stale.release("source") shouldBe false

    stale.acquire("source")
    val (unlockCode, unlockOutput, _) =
      this.run("unlock", path.toString, "source")
    unlockCode shouldBe 0
    unlockOutput should include ("removed incremental state lock")
    stale.release("source") shouldBe false
  }

  test("reject invalid usage and unknown commands") {
    val (unknownCode, _, unknownError) = this.run("unknown")
    unknownCode shouldBe 2
    unknownError should include ("unknown command")

    val (missingCode, _, missingError) = this.run("reset", "/tmp/state")
    missingCode shouldBe 2
    missingError should include ("requires a state target and a source id")

    val (conflictCode, _, conflictError) =
      this.run("resolve", "/tmp/state", "source", "--commit", "--abort")
    conflictCode shouldBe 2
    conflictError should include ("exactly one")
  }
}
