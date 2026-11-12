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
import java.time.Instant

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.pudding.core.PuddingException
import org.pudding.core.definition.{IncrementalSemantics, IncrementalSpec}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the incremental state journal, lock and legacy cursor migration.
 */
class IncrementalStateStoreTest extends AnyFunSuite with Matchers {

  private def statePath(): Path = Files.createTempDirectory("pudding-state-store")

  private def store(path: Path): IncrementalStateStore =
    new IncrementalStateStore(path.toString, new Configuration())

  private def fileBase(sourceId: String): String = {
    sourceId.replaceAll("[^A-Za-z0-9._-]", "_") + "-" +
      IncrementalStateCodec.sha256(sourceId).take(12)
  }

  private def withLockedStore(path: Path)(body: IncrementalStateStore => Unit): Unit = {
    val stateStore = this.store(path)
    stateStore.acquire("source")
    try {
      body(stateStore)
    } finally {
      stateStore.release("source")
    }
  }

  private val plan = IncrementalPlanDescriptor.create(
    sourceType = "test-source",
    sourceId = "source",
    sinkType = "test-sink",
    sinkId = "sink",
    semantics = IncrementalSemantics.Upsert,
    keyFields = Seq("id"),
    opColumn = "__op",
    sourceConfig = None,
    sinkConfig = None,
    stateBackend = "filesystem")

  private def begin(
      store: IncrementalStateStore,
      sourceId: String,
      fromCursor: Option[String],
      toCursor: String,
      transactionId: String): Unit = {
    val actual = if (sourceId == "source") this.plan else this.plan.copy(sourceId = sourceId)
    store.begin(sourceId, fromCursor, toCursor, transactionId, actual)
  }

  test("persist, replace and commit a cursor") {
    val path = this.statePath()
    val stateStore = this.store(path)

    stateStore.load("source").isEmpty shouldBe true
    stateStore.acquire("source")
    try {
      this.begin(stateStore, "source", None, "10", "txn-1")
      val pending = stateStore.load("source")
      pending.committedCursor shouldBe None
      pending.pending.map(_.toCursor) shouldBe Some("10")
      pending.pending.flatMap(_.plan).map(_.fingerprint) shouldBe Some(this.plan.fingerprint)

      stateStore.commit("source", "txn-1")
      val committed = stateStore.load("source")
      committed.committedCursor shouldBe Some("10")
      committed.pending shouldBe None

      this.begin(stateStore, "source", Some("10"), "20", "txn-2")
      stateStore.commit("source", "txn-2")
      stateStore.load("source").committedCursor shouldBe Some("20")
    } finally {
      stateStore.release("source")
    }

    Files.exists(path.resolve(this.fileBase("source") + ".state")) shouldBe true
    Files.exists(path.resolve("source.cursor")) shouldBe false
    val entries = {
      val stream = Files.list(path)
      try {
        stream.iterator().asScala.map(_.getFileName.toString)
          .filterNot(_.endsWith(".crc")).toSet
      } finally {
        stream.close()
      }
    }
    entries shouldBe Set(this.fileBase("source") + ".state")
  }

  test("read a legacy cursor and remove it after the next commit") {
    val path = this.statePath()
    Files.write(path.resolve("source.cursor"), "7".getBytes(StandardCharsets.UTF_8))
    val stateStore = this.store(path)

    stateStore.load("source").committedCursor shouldBe Some("7")

    stateStore.acquire("source")
    try {
      this.begin(stateStore, "source", Some("7"), "9", "txn-legacy")
      stateStore.commit("source", "txn-legacy")
    } finally {
      stateStore.release("source")
    }

    stateStore.load("source").committedCursor shouldBe Some("9")
    Files.exists(path.resolve("source.cursor")) shouldBe false
  }

  test("allow only one writer for a source") {
    val path = this.statePath()
    val first = this.store(path)
    val second = this.store(path)

    first.acquire("source")
    val error = intercept[PuddingException] {
      second.acquire("source")
    }
    error.getMessage should include ("locked")
    error.getMessage should include ("source")

    first.release("source") shouldBe true
    second.acquire("source")
    second.release("source") shouldBe true
  }

  test("do not release a lock owned by another process") {
    val path = this.statePath()
    val first = this.store(path)
    val second = this.store(path)

    first.acquire("source")
    second.forceUnlock("source") shouldBe true
    first.release("source") shouldBe false

    val third = this.store(path)
    third.acquire("source")
    third.release("source") shouldBe true
  }

  test("refuse to mutate state after the lock is lost") {
    val path = this.statePath()
    val first = this.store(path)
    val second = this.store(path)

    first.acquire("source")
    second.forceUnlock("source")
    val error = intercept[PuddingException] {
      this.begin(first, "source", None, "10", "txn-lost")
    }
    error.getMessage should include ("lock")
    error.getMessage should include ("lost")
    first.release("source") shouldBe false

    second.acquire("source")
    this.begin(second, "source", None, "10", "txn-new")
    second.commit("source", "txn-new")
    second.release("source") shouldBe true
  }

  test("keep a pending transaction when commit fails") {
    val path = this.statePath()
    val backend = new FileSystemStateBackend(path.toString, new Configuration()) {
      override def commit(sourceId: String, transactionId: String): IncrementalState =
        throw new PuddingException("simulated commit failure")
    }
    val stateStore = new IncrementalStateStore(backend)
    stateStore.acquire("source")
    try {
      this.begin(stateStore, "source", None, "10", "txn-commit-failure")
      val error = intercept[PuddingException] {
        stateStore.commit("source", "txn-commit-failure")
      }
      error.getMessage should include ("simulated commit failure")
      stateStore.load("source").pending.map(_.transactionId) shouldBe
        Some("txn-commit-failure")
      stateStore.load("source").committedCursor shouldBe None
    } finally {
      stateStore.release("source")
    }
  }

  test("support abort and a second transaction") {
    val path = this.statePath()
    this.withLockedStore(path) { stateStore =>
      this.begin(stateStore, "source", None, "10", "txn-1")
      stateStore.abort("source", "txn-1")
      stateStore.load("source").pending shouldBe None
      stateStore.load("source").committedCursor shouldBe None

      this.begin(stateStore, "source", None, "12", "txn-2")
      stateStore.commit("source", "txn-2")
      stateStore.load("source").committedCursor shouldBe Some("12")
    }
  }

  test("reject an unknown transaction and a second pending transaction") {
    val path = this.statePath()
    this.withLockedStore(path) { stateStore =>
      val missing = intercept[PuddingException] {
        stateStore.commit("source", "missing")
      }
      missing.getMessage should include ("no pending")

      this.begin(stateStore, "source", None, "10", "txn-1")
      val duplicate = intercept[PuddingException] {
        this.begin(stateStore, "source", None, "11", "txn-2")
      }
      duplicate.getMessage should include ("already has an uncommitted")

      val mismatch = intercept[PuddingException] {
        stateStore.commit("source", "txn-2")
      }
      mismatch.getMessage should include ("does not match")
    }
  }

  test("reject an inconsistent read start") {
    val path = this.statePath()
    this.withLockedStore(path) { stateStore =>
      this.begin(stateStore, "source", None, "10", "txn-1")
      stateStore.commit("source", "txn-1")
      val error = intercept[PuddingException] {
        this.begin(stateStore, "source", Some("9"), "20", "txn-2")
      }
      error.getMessage should include ("does not match read start")
    }
  }

  test("require the lock for state mutations") {
    val path = this.statePath()
    val stateStore = this.store(path)

    intercept[PuddingException] {
      this.begin(stateStore, "source", None, "10", "txn-1")
    }
    intercept[PuddingException] {
      stateStore.commit("source", "txn-1")
    }
    intercept[PuddingException] {
      stateStore.abort("source", "txn-1")
    }
    intercept[PuddingException] {
      stateStore.reset("source", Some("10"))
    }
    intercept[PuddingException] {
      stateStore.clear("source")
    }
  }

  test("reset, clear and list state") {
    val path = this.statePath()
    this.withLockedStore(path) { stateStore =>
      this.begin(stateStore, "source", None, "10", "txn-1")
      stateStore.commit("source", "txn-1")

      stateStore.reset("source", Some("3"))
      stateStore.load("source").committedCursor shouldBe Some("3")
      stateStore.load("source").pending shouldBe None
      stateStore.reset("source", None)
      stateStore.load("source").isEmpty shouldBe true

      this.begin(stateStore, "source", None, "4", "txn-2")
      stateStore.commit("source", "txn-2")
    }

    Files.write(path.resolve("legacy.cursor"), "1".getBytes(StandardCharsets.UTF_8))
    val stateStore = this.store(path)
    stateStore.listSourceIds() shouldBe Seq("legacy", "source")

    this.withLockedStore(path) { locked =>
      locked.clear("source")
    }
    this.store(path).listSourceIds() shouldBe Seq("legacy")
  }

  test("list a source id that required path sanitizing") {
    val path = this.statePath()
    val stateStore = this.store(path)
    val sourceId = "hive://orders/2026"
    stateStore.acquire(sourceId)
    try {
      this.begin(stateStore, sourceId, None, "1", "txn-1")
      stateStore.commit(sourceId, "txn-1")
    } finally {
      stateStore.release(sourceId)
    }

    Files.exists(path.resolve(this.fileBase("hive://orders/2026") + ".state")) shouldBe true
    stateStore.listSourceIds() shouldBe Seq(sourceId)
    stateStore.load(sourceId).committedCursor shouldBe Some("1")
  }

  test("reject malformed, incompatible and blank state files") {
    val path = this.statePath()
    val stateStore = this.store(path)

    Files.write(path.resolve(this.fileBase("source") + ".state"), "{not-json".getBytes(StandardCharsets.UTF_8))
    intercept[PuddingException] {
      stateStore.load("source")
    }

    Files.write(
      path.resolve(this.fileBase("source") + ".state"),
      """{"version":1,"sourceId":"other","committedCursor":"1","updatedAt":"now"}"""
        .getBytes(StandardCharsets.UTF_8))
    val other = intercept[PuddingException] {
      stateStore.load("source")
    }
    other.getMessage should include ("belongs to source")

    Files.write(
      path.resolve(this.fileBase("source") + ".state"),
      """{"version":2,"sourceId":"source","committedCursor":"1","updatedAt":"now"}"""
        .getBytes(StandardCharsets.UTF_8))
    val version = intercept[PuddingException] {
      stateStore.load("source")
    }
    version.getMessage should include ("unsupported incremental state version")

    Files.write(path.resolve("blank.cursor"), "  ".getBytes(StandardCharsets.UTF_8))
    stateStore.load("blank").committedCursor shouldBe None
  }

  test("expose lock information without the private token") {
    val path = this.statePath()
    val stateStore = this.store(path)
    stateStore.acquire("source")
    try {
      val info = stateStore.lockInfo("source").get
      info.owner should not be empty
      info.host should not be empty
      info.pid should not be empty
      info.acquiredAt should not be empty
    } finally {
      stateStore.release("source")
    }
    stateStore.lockInfo("source") shouldBe None
  }

  test("reject a lock lease on the filesystem backend") {
    val path = this.statePath()
    val error = intercept[PuddingException] {
      new FileSystemStateBackend(path.toString, new Configuration(), 60000L)
    }
    error.getMessage should include ("lockTtlMs")
    error.getMessage should include ("JDBC")

    val providerError = intercept[PuddingException] {
      new FileSystemStateBackendProvider().create(
        Map("statePath" -> path.toString, "lockTtlMs" -> "60000"),
        new Configuration())
    }
    providerError.getMessage should include ("lockTtlMs")

    // TTL zero remains valid and leaves a lock without an expiry timestamp.
    val store = new IncrementalStateStore(
      new FileSystemStateBackend(path.toString, new Configuration(), 0L))
    store.acquire("source")
    try {
      store.lockInfo("source").get.expiresAt shouldBe None
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("take over an expired legacy lock and increase the fencing token") {
    val path = this.statePath()
    val expired = LockDocument(
      token = "stale-token",
      owner = "stale",
      host = "stale-host",
      pid = "1",
      acquiredAt = Instant.now().minusSeconds(60).toString,
      fencingToken = 4L,
      expiresAt = Some(Instant.now().minusSeconds(30).toString))
    Files.write(
      path.resolve(this.fileBase("source") + ".lock"),
      LockDocument.encode(expired).getBytes(StandardCharsets.UTF_8))

    val store = this.store(path)
    val info = store.acquire("source")
    try {
      info.fencingToken shouldBe 5L
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("require an explicit unlock for a crashed writer lock without expiry") {
    val path = this.statePath()
    val crashed = LockDocument(
      token = "stale-token",
      owner = "stale",
      host = "stale-host",
      pid = "1",
      acquiredAt = Instant.now().minusSeconds(60).toString,
      fencingToken = 4L,
      expiresAt = None)
    Files.write(
      path.resolve(this.fileBase("source") + ".lock"),
      LockDocument.encode(crashed).getBytes(StandardCharsets.UTF_8))

    val store = this.store(path)
    intercept[PuddingException] {
      store.acquire("source")
    }.getMessage should include ("locked")

    store.forceUnlock("source") shouldBe true
    val info = store.acquire("source")
    try {
      info.fencingToken shouldBe 1L
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("refuse a stale writer after the lock token is replaced") {
    val path = this.statePath()
    val first = this.store(path)
    first.acquire("source")
    this.begin(first, "source", None, "10", "txn-1")

    val replacement = LockDocument(
      token = "new-token",
      owner = "new",
      host = "new-host",
      pid = "2",
      acquiredAt = Instant.now().toString,
      fencingToken = 2L,
      expiresAt = Some(Instant.now().plusSeconds(30).toString))
    Files.write(
      path.resolve(this.fileBase("source") + ".lock"),
      LockDocument.encode(replacement).getBytes(StandardCharsets.UTF_8))
    Files.deleteIfExists(path.resolve("." + this.fileBase("source") + ".lock.crc"))

    val error = intercept[PuddingException] {
      first.commit("source", "txn-1")
    }
    error.getMessage should include ("lost")
    first.release("source") shouldBe false
    first.lockInfo("source").get.fencingToken shouldBe 2L
  }

  test("keep the fencing token increasing across normal lock releases") {
    val path = this.statePath()
    val first = this.store(path)
    first.acquire("source")
    this.begin(first, "source", None, "10", "txn-1")
    first.commit("source", "txn-1")
    first.lockInfo("source").get.fencingToken shouldBe 1L
    first.release("source") shouldBe true

    this.store(path).load("source").fencingToken shouldBe 1L
    val second = this.store(path)
    second.acquire("source")
    try {
      second.lockInfo("source").get.fencingToken shouldBe 2L
    } finally {
      second.release("source")
    }
  }

  test("hash plan configuration without storing raw values") {
    def descriptor(
        sourceTable: String,
        sinkConnection: String): IncrementalPlanDescriptor = {
      IncrementalPlanDescriptor.create(
        sourceType = "test-source",
        sourceId = "source",
        sinkType = "test-sink",
        sinkId = "sink",
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id"),
        opColumn = "__op",
        sourceConfig = Some(Map("table" -> sourceTable, "password" -> "top-secret")),
        sinkConfig = Some(Map("table" -> "target", "_pudding.connection" -> sinkConnection)),
        stateBackend = "filesystem")
    }

    val first = descriptor("orders", "warehouse")
    val reordered = descriptor("orders", "warehouse")
    val otherTable = descriptor("customers", "warehouse")
    val otherConnection = descriptor("orders", "archive")

    first.sourceConfigHash.length shouldBe 64
    first.sourceConfigHash should not include ("orders")
    first.sourceConfigHash should not include ("top-secret")
    reordered.fingerprint shouldBe first.fingerprint
    otherTable.fingerprint should not be first.fingerprint
    otherConnection.fingerprint should not be first.fingerprint
  }

  test("require an explicit opt-in for object-store filesystem state") {
    val spec = IncrementalSpec(
      semantics = IncrementalSemantics.Append,
      statePath = Some("s3a://bucket/pudding-state"))

    val error = intercept[PuddingException] {
      IncrementalStateStore.open(spec, new Configuration())
    }
    error.getMessage should include ("best-effort lock")
    error.getMessage should include ("incremental.allowBestEffortLock=true")
  }

  test("recognize COS and a scheme-less object-store default filesystem") {
    val cos = IncrementalSpec(
      semantics = IncrementalSemantics.Append,
      statePath = Some("cosn://bucket/pudding-state"))
    intercept[PuddingException] {
      IncrementalStateStore.open(cos, new Configuration())
    }.getMessage should include ("best-effort lock")

    val conf = new Configuration()
    conf.set("fs.defaultFS", "s3a://bucket")
    val schemeLess = IncrementalSpec(
      semantics = IncrementalSemantics.Append,
      statePath = Some("pudding-state"))
    intercept[PuddingException] {
      IncrementalStateStore.open(schemeLess, conf)
    }.getMessage should include ("best-effort lock")
  }

  test("build stable transaction ids") {
    val first = IncrementalTransaction.id(
      "source", Some("1"), "2", IncrementalSemantics.Upsert, Seq("id"), "__op", Seq("ts"))
    val second = IncrementalTransaction.id(
      "source", Some("1"), "2", IncrementalSemantics.Upsert, Seq("id"), "__op", Seq("ts"))
    val other = IncrementalTransaction.id(
      "source", Some("1"), "3", IncrementalSemantics.Upsert, Seq("id"), "__op", Seq("ts"))

    first shouldBe second
    first should not be other
    first.length shouldBe 64
  }
}
