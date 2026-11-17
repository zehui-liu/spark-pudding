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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.DriverManager
import java.time.Instant
import java.util.UUID

import org.apache.hadoop.conf.Configuration

import org.pudding.core.PuddingException
import org.pudding.core.definition.IncrementalSemantics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the JDBC state backend using an in-memory H2 database.
 */
class JdbcStateBackendTest extends AnyFunSuite with Matchers {

  private val plan = IncrementalPlanDescriptor.create(
    sourceType = "test-source",
    sourceId = "source",
    sinkType = "test-sink",
    sinkId = "sink",
    semantics = IncrementalSemantics.Upsert,
    keyFields = Seq("id"),
    opColumn = "__op",
    sourceConfig = Some(Map("path" -> "/source")),
    sinkConfig = Some(Map("table" -> "target")),
    stateBackend = "jdbc").copy(orderFields = Seq("ts"))

  private def database(): (String, String) = {
    val name = "pudding_" + UUID.randomUUID().toString.replace("-", "")
    val url = s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1"
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(
          """CREATE TABLE pudding_incremental_state (
            |  source_id VARCHAR(255) PRIMARY KEY,
            |  state_json TEXT NOT NULL,
            |  lock_owner VARCHAR(255),
            |  lock_host VARCHAR(255),
            |  lock_pid VARCHAR(255),
            |  lock_token VARCHAR(64),
            |  lock_acquired_at VARCHAR(64),
            |  lock_fencing_token BIGINT,
            |  lock_expires_at_ms BIGINT
            |)""".stripMargin)
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
    (url, name)
  }

  private def emptyDatabase(): String = {
    val name = "pudding_" + UUID.randomUUID().toString.replace("-", "")
    s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1"
  }

  private def legacyDatabase(): String = {
    val url = this.emptyDatabase()
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(
          """CREATE TABLE pudding_incremental_state (
            |  source_id VARCHAR(255) PRIMARY KEY,
            |  state_json TEXT NOT NULL,
            |  lock_owner VARCHAR(255),
            |  lock_host VARCHAR(255),
            |  lock_pid VARCHAR(255),
            |  lock_token VARCHAR(64),
            |  lock_acquired_at VARCHAR(64)
            |)""".stripMargin)
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
    url
  }

  private def columns(url: String, schema: String = "PUBLIC"): Set[String] = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val result = connection.getMetaData
        .getColumns(null, schema, "PUDDING_INCREMENTAL_STATE", null)
      try {
        val values = scala.collection.mutable.Set[String]()
        while (result.next()) {
          values += result.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT)
        }
        values.toSet
      } finally {
        result.close()
      }
    } finally {
      connection.close()
    }
  }

  private def store(url: String): IncrementalStateStore = {
    new IncrementalStateStore(new JdbcStateBackend(Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> "")))
  }

  private def store(url: String, lockTtlMs: Long): IncrementalStateStore = {
    new IncrementalStateStore(new JdbcStateBackend(Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> "",
      "lockTtlMs" -> lockTtlMs.toString)))
  }

  private def expireLock(url: String, sourceId: String): Unit = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.prepareStatement(
        "UPDATE pudding_incremental_state SET lock_expires_at_ms = ? WHERE source_id = ?")
      try {
        statement.setLong(1, Instant.now().minusSeconds(30).toEpochMilli)
        statement.setString(2, sourceId)
        statement.executeUpdate()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  test("persist, lock, commit, reset and clear state") {
    val (url, _) = this.database()
    val first = this.store(url)
    val second = this.store(url)

    first.location should startWith ("jdbc:")
    first.load("source").isEmpty shouldBe true

    first.acquire("source")
    intercept[PuddingException] {
      second.acquire("source")
    }
    first.begin("source", None, "10", "txn-1", this.plan)
    first.load("source").pending.map(_.plan.get.fingerprint) shouldBe
      Some(this.plan.fingerprint)
    first.commit("source", "txn-1")
    first.release("source") shouldBe true

    second.load("source").committedCursor shouldBe Some("10")
    second.load("source").lastTransactionId shouldBe Some("txn-1")
    second.load("source").lastCommittedAt should not be empty
    second.listSourceIds() shouldBe Seq("source")

    second.acquire("source")
    try {
      second.reset("source", Some("3"))
      second.load("source").committedCursor shouldBe Some("3")
      second.clear("source")
    } finally {
      second.release("source")
    }
    second.listSourceIds() shouldBe empty
  }

  test("track attempts and the last failure, then clear them on commit") {
    val (url, _) = this.database()
    val store = this.store(url)

    store.acquire("source")
    try {
      store.begin("source", None, "10", "txn-1", this.plan)
      store.recordAttempt("source", "txn-1")
      store.load("source").attempts shouldBe 2
      store.recordFailure("source", "token=secret-value password=bad")
      val failed = store.load("source")
      failed.lastFailureAt should not be empty
      failed.lastFailureReason.get should not include ("secret-value")
      failed.lastFailureReason.get should not include ("bad")

      store.commit("source", "txn-1")
      val committed = store.load("source")
      committed.attempts shouldBe 0
      committed.lastFailureAt shouldBe None
      committed.lastFailureReason shouldBe None
      committed.committedCursor shouldBe Some("10")
    } finally {
      store.release("source")
    }
  }

  test("reject a stale writer after force unlock") {
    val (url, _) = this.database()
    val first = this.store(url)
    val second = this.store(url)

    first.acquire("source")
    second.forceUnlock("source") shouldBe true
    val lost = intercept[PuddingException] {
      first.begin("source", None, "10", "txn-lost", this.plan)
    }
    lost.getMessage should include ("lost")
    first.release("source") shouldBe false

    second.acquire("source")
    second.begin("source", None, "10", "txn-new", this.plan)
    second.commit("source", "txn-new")
    second.release("source") shouldBe true
    second.load("source").committedCursor shouldBe Some("10")
  }

  test("renew a JDBC lock lease while the writer is active") {
    val (url, _) = this.database()
    val first = this.store(url, 2000L)
    first.acquire("source")
    try {
      val info = first.lockInfo("source").get
      info.expiresAt should not be empty
      info.fencingToken shouldBe 1L
      Thread.sleep(3000L)
      val second = this.store(url, 2000L)
      val error = intercept[PuddingException] {
        second.acquire("source")
      }
      error.getMessage should include ("locked")
      first.ensureLock("source")
    } finally {
      first.release("source")
    }
  }

  test("take over an expired JDBC lock after the previous writer stops") {
    val (url, _) = this.database()
    val first = this.store(url, 60000L)
    first.acquire("source")
    first.begin("source", None, "10", "txn-1", this.plan)
    first.close()
    this.expireLock(url, "source")

    val second = this.store(url, 60000L)
    val info = second.acquire("source")
    info.fencingToken shouldBe 2L
    try {
      second.load("source").pending.map(_.transactionId) shouldBe Some("txn-1")
      second.abort("source", "txn-1")
    } finally {
      second.release("source") shouldBe true
    }
  }

  test("condition JDBC state writes on the current lock token") {
    val (url, _) = this.database()
    val backend = new JdbcStateBackend(Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> "")) {
      def currentToken(sourceId: String): String =
        readLock(sourceId).map(_.token).getOrElse("")
      def writeState(state: IncrementalState, token: String): Unit =
        persistState(state, token)
    }
    backend.acquire("source")
    try {
      val token = backend.currentToken("source")
      token should not be empty
      val state = IncrementalState(
        sourceId = "source",
        committedCursor = Some("1"),
        pending = None,
        updatedAt = Instant.now().toString)
      val error = intercept[PuddingException] {
        backend.writeState(state, "replaced-token")
      }
      error.getMessage should include ("lost")
      backend.writeState(state, token)
      backend.load("source").committedCursor shouldBe Some("1")
    } finally {
      backend.release("source")
    }
  }

  test("create a missing JDBC state table through migration") {
    val url = this.emptyDatabase()
    JdbcStateBackend.migrate(Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> "")) should startWith ("jdbc:")
    this.columns(url) should contain allOf (
      "source_id",
      "lock_fencing_token",
      "lock_expires_at_ms")
    val store = this.store(url)
    store.acquire("source")
    try {
      store.begin("source", None, "10", "txn-1", this.plan)
      store.commit("source", "txn-1")
      store.load("source").committedCursor shouldBe Some("10")
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("migrate only the state table in the connection schema") {
    val url = this.emptyDatabase()
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        statement.execute("CREATE SCHEMA other")
        statement.execute(
          """CREATE TABLE other.pudding_incremental_state (
            |  source_id VARCHAR(255) PRIMARY KEY,
            |  state_json TEXT NOT NULL,
            |  lock_fencing_token BIGINT,
            |  lock_expires_at_ms BIGINT
            |)""".stripMargin)
        statement.execute(
          """CREATE TABLE pudding_incremental_state (
            |  source_id VARCHAR(255) PRIMARY KEY,
            |  state_json TEXT NOT NULL
            |)""".stripMargin)
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }

    JdbcStateBackend.migrate(Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> ""))

    this.columns(url) should contain allOf ("lock_fencing_token", "lock_expires_at_ms")
    this.columns(url, "OTHER") should not contain "lock_owner"
  }

  test("upgrade a legacy JDBC state table on demand") {
    val url = this.legacyDatabase()
    this.columns(url) should not contain "lock_fencing_token"
    JdbcStateBackend.migrate(Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> ""))
    this.columns(url) should contain allOf ("lock_fencing_token", "lock_expires_at_ms")

    val store = this.store(url, 60000L)
    store.acquire("source")
    try {
      store.begin("source", None, "10", "txn-1", this.plan)
      store.commit("source", "txn-1")
      store.load("source").committedCursor shouldBe Some("10")
      store.lockInfo("source").get.expiresAt should not be empty
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("migrate the schema through the state tool") {
    val url = this.legacyDatabase()
    val file = Files.createTempFile("pudding-state", ".properties")
    Files.write(
      file,
      s"backend=jdbc\nurl=$url\nuser=sa\npassword=\n".getBytes(StandardCharsets.UTF_8))
    val output = new java.io.ByteArrayOutputStream()
    val err = new java.io.ByteArrayOutputStream()
    val code = IncrementalStateTool.run(
      Array("migrate", s"@${file.toString}"),
      new java.io.PrintStream(output),
      new java.io.PrintStream(err))
    code shouldBe 0
    new String(output.toByteArray, StandardCharsets.UTF_8) should include ("migrated")
    this.columns(url) should contain ("lock_fencing_token")
  }

  test("select JDBC through a state config file and a job spec") {
    val (url, _) = this.database()
    val file = Files.createTempFile("pudding-state", ".properties")
    Files.write(
      file,
      s"backend=jdbc\nurl=$url\nuser=sa\npassword=\n".getBytes(StandardCharsets.UTF_8))
    val configured = IncrementalStateStore.openTarget(
      "@" + file.toString, new Configuration())
    configured.acquire("source")
    try {
      configured.begin("source", None, "10", "txn-config", this.plan)
      configured.commit("source", "txn-config")
    } finally {
      configured.release("source")
    }
    configured.load("source").committedCursor shouldBe Some("10")

    val spec = org.pudding.core.definition.IncrementalSpec(
      semantics = IncrementalSemantics.Upsert,
      keyFields = Seq("id"),
      state = Some(org.pudding.core.definition.IncrementalStateConf(
        "jdbc", Map("url" -> url, "user" -> "sa"))))
    val fromSpec = IncrementalStateStore.open(spec, new Configuration())
    fromSpec.load("source").committedCursor shouldBe Some("10")
  }

  test("query JDBC state through the state tool properties target") {
    val (url, _) = this.database()
    val store = this.store(url)
    store.acquire("source")
    try {
      store.begin("source", None, "10", "txn-tool", this.plan)
      store.commit("source", "txn-tool")
    } finally {
      store.release("source")
    }
    val file = Files.createTempFile("pudding-state", ".properties")
    Files.write(
      file,
      s"backend=jdbc\nurl=$url\nuser=sa\npassword=\n".getBytes(StandardCharsets.UTF_8))
    val out = new java.io.ByteArrayOutputStream()
    val err = new java.io.ByteArrayOutputStream()
    val code = IncrementalStateTool.run(
      Array("status", "@" + file.toString, "source", "--json"),
      new java.io.PrintStream(out, true, StandardCharsets.UTF_8.name()),
      new java.io.PrintStream(err, true, StandardCharsets.UTF_8.name()))
    code shouldBe 0
    err.toString(StandardCharsets.UTF_8.name()) shouldBe empty
    val text = out.toString(StandardCharsets.UTF_8.name())
    text should include ("\"committedCursor\":\"10\"")
    text should include ("\"location\":\"jdbc:")
  }

  test("reject an invalid table name before opening a connection") {
    val error = intercept[PuddingException] {
      new JdbcStateBackend(Map(
        "url" -> "jdbc:h2:mem:unused",
        "table" -> "state; DROP TABLE users"))
    }
    error.getMessage should include ("table may contain only")
  }
}
