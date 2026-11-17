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

import java.sql.DriverManager
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.{
  DataSink,
  IncrementalSemantics,
  SinkContext
}
import org.pudding.connector.jdbc.{JdbcAtomicReplace, JdbcConnectorConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for JDBC sink option construction and atomic replacement.
 */
class JdbcSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("jdbc atomic replace test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "analytics-db",
      `type` = "jdbc",
      endpoint = Some("postgres:5432"),
      auth = Map.empty,
      properties = Map.empty,
      options = Map(
        "url" -> "jdbc:postgresql://postgres:5432/analytics",
        "user" -> "pudding",
        "password" -> "secret"))
  }

  test("registers the jdbc-sink identifier and incremental capabilities") {
    val sink = new JdbcSink()
    sink.identifier shouldBe "jdbc-sink"
    sink.supportsConnection shouldBe true
    sink.supportsIncrementalSync shouldBe true
    sink.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)
    sink.isIdempotentIncremental(IncrementalSemantics.Append) shouldBe false
    sink.isIdempotentIncremental(IncrementalSemantics.Upsert) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Cdc) shouldBe true
    DataSink.identifiers should contain ("jdbc-sink")
  }

  test("build writer options from a connection and infer the PostgreSQL driver") {
    val options = JdbcSink.writerOptions(SinkContext(
      config = Some(Map[String, Any]("table" -> "public.orders")),
      connection = Some(connection)))

    options("url") shouldBe "jdbc:postgresql://postgres:5432/analytics"
    options("dbtable") shouldBe "public.orders"
    options("driver") shouldBe "org.postgresql.Driver"
  }

  test("reject a wrong connection type and missing configuration") {
    val wrongConnection = intercept[PuddingException] {
      JdbcSink.writerOptions(SinkContext(
        config = Some(Map[String, Any]("table" -> "orders")),
        connection = Some(connection.copy(`type` = "mongodb"))))
    }
    wrongConnection.getMessage should include ("requires a jdbc connection")

    intercept[PuddingException] {
      JdbcSink.writerOptions(SinkContext(None))
    }
  }

  test("require a checkpoint for a JDBC streaming write") {
    val session = this.sparkSession
    import session.implicits._
    val data = Seq((1, "name")).toDF("id", "name")

    val error = intercept[PuddingException] {
      new JdbcSink().writer(data, SinkContext(
        config = Some(Map[String, Any](
          "url" -> "jdbc:postgresql://db:5432/analytics",
          "table" -> "orders",
          "user" -> "app",
          "password" -> "secret")),
        executionMode = Some(ExecutionMode.Streaming)))
    }

    error.getMessage should include ("checkpointLocation")
  }

  test("reject unsupported JDBC streaming output modes and missing keys") {
    val session = this.sparkSession
    import session.implicits._
    val data = Seq((1, "name")).toDF("id", "name")
    val base = Map[String, Any](
      "url" -> "jdbc:postgresql://db:5432/analytics",
      "table" -> "orders",
      "user" -> "app",
      "password" -> "secret",
      "checkpointLocation" -> "/tmp/pudding-jdbc-test-checkpoint")

    val complete = intercept[PuddingException] {
      new JdbcSink().writer(data, SinkContext(
        config = Some(base + ("outputMode" -> "complete")),
        executionMode = Some(ExecutionMode.Streaming)))
    }
    complete.getMessage should include ("outputMode")

    val missingKeys = intercept[PuddingException] {
      new JdbcSink().writer(data, SinkContext(
        config = Some(base + ("outputMode" -> "update")),
        executionMode = Some(ExecutionMode.Streaming)))
    }
    missingKeys.getMessage should include ("recordKeyFields")

    val keysWithoutUpdate = intercept[PuddingException] {
      new JdbcSink().writer(data, SinkContext(
        config = Some(base + ("recordKeyFields" -> Seq("id"))),
        executionMode = Some(ExecutionMode.Streaming)))
    }
    keysWithoutUpdate.getMessage should include ("outputMode=update")
  }

  test("atomically replace an existing JDBC table") {
    val database = "atomic_" + UUID.randomUUID().toString.replace("-", "")
    val url = s"jdbc:h2:mem:$database;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
    this.createTable(url, "orders", "old", 1)
    val data = sparkSession.createDataFrame(Seq(
      (1, "new-1"),
      (2, "new-2"))).toDF("id", "name")

    JdbcAtomicReplace.write(data, this.options(url, "orders"))

    this.readNames(url, "orders") shouldBe Seq("new-1", "new-2")
  }

  test("never overwrite a case-folded table through a differently cased name") {
    val database = "folded_" + UUID.randomUUID().toString.replace("-", "")
    val url = s"jdbc:h2:mem:$database;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
    this.createTable(url, "orders", "old", 1)
    val data = sparkSession.createDataFrame(Seq(
      (1, "new-1"))).toDF("id", "name")

    // The quoted probe for "Orders" misses the folded table. Metadata confirms that the target
    // exists, so the write must fail safely instead of taking the destructive overwrite path.
    intercept[Exception] {
      JdbcAtomicReplace.write(data, this.options(url, "Orders"))
    }

    this.readNames(url, "orders") shouldBe Seq("old")
  }

  test("preserve the old table when the atomic insert violates a constraint") {
    val database = "rollback_" + UUID.randomUUID().toString.replace("-", "")
    val url = s"jdbc:h2:mem:$database;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
    this.createTable(url, "orders", "old", 11, checkId = Some(10))
    val data = sparkSession.createDataFrame(Seq(
      (1, "invalid"),
      (11, "valid"))).toDF("id", "name")

    val error = intercept[PuddingException] {
      JdbcAtomicReplace.write(data, this.options(url, "orders"))
    }

    error.getMessage should include ("previous target remains")
    this.readNames(url, "orders") shouldBe Seq("old")
  }

  private def options(url: String, table: String): Map[String, String] = {
    Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> "",
      "driver" -> "org.h2.Driver",
      "dbtable" -> table)
  }

  private def createTable(
      url: String,
      table: String,
      oldName: String,
      oldId: Int,
      checkId: Option[Int] = None): Unit = {
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val check = checkId.map(value => s""" CHECK ("id" > $value)""").getOrElse("")
      val statement = connection.createStatement()
      try {
        statement.execute(
          s"""CREATE TABLE "$table" ("id" INT$check, "name" VARCHAR(20))""")
        statement.execute(
          s"""INSERT INTO "$table" ("id", "name") VALUES ($oldId, '$oldName')""")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def readNames(url: String, table: String): Seq[String] = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(
          s"""SELECT "name" FROM "$table" ORDER BY "id"""")
        val names = Seq.newBuilder[String]
        while (result.next()) {
          names += result.getString(1)
        }
        names.result()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  test("validate save modes through the shared JDBC configuration") {
    JdbcConnectorConfig.saveMode(Map("mode" -> "Append")) shouldBe "append"
    JdbcConnectorConfig.saveMode(Map.empty) shouldBe "overwrite"
    intercept[PuddingException] {
      JdbcConnectorConfig.saveMode(Map("mode" -> "merge"))
    }
  }
}
