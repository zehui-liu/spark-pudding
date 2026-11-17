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

package org.pudding.connector.jdbc

import java.sql.DriverManager
import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.definition.{
  IncrementalSemantics,
  IncrementalWrite
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Key-based JDBC upsert behavior against H2's standard `MERGE` implementation.
 */
class JdbcStreamingTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("jdbc streaming upsert test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("merge inserts new keys and updates existing keys in one batch") {
    val url = this.database("merge")
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT PRIMARY KEY,
        |  name VARCHAR(40),
        |  amount INT
        |)""".stripMargin)
    this.execute(url, "INSERT INTO orders (id, name, amount) VALUES (1, 'old', 10)")

    this.merge(url, "orders", this.rows((1, "new", 11), (2, "second", 20)), Seq("id"))

    this.readRows(url, "orders") shouldBe Seq((1, "new", 11), (2, "second", 20))
    this.stagingTables(url) shouldBe empty
  }

  test("merge accepts a composite unique key") {
    val url = this.database("composite")
    this.execute(url,
      """CREATE TABLE daily (
        |  region VARCHAR(20),
        |  business_date VARCHAR(20),
        |  total INT,
        |  CONSTRAINT daily_key UNIQUE (region, business_date)
        |)""".stripMargin)

    JdbcStreaming.merge(
      this.dailyRows(("east", "2026-01-01", 1), ("west", "2026-01-01", 2)),
      this.options(url, "daily"),
      Seq("region", "business_date"))

    this.readDaily(url) shouldBe Seq(("east", "2026-01-01", 1), ("west", "2026-01-01", 2))
    this.stagingTables(url) shouldBe empty
  }

  test("reject a target without a matching primary key or unique index") {
    val url = this.database("nokey")
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT,
        |  name VARCHAR(40),
        |  amount INT
        |)""".stripMargin)

    val error = intercept[PuddingException] {
      this.merge(url, "orders", this.rows((1, "new", 11)), Seq("id"))
    }

    error.getMessage should include ("primary key or unique index")
    error.getMessage should include ("id")
  }

  test("reject rows missing a target column") {
    val url = this.database("missing")
    this.createTarget(url)

    val error = intercept[PuddingException] {
      this.merge(url, "orders", this.rows((1, "new", 11)), Seq("id"), targetColumns = Seq("id", "name"))
    }

    error.getMessage should include ("missing")
    error.getMessage should include ("amount")
  }

  test("reject duplicate keys in one microbatch") {
    val url = this.database("duplicate")
    this.createTarget(url)

    val error = intercept[PuddingException] {
      this.merge(url, "orders", this.rows((1, "first", 1), (1, "second", 2)), Seq("id"))
    }

    error.getMessage should include ("duplicate keys")
    error.getMessage should include ("id")
  }

  test("reject a null key in one microbatch") {
    val url = this.database("null-key")
    this.createTarget(url)
    val session = this.sparkSession
    import session.implicits._
    val data = Seq((None: Option[Int], "name", 1)).toDF("id", "name", "amount")

    val error = intercept[PuddingException] {
      JdbcStreaming.merge(data, this.options(url, "orders"), Seq("id"))
    }

    error.getMessage should include ("null key")
    error.getMessage should include ("id")
  }

  test("reject an unsupported dialect before connecting to the target") {
    val error = intercept[PuddingException] {
      JdbcStreaming.merge(
        this.rows((1, "name", 1)),
        Map(
          "url" -> "jdbc:sqlserver://db:1433/analytics",
          "dbtable" -> "orders",
          "user" -> "app",
          "password" -> "secret",
          "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
        Seq("id"))
    }

    error.getMessage should include ("postgresql, mysql, mariadb and h2")
  }

  test("skip an empty microbatch without creating a staging table") {
    val url = this.database("empty")
    this.createTarget(url)

    this.merge(url, "orders", this.rows(), Seq("id"))

    this.readRows(url, "orders") shouldBe empty
    this.stagingTables(url) shouldBe empty
  }

  test("apply CDC deletes and upserts in one ordered batch") {
    val url = this.database("cdc")
    this.createTarget(url)
    this.execute(url,
      "INSERT INTO orders (id, name, amount) VALUES (1, 'old', 1), (3, 'delete-me', 3)")

    val session = this.sparkSession
    import session.implicits._
    val batch = Seq(
      (1, "updated", 11, "U", 1L),
      (2, "inserted", 22, "I", 2L),
      (3, "delete-me", 33, "D", 3L),
      (4, "absent", 44, "D", 4L)).toDF("id", "name", "amount", "__op", "__seq")

    JdbcStreaming.writeIncremental(
      batch,
      Map.empty,
      this.options(url, "orders"),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "__op",
        fromCursor = Some("0"),
        toCursor = "4",
        cursorType = "long",
        orderFields = Seq("__seq")))

    this.readRows(url, "orders") shouldBe Seq((1, "updated", 11), (2, "inserted", 22))
    this.stagingTables(url) shouldBe empty
  }

  test("order upsert rows using the sink cdcOrderFields") {
    val url = this.database("upsert-order")
    this.createTarget(url)
    val session = this.sparkSession
    import session.implicits._
    val batch = Seq(
      (1, "first", 1, 1L),
      (1, "second", 2, 2L)).toDF("id", "name", "amount", "__seq")

    JdbcStreaming.writeIncremental(
      batch,
      Map("cdcOrderFields" -> Seq("__seq")),
      this.options(url, "orders"),
      IncrementalWrite(
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id"),
        opColumn = "__op",
        fromCursor = Some("0"),
        toCursor = "2",
        cursorType = "long"))

    this.readRows(url, "orders") shouldBe Seq((1, "second", 2))
  }

  test("reject a null key in a CDC upsert row") {
    val url = this.database("cdc-null-upsert")
    this.createTarget(url)
    val session = this.sparkSession
    import session.implicits._
    val batch = Seq((None: Option[Int], "inserted", 1, "I", 1L))
      .toDF("id", "name", "amount", "__op", "__seq")

    val error = intercept[PuddingException] {
      JdbcStreaming.writeIncremental(
        batch,
        Map.empty,
        this.options(url, "orders"),
        IncrementalWrite(
          semantics = IncrementalSemantics.Cdc,
          keyFields = Seq("id"),
          opColumn = "__op",
          fromCursor = Some("0"),
          toCursor = "1",
          cursorType = "long",
          orderFields = Seq("__seq")))
    }

    error.getMessage should include ("null key")
    error.getMessage should include ("id")
    this.readRows(url, "orders") shouldBe empty
  }

  test("keep the last operation per key before applying CDC") {
    val url = this.database("cdc-order")
    this.createTarget(url)
    this.execute(url,
      "INSERT INTO orders (id, name, amount) VALUES (5, 'old', 5), (6, 'old', 6)")

    val session = this.sparkSession
    import session.implicits._
    val batch = Seq(
      (5, "first", 1, "I", 1L),
      (5, "last", 2, "U", 3L),
      (6, "first", 3, "U", 2L),
      (6, "deleted", 4, "D", 4L)).toDF("id", "name", "amount", "__op", "__seq")

    JdbcStreaming.writeIncremental(
      batch,
      Map.empty,
      this.options(url, "orders"),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "__op",
        fromCursor = Some("0"),
        toCursor = "4",
        cursorType = "long",
        orderFields = Seq("__seq")))

    this.readRows(url, "orders") shouldBe Seq((5, "last", 2))
  }

  private def merge(
      url: String,
      table: String,
      data: DataFrame,
      keys: Seq[String],
      targetColumns: Seq[String] = Seq("id", "name", "amount")): Unit = {
    val projected = data.select(targetColumns.map(data(_)): _*)
    JdbcStreaming.merge(projected, this.options(url, table), keys)
  }

  private def rows(values: (Int, String, Int)*): DataFrame = {
    val session = this.sparkSession
    session.createDataFrame(values).toDF("id", "name", "amount")
  }

  private def dailyRows(values: (String, String, Int)*): DataFrame = {
    val session = this.sparkSession
    session.createDataFrame(values).toDF("region", "business_date", "total")
  }

  private def createTarget(url: String): Unit = {
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT PRIMARY KEY,
        |  name VARCHAR(40),
        |  amount INT
        |)""".stripMargin)
  }

  private def database(prefix: String): String = {
    val name = prefix + "_" + UUID.randomUUID().toString.replace("-", "")
    s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
  }

  private def options(url: String, table: String): Map[String, String] = {
    Map(
      "url" -> url,
      "user" -> "sa",
      "password" -> "",
      "driver" -> "org.h2.Driver",
      "dbtable" -> table)
  }

  private def execute(url: String, sql: String): Unit = {
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(sql)
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def readRows(url: String, table: String): Seq[(Int, String, Int)] = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(
          s"SELECT id, name, amount FROM $table ORDER BY id")
        val rows = Seq.newBuilder[(Int, String, Int)]
        while (result.next()) {
          rows += ((result.getInt(1), result.getString(2), result.getInt(3)))
        }
        result.close()
        rows.result()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def readDaily(url: String): Seq[(String, String, Int)] = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(
          "SELECT region, business_date, total FROM daily ORDER BY region")
        val rows = Seq.newBuilder[(String, String, Int)]
        while (result.next()) {
          rows += ((result.getString(1), result.getString(2), result.getInt(3)))
        }
        result.close()
        rows.result()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def stagingTables(url: String): Seq[String] = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(
          "SELECT table_name FROM information_schema.tables " +
            "WHERE table_name LIKE '%pudding_stream%'")
        val names = Seq.newBuilder[String]
        while (result.next()) {
          names += result.getString(1)
        }
        result.close()
        names.result()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }
}
