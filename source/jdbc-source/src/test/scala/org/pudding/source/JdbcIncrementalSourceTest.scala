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

package org.pudding.source

import java.sql.DriverManager
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.pudding.core.PuddingException
import org.pudding.core.definition.{
  IncrementalRequest,
  IncrementalSemantics,
  SourceContext
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Cursor-based incremental reads against H2, including numeric and timestamp cursors.
 */
class JdbcIncrementalSourceTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("jdbc incremental source test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("read an append range and advance the cursor") {
    val url = this.database("append")
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT PRIMARY KEY,
        |  name VARCHAR(40),
        |  updated_at BIGINT
        |)""".stripMargin)
    this.execute(url,
      "INSERT INTO orders (id, name, updated_at) VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")

    val read = new JdbcSource().readIncremental(
      this.sparkSession,
      this.context(url, "updated_at"),
      this.request(IncrementalSemantics.Append, start = Some("1")))

    read.fromCursor shouldBe Some("1")
    read.toCursor shouldBe "3"
    read.orderFields shouldBe empty
    read.dataFrame.select("id", "name").collect().map(row => (row.getInt(0), row.getString(1)))
      .toSet shouldBe Set((2, "b"), (3, "c"))
  }

  test("upsert reads keep the latest row per key in the range") {
    val url = this.database("upsert")
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT,
        |  name VARCHAR(40),
        |  updated_at BIGINT
        |)""".stripMargin)
    this.execute(url,
      """INSERT INTO orders (id, name, updated_at)
        |VALUES (1, 'old', 1), (1, 'new', 3), (2, 'other', 2)""".stripMargin)

    val read = new JdbcSource().readIncremental(
      this.sparkSession,
      this.context(url, "updated_at"),
      this.request(
        IncrementalSemantics.Upsert,
        start = Some("1"),
        keys = Seq("id")))

    read.orderFields shouldBe Seq("updated_at")
    val rows = read.dataFrame.select("id", "name", "updated_at").collect()
      .map(row => (row.getInt(0), row.getString(1), row.getLong(2))).toSet
    rows shouldBe Set((1, "new", 3L), (2, "other", 2L))
  }

  test("initial latest starts at the current maximum without reading history") {
    val url = this.database("latest")
    this.execute(url,
      "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(40), updated_at BIGINT)")
    this.execute(url,
      "INSERT INTO orders (id, name, updated_at) VALUES (1, 'a', 10)")

    val read = new JdbcSource().readIncremental(
      this.sparkSession,
      this.context(url, "updated_at"),
      this.request(IncrementalSemantics.Append, initial = "latest"))

    read.fromCursor shouldBe Some("10")
    read.toCursor shouldBe "10"
    read.dataFrame.count() shouldBe 0
  }

  test("initial full reads all current rows") {
    val url = this.database("full")
    this.execute(url,
      "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(40), updated_at BIGINT)")
    this.execute(url,
      "INSERT INTO orders (id, name, updated_at) VALUES (1, 'a', 10), (2, 'b', 20)")

    val read = new JdbcSource().readIncremental(
      this.sparkSession,
      this.context(url, "updated_at"),
      this.request(IncrementalSemantics.Append, initial = "full"))

    read.fromCursor shouldBe None
    read.toCursor shouldBe "20"
    read.dataFrame.count() shouldBe 2
  }

  test("support a timestamp cursor") {
    val url = this.database("timestamp")
    this.execute(url,
      "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(40), updated_at TIMESTAMP)")
    this.execute(url,
      """INSERT INTO orders (id, name, updated_at) VALUES
        |(1, 'a', TIMESTAMP '2026-01-01 00:00:00'),
        |(2, 'b', TIMESTAMP '2026-01-02 00:00:00')""".stripMargin)

    val read = new JdbcSource().readIncremental(
      this.sparkSession,
      this.context(url, "updated_at"),
      this.request(
        IncrementalSemantics.Append,
        start = Some("2026-01-01 00:00:00")))

    read.cursorType.toLowerCase should include ("timestamp")
    read.toCursor should startWith ("2026-01-02")
    read.dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2)
  }

  test("reject cdc semantics and a backwards configured range") {
    val url = this.database("errors")
    this.execute(url,
      "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(40), updated_at BIGINT)")
    this.execute(url,
      "INSERT INTO orders (id, name, updated_at) VALUES (1, 'a', 10)")

    val cdc = intercept[PuddingException] {
      new JdbcSource().readIncremental(
        this.sparkSession,
        this.context(url, "updated_at"),
        this.request(IncrementalSemantics.Cdc, start = Some("1"), keys = Seq("id")))
    }
    cdc.getMessage should include ("cdc is not supported")

    val backwards = intercept[PuddingException] {
      new JdbcSource().readIncremental(
        this.sparkSession,
        this.context(url, "updated_at"),
        this.request(
          IncrementalSemantics.Append,
          start = Some("20"),
          end = Some("10")))
    }
    backwards.getMessage should include ("after")
  }

  private def context(url: String, cursorField: String): SourceContext = {
    SourceContext(config = Some(Map[String, Any](
      "table" -> "orders",
      "url" -> url,
      "user" -> "sa",
      "password" -> "",
      "driver" -> "org.h2.Driver",
      "cursorField" -> cursorField)))
  }

  private def request(
      semantics: IncrementalSemantics,
      start: Option[String] = None,
      end: Option[String] = None,
      initial: String = "none",
      keys: Seq[String] = Seq.empty): IncrementalRequest = {
    IncrementalRequest(
      semantics = semantics,
      keyFields = keys,
      opColumn = "__op",
      start = start,
      end = end,
      initial = initial)
  }

  private def database(name: String): String = {
    s"jdbc:h2:mem:${name}_${UUID.randomUUID().toString.replace("-", "")};" +
      "DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
  }

  private def execute(url: String, sql: String): Unit = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try statement.execute(sql) finally statement.close()
    } finally {
      connection.close()
    }
  }
}
