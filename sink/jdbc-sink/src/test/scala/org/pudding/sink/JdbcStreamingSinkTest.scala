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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.sql.DriverManager
import java.util.{Comparator, UUID}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.SinkContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * End-to-end checkpointed streaming writes for `jdbc-sink`, using H2 as the target.
 */
class JdbcStreamingSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("jdbc streaming sink test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  test("checkpointed update output upserts full rows by recordKeyFields") {
    val url = this.database("update")
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT PRIMARY KEY,
        |  name VARCHAR(40),
        |  amount INT
        |)""".stripMargin)
    this.execute(url, "INSERT INTO orders (id, name, amount) VALUES (1, 'old', 10)")
    val input = this.inputDirectory(
      """{"id": 1, "name": "new", "amount": 11}""",
      """{"id": 2, "name": "second", "amount": 20}""")
    val checkpoint = this.checkpointDirectory()
    val source = this.sparkSession.readStream
      .schema("id INT, name STRING, amount INT")
      .json(input.toString)
    val stream = source
      .groupBy("id")
      .agg(max("name").as("name"), max("amount").as("amount"))

    try {
      new JdbcSink().writer(stream, this.updateContext(url, checkpoint))
    } finally {
      this.deleteTree(input)
      this.deleteTree(Paths.get(checkpoint))
    }

    this.readRows(url) shouldBe Seq((1, "new", 11), (2, "second", 20))
    this.stagingTables(url) shouldBe empty
  }

  test("checkpointed append output writes each microbatch") {
    val url = this.database("append")
    this.execute(url,
      """CREATE TABLE orders (
        |  id INT PRIMARY KEY,
        |  name VARCHAR(40),
        |  amount INT
        |)""".stripMargin)
    val input = this.inputDirectory(
      """{"id": 1, "name": "first", "amount": 10}""",
      """{"id": 2, "name": "second", "amount": 20}""")
    val checkpoint = this.checkpointDirectory()
    val source = this.sparkSession.readStream
      .schema("id INT, name STRING, amount INT")
      .json(input.toString)

    try {
      new JdbcSink().writer(
        source,
        SinkContext(
          config = Some(Map[String, Any](
            "url" -> url,
            "table" -> "orders",
            "user" -> "sa",
            "password" -> "",
            "driver" -> "org.h2.Driver",
            "checkpointLocation" -> checkpoint)),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    } finally {
      this.deleteTree(input)
      this.deleteTree(Paths.get(checkpoint))
    }

    this.readRows(url) shouldBe Seq((1, "first", 10), (2, "second", 20))
  }

  private def updateContext(url: String, checkpoint: String): SinkContext = {
    SinkContext(
      config = Some(Map[String, Any](
        "url" -> url,
        "table" -> "orders",
        "user" -> "sa",
        "password" -> "",
        "driver" -> "org.h2.Driver",
        "checkpointLocation" -> checkpoint,
        "outputMode" -> "update",
        "recordKeyFields" -> Seq("id"))),
      executionMode = Some(ExecutionMode.Streaming),
      trigger = Some("available-now"))
  }

  private def database(prefix: String): String = {
    val name = prefix + "_" + UUID.randomUUID().toString.replace("-", "")
    s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
  }

  private def inputDirectory(lines: String*): Path = {
    val directory = Files.createTempDirectory("jdbc-stream-input")
    Files.write(
      directory.resolve("batch.json"),
      lines.asJava,
      StandardCharsets.UTF_8)
    directory
  }

  private def checkpointDirectory(): String =
    Files.createTempDirectory("jdbc-stream-checkpoint").toString

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

  private def readRows(url: String): Seq[(Int, String, Int)] = {
    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(
          "SELECT id, name, amount FROM orders ORDER BY id")
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

  private def deleteTree(root: Path): Unit = {
    if (Files.exists(root)) {
      val stream = Files.walk(root)
      try {
        stream.sorted(Comparator.reverseOrder[Path]()).forEach { path =>
          Files.deleteIfExists(path)
          ()
        }
      } finally {
        stream.close()
      }
    }
  }
}
