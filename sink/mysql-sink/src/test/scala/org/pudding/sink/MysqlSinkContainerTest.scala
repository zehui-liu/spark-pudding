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

package org.pudding.sink

import java.sql.{Connection, DriverManager}
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.pudding.core.PuddingException
import org.pudding.core.definition.SinkContext
import org.pudding.connector.jdbc.JdbcStreaming
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Opt-in MySQL/InnoDB coverage for the mysql-sink atomic full-refresh contract.
 *
 * The test is enabled by `PUDDING_TESTCONTAINERS=true`. A missing Docker daemon or image skips
 * the suite unless `PUDDING_TESTCONTAINERS_REQUIRED=true` is set. `PUDDING_TEST_MYSQL_IMAGE`
 * overrides the image for restricted registries.
 */
class MysqlSinkContainerTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val enabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image =
    sys.env.getOrElse("PUDDING_TEST_MYSQL_IMAGE", "mysql:8.0")

  private var container: GenericContainer[_] = _

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    if (this.enabled) {
      try {
        this.container = new GenericContainer(DockerImageName.parse(this.image))
        this.container.withEnv("MYSQL_ROOT_PASSWORD", "root-secret")
        this.container.withEnv("MYSQL_DATABASE", "pudding")
        this.container.withEnv("MYSQL_USER", "pudding")
        this.container.withEnv("MYSQL_PASSWORD", "pudding-secret")
        this.container.addExposedPort(3306)
        // The entrypoint starts a temporary server for initialization. Match the mysqld line,
        // not the X Plugin line, and wait for both the temporary and the final server so the
        // container is not reported ready while the temporary server is shutting down.
        this.container.waitingFor(
          Wait.forLogMessage(".*mysqld: ready for connections.*", 2)
            .withStartupTimeout(java.time.Duration.ofMinutes(2)))
        this.container.start()
        this.sparkSession = SparkSession.builder()
          .appName("test for mysql atomic replace")
          .master("local[1]")
          .config("spark.ui.enabled", "false")
          .getOrCreate()
      } catch {
        case NonFatal(error) =>
          this.stopContainer()
          if (this.required) {
            throw new IllegalStateException(
              "MySQL container is required but failed to start: " +
                error.getClass.getSimpleName, error)
          }
      }
    }
  }

  override def afterAll(): Unit = {
    if (this.sparkSession != null) {
      this.sparkSession.stop()
    }
    this.stopContainer()
  }

  test("atomically replace an existing InnoDB table through mysql-sink") {
    assume(this.container != null, "MySQL container is not available")
    val table = this.tableName("replace")
    this.createTable(table, None, 1, "old")
    this.write(table, Seq((2, "new-2"), (3, "new-3")))

    this.readRows(table) shouldBe Seq((2, "new-2"), (3, "new-3"))
    this.stagingTables() shouldBe empty
  }

  test("roll back a failed MySQL replacement and keep the old rows") {
    assume(this.container != null, "MySQL container is not available")
    val table = this.tableName("rollback")
    this.createTable(table, Some(10), 11, "old")

    val error = intercept[PuddingException] {
      this.write(table, Seq((1, "invalid"), (12, "valid")))
    }

    error.getMessage should include ("previous target remains")
    this.readRows(table) shouldBe Seq((11, "old"))
    this.stagingTables() shouldBe empty
  }

  test("upsert MySQL rows by primary key in one transaction") {
    assume(this.container != null, "MySQL container is not available")
    val table = this.tableName("upsert")
    this.createKeyedTable(table, 1, "old")

    this.upsert(table, Seq((1, "new"), (2, "second")))

    this.readRows(table) shouldBe Seq((1, "new"), (2, "second"))
    this.streamingTables() shouldBe empty
  }

  test("upsert MySQL matches only recordKeyFields, not unrelated unique indexes") {
    assume(this.container != null, "MySQL container is not available")
    val table = this.tableName("key_only")
    this.withConnection { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute(
          s"CREATE TABLE $table (id INT PRIMARY KEY, email VARCHAR(80) UNIQUE, " +
            "name VARCHAR(40)) ENGINE=InnoDB")
        statement.execute(
          s"INSERT INTO $table (id, email, name) VALUES (1, 'first@example.com', 'old')")
      } finally {
        statement.close()
      }
    }
    val session = this.sparkSession
    val data = session.createDataFrame(
      Seq((2, "first@example.com", "new"))).toDF("id", "email", "name")

    val error = intercept[PuddingException] {
      JdbcStreaming.merge(
        data,
        Map(
          "url" -> this.jdbcUrl,
          "dbtable" -> table,
          "user" -> "pudding",
          "password" -> "pudding-secret",
          "driver" -> "com.mysql.cj.jdbc.Driver"),
        Seq("id"))
    }

    error.getMessage should include ("rolled back")
    this.readEmailRows(table) shouldBe Seq((1, "first@example.com", "old"))
    this.streamingTables() shouldBe empty
  }

  test("create a missing MySQL target table") {
    assume(this.container != null, "MySQL container is not available")
    val table = this.tableName("create")
    this.write(table, Seq((1, "new-1"), (2, "new-2")))

    this.readRows(table) shouldBe Seq((1, "new-1"), (2, "new-2"))
    this.stagingTables() shouldBe empty
  }

  private def tableName(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.replace("-", "").substring(0, 8)
  }

  private def createTable(
      table: String,
      checkMin: Option[Int],
      oldId: Int,
      oldName: String): Unit = {
    val check = checkMin
      .map(value => s", CONSTRAINT ${table}_check CHECK (id > $value)")
      .getOrElse("")
    this.withConnection { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute(
          s"CREATE TABLE $table (id INT, name VARCHAR(40)$check) ENGINE=InnoDB")
        statement.execute(s"INSERT INTO $table (id, name) VALUES ($oldId, '$oldName')")
      } finally {
        statement.close()
      }
    }
  }

  private def createKeyedTable(
      table: String,
      oldId: Int,
      oldName: String): Unit = {
    this.withConnection { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute(
          s"CREATE TABLE $table (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB")
        statement.execute(s"INSERT INTO $table (id, name) VALUES ($oldId, '$oldName')")
      } finally {
        statement.close()
      }
    }
  }

  private def upsert(table: String, rows: Seq[(Int, String)]): Unit = {
    val session = this.sparkSession
    val data = session.createDataFrame(rows).toDF("id", "name")
    JdbcStreaming.merge(
      data,
      Map(
        "url" -> this.jdbcUrl,
        "dbtable" -> table,
        "user" -> "pudding",
        "password" -> "pudding-secret",
        "driver" -> "com.mysql.cj.jdbc.Driver"),
      Seq("id"))
  }

  private def write(table: String, rows: Seq[(Int, String)]): Unit = {
    val session = this.sparkSession
    val data = session.createDataFrame(rows).toDF("id", "name")
    new MysqlSink().writeFullRefresh(data, SinkContext(Some(Map[String, Any](
      "url" -> this.jdbcUrl,
      "table" -> table,
      "user" -> "pudding",
      "password" -> "pudding-secret"))))
  }

  private def readEmailRows(table: String): Seq[(Int, String, String)] =
    this.withConnection { connection =>
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(
          s"SELECT id, email, name FROM $table ORDER BY id")
        val rows = Seq.newBuilder[(Int, String, String)]
        while (result.next()) {
          rows += ((result.getInt(1), result.getString(2), result.getString(3)))
        }
        result.close()
        rows.result()
      } finally {
        statement.close()
      }
    }

  private def readRows(table: String): Seq[(Int, String)] = this.withConnection { connection =>
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(s"SELECT id, name FROM $table ORDER BY id")
      val rows = Seq.newBuilder[(Int, String)]
      while (result.next()) {
        rows += ((result.getInt(1), result.getString(2)))
      }
      result.close()
      rows.result()
    } finally {
      statement.close()
    }
  }

  private def stagingTables(): Seq[String] = this.withConnection { connection =>
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(
        "SELECT table_name FROM information_schema.tables " +
          "WHERE table_schema = DATABASE() AND table_name LIKE '%pudding_stage%' " +
          "ORDER BY table_name")
      val names = Seq.newBuilder[String]
      while (result.next()) {
        names += result.getString(1)
      }
      result.close()
      names.result()
    } finally {
      statement.close()
    }
  }

  private def streamingTables(): Seq[String] = this.withConnection { connection =>
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(
        "SELECT table_name FROM information_schema.tables " +
          "WHERE table_schema = DATABASE() AND table_name LIKE '%pudding_stream%' " +
          "ORDER BY table_name")
      val names = Seq.newBuilder[String]
      while (result.next()) {
        names += result.getString(1)
      }
      result.close()
      names.result()
    } finally {
      statement.close()
    }
  }

  private def jdbcUrl: String = {
    val host = this.container.getHost
    val port = this.container.getMappedPort(3306)
    s"jdbc:mysql://$host:$port/pudding?allowPublicKeyRetrieval=true&useSSL=false"
  }

  private def withConnection[T](body: Connection => T): T = {
    val connection = DriverManager.getConnection(
      this.jdbcUrl,
      "pudding",
      "pudding-secret")
    try {
      body(connection)
    } finally {
      connection.close()
    }
  }

  private def stopContainer(): Unit = {
    if (this.container != null) {
      try this.container.stop()
      catch {
        case NonFatal(_) => ()
      }
      this.container = null
    }
  }
}
