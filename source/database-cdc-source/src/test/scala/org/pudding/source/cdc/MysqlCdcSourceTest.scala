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

package org.pudding.source.cdc

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.SourceContext

/**
 * End-to-end MySQL binlog test: snapshot, insert/update/delete, checkpoint commit and restart.
 */
class MysqlCdcSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val enabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image = sys.env.getOrElse("PUDDING_TEST_MYSQL_IMAGE", "mysql:8.0")

  private var container: GenericContainer[_] = _

  private var spark: SparkSession = _

  private var checkpoint: Path = _

  private val rows = new CopyOnWriteArrayList[(String, Int, String, String)]()

  override def beforeAll(): Unit = {
    if (this.enabled) {
      try {
        val mysql = new GenericContainer(DockerImageName.parse(this.image))
        mysql.withEnv("MYSQL_ROOT_PASSWORD", "root-secret")
        mysql.withEnv("MYSQL_DATABASE", "inventory")
        mysql.withExposedPorts(3306)
        mysql.withCommand(
          "mysqld",
          "--log-bin=mysql-bin",
          "--binlog-format=ROW",
          "--binlog-row-image=FULL",
          "--server-id=1")
        mysql.waitingFor(
          Wait.forLogMessage(".*mysqld: ready for connections.*", 2)
            .withStartupTimeout(java.time.Duration.ofMinutes(2)))
        this.container = mysql
        this.container.start()
        this.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(50))")
        this.execute("INSERT INTO orders VALUES (1, 'tom'), (2, 'jerry')")
        this.spark = SparkSession.builder()
          .appName("mysql-cdc-source-test")
          .master("local[2]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()
        this.checkpoint = Files.createTempDirectory("mysql-cdc-checkpoint")
      } catch {
        case NonFatal(error) =>
          this.closeAll()
          if (this.required) {
            throw new IllegalStateException(
              "MySQL container is required but the CDC source test failed to start: " +
                error.getClass.getSimpleName,
              error)
          }
      }
    }
  }

  override def afterAll(): Unit = {
    this.closeAll()
  }

  test("snapshot, changes and checkpoint restart") {
    assume(this.container != null && this.spark != null, "MySQL container is not available")
    this.rows.clear()
    var query = this.startQuery()
    try {
      this.waitForOps(Set("I"), 2)
      this.rows.asScala.filter(_. _1 == "I").map(row => row._2 -> row._3).toSet shouldBe
        Set(1 -> "tom", 2 -> "jerry")

      this.execute("INSERT INTO orders VALUES (3, 'spark')")
      this.execute("UPDATE orders SET name = 'tom_v2' WHERE id = 1")
      this.execute("DELETE FROM orders WHERE id = 2")
      this.waitForOps(Set("I", "U", "D"), 4)
      this.rows.asScala.exists(row =>
        row._1 == "U" && row._2 == 1 && row._3 == "tom_v2") shouldBe true
      this.rows.asScala.exists(row =>
        row._1 == "D" && row._2 == 2) shouldBe true
      this.rows.asScala.forall(_._4 == "inventory.orders") shouldBe true
    } finally {
      this.stop(query)
    }

    query = this.startQuery()
    try {
      this.execute("INSERT INTO orders VALUES (4, 'restart')")
      this.waitUntil(60, "row after checkpoint restart") {
        this.rows.asScala.exists(row => row._1 == "I" && row._2 == 4 && row._3 == "restart")
      }
    } finally {
      this.stop(query)
    }
  }

  private def startQuery(): StreamingQuery = {
    val source = new MysqlCdcSource()
    val config = Map[String, Any](
      "host" -> this.container.getHost,
      "port" -> this.container.getMappedPort(3306),
      "database" -> "inventory",
      "table" -> "orders",
      "user" -> "root",
      "password" -> "root-secret",
      "schema" -> "id int, name string",
      "maxRecordsPerBatch" -> 2)
    val frame = source.initData(
      this.spark,
      SourceContext(Some(config), executionMode = Some(ExecutionMode.Streaming)))
    frame.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("500 milliseconds"))
      .option("checkpointLocation", this.checkpoint.toAbsolutePath.toString)
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, _: Long) =>
        batch.collect().foreach { row =>
          this.rows.add((
            row.getAs[String]("__op"),
            row.getAs[Int]("id"),
            row.getAs[String]("name"),
            row.getAs[String]("_cdc_table")))
        }
        ()
      }
      .start()
  }

  private def stop(query: StreamingQuery): Unit = {
    if (query != null) {
      try query.stop()
      catch {
        case NonFatal(_) => ()
      }
      try query.awaitTermination(30)
      catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
  }

  private def waitForOps(expected: Set[String], minimum: Int): Unit = {
    this.waitUntil(90, s"CDC operations $expected") {
      val ops = this.rows.asScala.map(_._1).toSet
      expected.subsetOf(ops) && this.rows.size() >= minimum
    }
  }

  private def waitUntil(seconds: Int, description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds.toLong)
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(250L)
    }
    condition shouldBe true
  }

  private def execute(sql: String): Unit = {
    val connection = DriverManager.getConnection(this.jdbcUrl, "root", "root-secret")
    try {
      val statement = connection.createStatement()
      try statement.execute(sql)
      finally statement.close()
    } finally {
      connection.close()
    }
  }

  private def jdbcUrl: String = {
    s"jdbc:mysql://${this.container.getHost}:${this.container.getMappedPort(3306)}/inventory"
  }

  private def closeAll(): Unit = {
    if (this.spark != null) {
      try this.spark.close()
      catch {
        case NonFatal(_) => ()
      }
      this.spark = null
    }
    if (this.checkpoint != null && Files.exists(this.checkpoint)) {
      try {
        val stream = Files.walk(this.checkpoint)
        try {
          import scala.collection.JavaConverters._
          stream.iterator().asScala.toVector
            .sortBy(_.getNameCount)(Ordering.Int.reverse)
            .foreach(value => Try(Files.deleteIfExists(value)))
        } finally {
          stream.close()
        }
      } catch {
        case NonFatal(_) => ()
      }
      this.checkpoint = null
    }
    if (this.container != null) {
      try this.container.stop()
      catch {
        case NonFatal(_) => ()
      }
      this.container = null
    }
  }
}
