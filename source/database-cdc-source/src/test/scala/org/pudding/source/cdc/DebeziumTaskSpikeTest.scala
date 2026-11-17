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

package org.pudding.source.cdc

import java.sql.DriverManager
import java.util
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import io.debezium.connector.mysql.MySqlConnector
import org.apache.kafka.connect.source.{SourceRecord, SourceTask, SourceTaskContext}
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Technical spike for the native CDC source. It validates that a Debezium connector task can be
 * driven directly with a custom SourceTaskContext, without Kafka Connect or an EmbeddedEngine.
 */
class DebeziumTaskSpikeTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val enabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image =
    sys.env.getOrElse("PUDDING_TEST_MYSQL_IMAGE", "mysql:8.0")

  private var container: GenericContainer[_] = _

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
      } catch {
        case NonFatal(error) =>
          this.stopContainer()
          if (this.required) {
            throw new IllegalStateException(
              "MySQL container is required but the Debezium spike failed to start: " +
                error.getClass.getSimpleName,
              error)
          }
      }
    }
  }

  override def afterAll(): Unit = {
    this.stopContainer()
  }

  test("drive a Debezium MySQL task directly with a custom context") {
    assume(this.container != null, "MySQL container is not available")
    this.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(50))")
    this.execute("INSERT INTO orders VALUES (1, 'tom'), (2, 'jerry')")

    val props = Map(
      "name" -> "pudding-spike",
      "connector.class" -> "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname" -> this.container.getHost,
      "database.port" -> this.container.getMappedPort(3306).toString,
      "database.user" -> "root",
      "database.password" -> "root-secret",
      "database.server.id" -> "184054",
      "database.server.name" -> "pudding",
      "database.include.list" -> "inventory",
      "table.include.list" -> "inventory.orders",
      "database.history" -> "io.debezium.relational.history.MemoryDatabaseHistory",
      "snapshot.mode" -> "initial",
      "tombstones.on.delete" -> "false",
      "include.schema.changes" -> "false",
      "decimal.handling.mode" -> "string")
    val connector = new MySqlConnector()
    val records = new CopyOnWriteArrayList[SourceRecord]()
    var task: SourceTask = null
    try {
      connector.start(props.asJava)
      val taskClass = connector.taskClass()
      task = taskClass.getDeclaredConstructor().newInstance().asInstanceOf[SourceTask]
      task.initialize(new SpikeTaskContext(props))
      task.start(connector.taskConfigs(1).get(0))
      val poller = new Thread(new Runnable {
        override def run(): Unit = {
          while (!Thread.currentThread().isInterrupted) {
            try {
              val batch = task.poll()
              if (batch != null) {
                records.addAll(batch)
              }
            } catch {
              case _: InterruptedException => Thread.currentThread().interrupt()
              case NonFatal(_) =>
            }
          }
        }
      }, "debezium-spike-poller")
      poller.setDaemon(true)
      poller.start()

      this.waitFor(records, 2)
      this.execute("INSERT INTO orders VALUES (3, 'spark')")
      this.execute("UPDATE orders SET name = 'tom_v2' WHERE id = 1")
      this.execute("DELETE FROM orders WHERE id = 2")
      this.waitForOps(records, Set("c", "u", "d"))

      val ops = records.asScala.map(record =>
        record.value().asInstanceOf[org.apache.kafka.connect.data.Struct]
          .getString("op")).toSet
      ops should contain allOf ("r", "c", "u", "d")
      records.asScala.exists(record => record.sourceOffset() != null) shouldBe true
      poller.interrupt()
    } finally {
      if (task != null) {
        Try(task.stop())
      }
      Try(connector.stop())
    }
  }

  private def waitFor(records: java.util.List[SourceRecord], count: Int): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    while (records.size() < count && System.nanoTime() < deadline) {
      Thread.sleep(100)
    }
    records.size() should be >= count
  }

  private def waitForOps(records: java.util.List[SourceRecord], expected: Set[String]): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    def ops: Set[String] = records.asScala.map(record =>
      record.value().asInstanceOf[org.apache.kafka.connect.data.Struct]
        .getString("op")).toSet
    while (!expected.subsetOf(ops) && System.nanoTime() < deadline) {
      Thread.sleep(100)
    }
    expected.subsetOf(ops) shouldBe true
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

  private def jdbcUrl: String =
    s"jdbc:mysql://${this.container.getHost}:${this.container.getMappedPort(3306)}/inventory"

  private def stopContainer(): Unit = {
    if (this.container != null) {
      try this.container.stop()
      catch {
        case NonFatal(_) => ()
      }
      this.container = null
    }
  }

  private class SpikeTaskContext(values: Map[String, String]) extends SourceTaskContext {
    override def configs(): java.util.Map[String, String] = values.asJava

    override def offsetStorageReader(): OffsetStorageReader = new OffsetStorageReader {
      override def offset[T](partition: java.util.Map[String, T]): java.util.Map[String, Object] =
        null

      override def offsets[T](
          partitions: java.util.Collection[java.util.Map[String, T]]):
          java.util.Map[java.util.Map[String, T], java.util.Map[String, Object]] =
        util.Collections.emptyMap()
    }
  }
}
