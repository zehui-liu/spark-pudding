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

package org.pudding.integration

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSink, SinkContext, SourceContext}
import org.pudding.sink.{HudiSink, PaimonSink}
import org.pudding.source.cdc.MysqlCdcSource

/**
 * End-to-end native CDC -> lakehouse regression.
 *
 * A real MySQL binlog stream feeds each supported lakehouse sink. The tests cover snapshot,
 * insert/update/delete, checkpoint restart and the sink's own checkpoint semantics. They are
 * opt-in because they start a MySQL container and three lakehouse runtimes in one Spark session.
 */
class DatabaseCdcHudiPaimonTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val enabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image =
    sys.env.getOrElse("PUDDING_TEST_MYSQL_IMAGE", "mysql:8.0")

  private var container: GenericContainer[_] = _

  private var spark: SparkSession = _

  private var workspace: Path = _

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
        this.createSourceTables()

        this.workspace = Files.createTempDirectory("cdc-lakehouse-test")
        this.spark = SparkSession.builder()
          .appName("database-cdc-hudi-paimon-test")
          .master("local[2]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
          .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
          .config(
            "spark.sql.catalog.paimon.warehouse",
            this.workspace.resolve("paimon-warehouse").toString)
          .getOrCreate()
      } catch {
        case NonFatal(error) =>
          this.closeAll()
          if (this.required) {
            throw new IllegalStateException(
              "MySQL/lakehouse CDC integration environment is required but unavailable: " +
                error.getClass.getSimpleName,
              error)
          }
      }
    }
  }

  override def afterAll(): Unit = {
    this.closeAll()
  }

  test("mysql CDC streams snapshot, upserts and deletes into Hudi") {
    assume(this.container != null && this.spark != null, "CDC integration environment not available")
    val path = this.workspace.resolve("hudi_orders")
    val checkpoint = this.workspace.resolve("hudi_checkpoint")
    val sourceTable = "orders_hudi"
    val sink = new HudiSink()

    // Seed a stale table so the snapshot path is observable, not just the change path.
    sink.writer(
      this.staleRows(),
      Some(this.hudiBaseConfig(path, "insert", "overwrite")))

    this.runCdcScenario(
      sourceTable, sink, this.hudiSinkContext(path, checkpoint), this.hudiRows(path))
  }

  test("mysql CDC streams snapshot, upserts and deletes into Paimon") {
    assume(this.container != null && this.spark != null, "CDC integration environment not available")
    val table = "cdc_paimon_orders"
    val path = this.createPaimonTable(table)
    val checkpoint = this.workspace.resolve("paimon_checkpoint")
    val sourceTable = "orders_paimon"
    val sink = new PaimonSink()

    this.runCdcScenario(
      sourceTable, sink, this.paimonSinkContext(path, checkpoint), this.paimonRows(path))
  }

  /**
   * Run one CDC query, verify snapshot and DML, stop it, restart from the same sink checkpoint
   * and verify that a row inserted while stopped is consumed.
   */
  private def runCdcScenario(
      sourceTable: String,
      sink: DataSink,
      context: SinkContext,
      read: => Set[(Int, String)]): Unit = {
    val (first, firstFailure) = this.startSink(this.sourceFrame(sourceTable), sink, context)
    try {
      this.waitFor(firstFailure, "initial snapshot") {
        read == Set(1 -> "tom", 2 -> "jerry")
      }
      this.execute(
        s"UPDATE $sourceTable SET name = 'tom_v2', ts = 10 WHERE id = 1")
      this.execute(s"DELETE FROM $sourceTable WHERE id = 2")
      this.execute(
        s"INSERT INTO $sourceTable VALUES (3, 'spark', '2026-01-03', 3)")
      this.waitFor(firstFailure, "CDC updates") {
        read == Set(1 -> "tom_v2", 3 -> "spark")
      }
    } finally {
      this.stopSink(first)
    }
    Option(firstFailure.get).foreach(throw _)

    this.execute(
      s"INSERT INTO $sourceTable VALUES (4, 'restart', '2026-01-04', 4)")
    val (second, secondFailure) =
      this.startSink(this.sourceFrame(sourceTable), sink, context)
    try {
      this.waitFor(secondFailure, "row after checkpoint restart") {
        read == Set(1 -> "tom_v2", 3 -> "spark", 4 -> "restart")
      }
    } finally {
      this.stopSink(second)
    }
    Option(secondFailure.get).foreach(throw _)
  }

  private def startSink(
      frame: DataFrame,
      sink: DataSink,
      context: SinkContext): (Thread, AtomicReference[Throwable]) = {
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        try sink.writer(frame, context)
        catch {
          case NonFatal(error) => failure.compareAndSet(null, error)
        }
      }
    }, s"cdc-lakehouse-${sink.identifier}")
    thread.setDaemon(true)
    thread.start()
    (thread, failure)
  }

  private def stopSink(thread: Thread): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120)
    while (this.spark.streams.active.nonEmpty && System.nanoTime() < deadline) {
      this.spark.streams.active.foreach { query =>
        try query.stop()
        catch {
          case NonFatal(_) => ()
        }
      }
      Thread.sleep(100L)
    }
    thread.join(TimeUnit.SECONDS.toMillis(120L))
    if (thread.isAlive) {
      thread.interrupt()
      fail("CDC lakehouse writer did not stop")
    }
  }

  private def waitFor(
      failure: AtomicReference[Throwable],
      description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120L)
    while (!condition && System.nanoTime() < deadline) {
      Option(failure.get) match {
        case Some(error) => throw error
        case None => ()
      }
      Thread.sleep(250L)
    }
    Option(failure.get) match {
      case Some(error) => throw error
      case None => ()
    }
    condition shouldBe true
  }

  private def sourceFrame(table: String): DataFrame = {
    val config = Map[String, Any](
      "host" -> this.container.getHost,
      "port" -> this.container.getMappedPort(3306),
      "database" -> "inventory",
      "table" -> table,
      "user" -> "root",
      "password" -> "root-secret",
      "schema" -> "id int, name string, dt string, ts long",
      "maxRecordsPerBatch" -> 2)
    new MysqlCdcSource().initData(
      this.spark,
      SourceContext(Some(config), executionMode = Some(ExecutionMode.Streaming)))
  }

  private def hudiBaseConfig(
      path: Path,
      operation: String,
      mode: String): Map[String, Any] = {
    Map(
      "table" -> path.getFileName.toString,
      "tablePath" -> path.toString,
      "operation" -> operation,
      "tableType" -> "COPY_ON_WRITE",
      "partitionPathField" -> "dt",
      "preCombineField" -> "ts",
      "recordKeyFields" -> Seq("id"),
      "mode" -> mode)
  }

  private def hudiSinkContext(path: Path, checkpoint: Path): SinkContext = {
    SinkContext(
      Some(this.hudiBaseConfig(path, "upsert", "append") ++ Map(
        "checkpointLocation" -> checkpoint.toString,
        "outputMode" -> "append",
        "cdcOpColumn" -> "__op",
        "cdcOrderFields" -> Seq("_cdc_timestamp", "_cdc_sequence"))),
      executionMode = Some(ExecutionMode.Streaming),
      trigger = Some("processing-time: 1 second"))
  }

  private def paimonSinkContext(path: String, checkpoint: Path): SinkContext = {
    SinkContext(
      Some(Map(
        "path" -> path,
        "primaryKey" -> Seq("id"),
        "checkpointLocation" -> checkpoint.toString,
        "outputMode" -> "append",
        "cdcOpColumn" -> "__op",
        "cdcOrderFields" -> Seq("_cdc_timestamp", "_cdc_sequence"))),
      executionMode = Some(ExecutionMode.Streaming),
      trigger = Some("processing-time: 1 second"))
  }

  private def staleRows(): DataFrame = {
    val spark = this.spark
    import spark.implicits._
    Seq(
      (1, "stale_tom", "2026-01-01", 0L),
      (2, "stale_jerry", "2026-01-02", 0L)).toDF("id", "name", "dt", "ts")
  }

  private def hudiRows(path: Path): Set[(Int, String)] = {
    this.spark.read.format("org.apache.hudi").load(path.toString)
      .select("id", "name").collect()
      .map(row => row.getInt(0) -> row.getString(1)).toSet
  }

  private def paimonRows(path: String): Set[(Int, String)] = {
    this.spark.read.format("paimon").load(path)
      .select("id", "name").collect()
      .map(row => row.getInt(0) -> row.getString(1)).toSet
  }

  private def createPaimonTable(name: String): String = {
    this.spark.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
    this.spark.sql(s"DROP TABLE IF EXISTS paimon.default.$name")
    this.spark.sql(
      s"""CREATE TABLE paimon.default.$name (
         |  id INT,
         |  name STRING,
         |  dt STRING,
         |  ts BIGINT
         |) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
    this.spark.sql(s"DESCRIBE EXTENDED paimon.default.$name")
      .where("upper(col_name) = 'LOCATION'")
      .select("data_type")
      .head()
      .getString(0)
  }

  private def createSourceTables(): Unit = {
    Seq("orders_hudi", "orders_paimon").foreach { table =>
      this.execute(
        s"CREATE TABLE $table (id INT PRIMARY KEY, name VARCHAR(50), dt VARCHAR(20), ts BIGINT)")
      this.execute(
        s"INSERT INTO $table VALUES (1, 'tom', '2026-01-01', 1), " +
          "(2, 'jerry', '2026-01-02', 2)")
    }
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
    if (this.workspace != null && Files.exists(this.workspace)) {
      try {
        val stream = Files.walk(this.workspace)
        try {
          stream.iterator().asScala.toVector
            .sortBy(_.getNameCount)(Ordering.Int.reverse)
            .foreach(path => Try(Files.deleteIfExists(path)))
        } finally {
          stream.close()
        }
      } catch {
        case NonFatal(_) => ()
      }
      this.workspace = null
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
