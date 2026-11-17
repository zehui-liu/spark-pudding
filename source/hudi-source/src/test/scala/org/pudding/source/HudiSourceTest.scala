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

import java.net.URI
import java.nio.file.{Files, Paths, Path => FilePath}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.definition.{
  IncrementalRequest,
  IncrementalSemantics,
  SourceContext
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Hudi path and catalog-aware source configuration.
 */
class HudiSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private var workspace: FilePath = _

  override def beforeAll(): Unit = {
    workspace = Files.createTempDirectory("hudi-source-test")
    sparkSession = SparkSession.builder()
      .appName("test for hudi source")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
    }
    if (workspace != null) {
      val stream = Files.walk(workspace)
      try {
        stream.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(path => Try(Files.deleteIfExists(path)))
      } finally {
        stream.close()
      }
    }
  }

  private def orders(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "2026-01-01", 1L),
      (2, "jerry", "2026-01-02", 1L),
      (3, "spark", "2026-01-01", 1L)
    ).toDF("id", "name", "dt", "ts")
  }

  private def writeTable(name: String, cdcEnabled: Boolean = false): FilePath = {
    val path = workspace.resolve(name)
    orders().write.format("org.apache.hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "dt")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.hive_style_partitioning", "false")
      .option("hoodie.table.cdc.enabled", cdcEnabled.toString)
      .option("hoodie.table.cdc.supplemental.logging.mode", "DATA_BEFORE_AFTER")
      .mode("overwrite")
      .save(path.toString)
    path
  }

  private def appendTable(
      name: String,
      path: FilePath,
      dataFrame: DataFrame,
      operation: String = "upsert"): Unit = {
    // Hudi instants have millisecond resolution. Keep consecutive commits distinct.
    Thread.sleep(20)
    dataFrame.write.format("org.apache.hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", operation)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "dt")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.hive_style_partitioning", "false")
      .mode("append")
      .save(path.toString)
  }

  private def typedSeed(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "p", java.sql.Date.valueOf("2026-01-01"),
        java.sql.Timestamp.valueOf("2026-01-01 10:00:00"),
        new java.math.BigDecimal("12.34"), Array[Byte](1, 2, 3, -1), 1L)
    ).toDF("id", "part", "d", "ts", "amount", "bin", "seq")
  }

  private def writeTypedTable(name: String): FilePath = {
    val path = workspace.resolve(name)
    typedSeed().write.format("org.apache.hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "part")
      .option("hoodie.datasource.write.precombine.field", "seq")
      .option("hoodie.datasource.write.hive_style_partitioning", "false")
      .option("hoodie.table.cdc.enabled", "true")
      .option("hoodie.table.cdc.supplemental.logging.mode", "DATA_BEFORE_AFTER")
      .mode("overwrite")
      .save(path.toString)
    path
  }

  private def appendTypedTable(name: String, path: FilePath, dataFrame: DataFrame): Unit = {
    Thread.sleep(20)
    dataFrame.write.format("org.apache.hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "part")
      .option("hoodie.datasource.write.precombine.field", "seq")
      .option("hoodie.datasource.write.hive_style_partitioning", "false")
      .mode("append")
      .save(path.toString)
  }

  private def appendBulkInsert(
      name: String,
      path: FilePath,
      dataFrame: DataFrame,
      partitionField: String,
      precombineField: String): Unit = {
    Thread.sleep(20)
    dataFrame.write.format("org.apache.hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", partitionField)
      .option("hoodie.datasource.write.precombine.field", precombineField)
      .option("hoodie.datasource.write.hive_style_partitioning", "false")
      .mode("append")
      .save(path.toString)
  }

  private def writeBinaryTable(name: String): FilePath = {
    val path = workspace.resolve(name)
    val session = sparkSession
    import session.implicits._
    Seq((1, "seed", "2026-01-01", 1L, Array[Byte](1, 2, 3)))
      .toDF("id", "name", "dt", "ts", "bin")
      .write.format("org.apache.hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "dt")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.hive_style_partitioning", "false")
      .option("hoodie.table.cdc.enabled", "true")
      .option("hoodie.table.cdc.supplemental.logging.mode", "DATA_BEFORE_AFTER")
      .mode("overwrite")
      .save(path.toString)
    path
  }

  private def rootMessages(error: Throwable): String = {
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null)
      .map(cause => Option(cause.getMessage).getOrElse(""))
      .mkString(" | ")
  }

  private def latestCommit(path: FilePath): String = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(sparkSession.sessionState.newHadoopConf())
      .setBasePath(path.toString)
      .build()
    metaClient.getCommitsTimeline.filterCompletedInstants.lastInstant().get().getTimestamp
  }

  test("declare lakehouse incremental source capabilities") {
    val source = new HudiSource()
    source.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)
  }

  test("read a Hudi table by path") {
    val path = writeTable("orders_path")
    val dataFrame = new HudiSource().initData(
      sparkSession,
      Some(Map(
        "table" -> "orders_path",
        "tablePath" -> path.toString,
        "filterExpr" -> "id >= 2",
        "numPartitions" -> 2)))

    dataFrame.count() shouldBe 2
    dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2, 3)
    dataFrame.rdd.getNumPartitions shouldBe 2
  }

  test("read optimized and force convert schema") {
    val path = writeTable("orders_schema")
    val dataFrame = new HudiSource().initData(
      sparkSession,
      Some(Map(
        "table" -> "orders_schema",
        "tablePath" -> path.toString,
        "queryType" -> "read_optimized",
        "forceConvertSchema" -> true,
        "schema" -> Map("id" -> "string", "name" -> "string"))))

    dataFrame.schema.fieldNames.toSeq shouldBe Seq("id", "name")
    dataFrame.select("id").collect().map(_.getString(0)).toSet shouldBe Set("1", "2", "3")
  }

  test("incremental read requires begin and end instants") {
    val path = writeTable("orders_incremental")
    val error = intercept[PuddingException] {
      new HudiSource().initData(
        sparkSession,
        Some(Map(
          "table" -> "orders_incremental",
          "tablePath" -> path.toString,
          "queryType" -> "incremental",
          "beginTime" -> "20260101000000")))
    }
    error.getMessage should include ("endTime")
  }

  test("read only changed latest state for upsert incremental semantics") {
    val path = writeTable("orders_incremental_upsert")
    val begin = latestCommit(path)
    val session = sparkSession
    import session.implicits._
    appendTable("orders_incremental_upsert", path,
      Seq(
        (1, "tom_v2", "2026-01-01", 2L),
        (4, "new", "2026-01-03", 1L)
      ).toDF("id", "name", "dt", "ts"))

    val read = new HudiSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map(
        "table" -> "orders_incremental_upsert",
        "tablePath" -> path.toString))),
      IncrementalRequest(
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id"),
        opColumn = "__op",
        start = Some(begin),
        end = None,
        initial = "none"))

    read.dataFrame.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((1, "tom_v2"), (4, "new"))
    read.dataFrame.columns.exists(_.startsWith("_hoodie_")) shouldBe false
    read.fromCursor shouldBe Some(begin)
    read.toCursor shouldBe latestCommit(path)
    read.orderFields shouldBe empty
  }

  test("read a full snapshot as cdc inserts") {
    val path = writeTable("orders_incremental_full_cdc")
    val read = new HudiSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map(
        "table" -> "orders_incremental_full_cdc",
        "tablePath" -> path.toString))),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        start = None,
        end = None,
        initial = "full"))

    read.dataFrame.count() shouldBe 3
    read.dataFrame.select("op").collect().map(_.getString(0)).toSet shouldBe Set("I")
    read.dataFrame.columns.exists(_.startsWith("_hoodie_")) shouldBe false
    read.fromCursor shouldBe None
  }

  test("read changed rows as cdc with operations and commit ordering") {
    val path = writeTable("orders_incremental_cdc", cdcEnabled = true)
    val begin = latestCommit(path)
    val session = sparkSession
    import session.implicits._
    appendTable("orders_incremental_cdc", path,
      Seq((1, "tom_v3", "2026-01-01", 3L)).toDF("id", "name", "dt", "ts"))
    appendTable("orders_incremental_cdc", path,
      Seq((2, "jerry", "2026-01-02", 2L)).toDF("id", "name", "dt", "ts"),
      operation = "delete")

    val read = new HudiSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map(
        "table" -> "orders_incremental_cdc",
        "tablePath" -> path.toString))),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        start = Some(begin),
        end = None,
        initial = "none"))

    val changes = read.dataFrame.select("id", "name", "op").collect()
      .map(row => (row.getInt(0), row.getString(1), row.getString(2))).toSet
    changes shouldBe Set((1, "tom_v3", "U"), (2, "jerry", "D"))
    read.dataFrame.columns.exists(_.startsWith("_hoodie_")) shouldBe false
    read.orderFields shouldBe Seq("__pudding_cdc_commit_time")
  }

  test("decode cdc before and after images with logical date, timestamp, decimal and binary") {
    val path = writeTypedTable("orders_incremental_cdc_types")
    val begin = latestCommit(path)
    val session = sparkSession
    import session.implicits._
    appendTypedTable(
      "orders_incremental_cdc_types",
      path,
      Seq(
        (2, "p", java.sql.Date.valueOf("2026-01-02"),
          java.sql.Timestamp.valueOf("2026-01-02 11:00:00"),
          new java.math.BigDecimal("56.78"), Array[Byte](9, 8, 7), 2L)
      ).toDF("id", "part", "d", "ts", "amount", "bin", "seq"))
    appendTypedTable(
      "orders_incremental_cdc_types",
      path,
      Seq(
        (1, "p", java.sql.Date.valueOf("2026-01-05"),
          java.sql.Timestamp.valueOf("2026-01-05 12:00:00"),
          new java.math.BigDecimal("99.99"), Array[Byte](4, 5, 6), 3L)
      ).toDF("id", "part", "d", "ts", "amount", "bin", "seq"))

    val read = new HudiSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map(
        "table" -> "orders_incremental_cdc_types",
        "tablePath" -> path.toString))),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        start = Some(begin),
        end = None,
        initial = "none"))

    val rows = read.dataFrame.select("id", "d", "ts", "amount", "bin", "op").collect()
      .map(row => row.getInt(0) -> row).toMap
    rows.keySet shouldBe Set(1, 2)

    val inserted = rows(2)
    inserted.getString(5) shouldBe "I"
    inserted.getDate(1).toString shouldBe "2026-01-02"
    inserted.getTimestamp(2).getTime shouldBe
      java.sql.Timestamp.valueOf("2026-01-02 11:00:00").getTime
    inserted.getDecimal(3).compareTo(
      new java.math.BigDecimal("56.78")) shouldBe 0
    inserted.getAs[Array[Byte]](4).toSeq shouldBe Seq[Byte](9, 8, 7)

    val updated = rows(1)
    updated.getString(5) shouldBe "U"
    updated.getDate(1).toString shouldBe "2026-01-05"
    updated.getTimestamp(2).getTime shouldBe
      java.sql.Timestamp.valueOf("2026-01-05 12:00:00").getTime
    updated.getDecimal(3).compareTo(
      new java.math.BigDecimal("99.99")) shouldBe 0
    updated.getAs[Array[Byte]](4).toSeq shouldBe Seq[Byte](4, 5, 6)
  }

  test("decode a bulk-insert base-file cdc image with binary data") {
    val path = writeBinaryTable("orders_cdc_bulk_binary")
    val begin = latestCommit(path)
    val session = sparkSession
    import session.implicits._
    appendBulkInsert(
      "orders_cdc_bulk_binary",
      path,
      Seq((2, "bulk", "2026-01-02", 2L, Array[Byte](9, 8, 7)))
        .toDF("id", "name", "dt", "ts", "bin"),
      partitionField = "dt",
      precombineField = "ts")

    val read = new HudiSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map(
        "table" -> "orders_cdc_bulk_binary",
        "tablePath" -> path.toString))),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        start = Some(begin),
        end = None,
        initial = "none"))

    val row = read.dataFrame.where("id = 2").select("bin", "op").head()
    row.getString(1) shouldBe "I"
    row.getAs[Array[Byte]](0).toSeq shouldBe Seq[Byte](9, 8, 7)
  }

  test("reject a bulk-insert base-file cdc image with decimal data") {
    val path = writeTypedTable("orders_cdc_bulk_decimal")
    val begin = latestCommit(path)
    val session = sparkSession
    import session.implicits._
    appendBulkInsert(
      "orders_cdc_bulk_decimal",
      path,
      Seq(
        (2, "p", java.sql.Date.valueOf("2026-01-02"),
          java.sql.Timestamp.valueOf("2026-01-02 11:00:00"),
          new java.math.BigDecimal("56.78"), Array[Byte](9, 8, 7), 2L)
      ).toDF("id", "part", "d", "ts", "amount", "bin", "seq"),
      partitionField = "part",
      precombineField = "seq")

    val read = new HudiSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map(
        "table" -> "orders_cdc_bulk_decimal",
        "tablePath" -> path.toString))),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        start = Some(begin),
        end = None,
        initial = "none"))

    val error = intercept[Exception] {
      read.dataFrame.select("id", "amount").collect()
    }
    rootMessages(error) should include ("amount")
    rootMessages(error) should include ("DECIMAL")
  }

  test("reject cdc reads on a table without cdc enabled") {
    val path = writeTable("orders_incremental_cdc_disabled")
    val begin = latestCommit(path)
    val session = sparkSession
    import session.implicits._
    appendTable("orders_incremental_cdc_disabled", path,
      Seq((1, "tom_v2", "2026-01-01", 2L)).toDF("id", "name", "dt", "ts"))

    val error = intercept[PuddingException] {
      new HudiSource().readIncremental(
        sparkSession,
        SourceContext(Some(Map(
          "table" -> "orders_incremental_cdc_disabled",
          "tablePath" -> path.toString))),
        IncrementalRequest(
          semantics = IncrementalSemantics.Cdc,
          keyFields = Seq("id"),
          opColumn = "op",
          start = Some(begin),
          end = None,
          initial = "none"))
    }
    error.getMessage should include ("hoodie.table.cdc.enabled")
  }

  test("require a table path when no catalog is configured") {
    val error = intercept[PuddingException] {
      new HudiSource().initData(sparkSession, Some(Map("table" -> "orders")))
    }
    error.getMessage should include ("tablePath is required")
  }

  test("reject a non-Hudi catalog") {
    val catalog = ResolvedCatalog(
      "lake",
      "iceberg",
      "lake",
      sessionCatalog = false,
      sparkConf = Map.empty,
      properties = Map.empty)
    val error = intercept[PuddingException] {
      new HudiSource().initData(
        sparkSession,
        SourceContext(
          Some(Map("table" -> "orders", "tablePath" -> workspace.resolve("orders").toString)),
          catalog = Some(catalog)))
    }
    error.getMessage should include ("requires a hudi catalog")
  }

  test("reject an unsupported query type") {
    val path = writeTable("orders_query_type")
    val error = intercept[PuddingException] {
      new HudiSource().initData(
        sparkSession,
        Some(Map(
          "table" -> "orders_query_type",
          "tablePath" -> path.toString,
          "queryType" -> "realtime")))
    }
    error.getMessage should include ("unsupported queryType")
  }

  test("read a Hudi table through a catalog") {
    withCatalogSession { (session, tableName) =>
      val current = session
      import current.implicits._
      Seq(
        (1, "tom", "2026-01-01", 1L),
        (2, "jerry", "2026-01-02", 1L)
      ).toDF("id", "name", "dt", "ts")
        .write.format("org.apache.hudi")
        .option("hoodie.table.name", tableName)
        .option("primaryKey", "id")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("preCombineField", "ts")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "dt")
        .option("type", "cow")
        .partitionBy("dt")
        .mode("overwrite")
        .saveAsTable(s"default.$tableName")

      val catalog = ResolvedCatalog(
        "hudi_prod",
        "hudi",
        "spark_catalog",
        sessionCatalog = true,
        sparkConf = Map.empty,
        properties = Map.empty)
      val dataFrame = new HudiSource().initData(
        session,
        SourceContext(
          Some(Map(
            "table" -> s"default.$tableName",
            "filterExpr" -> "id >= 2")),
          catalog = Some(catalog)))

      dataFrame.count() shouldBe 1
      dataFrame.select("id").collect().head.getInt(0) shouldBe 2

      val location = session.sql(s"DESCRIBE EXTENDED default.$tableName")
        .where("col_name = 'Location'")
        .select("data_type")
        .head()
        .getString(0)
      val path = Paths.get(URI.create(location))
      val begin = this.latestCommit(path)
      this.appendTable(
        tableName,
        path,
        Seq(
          (1, "tom_v2", "2026-01-01", 2L),
          (4, "new", "2026-01-03", 2L)
        ).toDF("id", "name", "dt", "ts"))

      val incremental = new HudiSource().readIncremental(
        session,
        SourceContext(
          Some(Map("table" -> s"default.$tableName")),
          catalog = Some(catalog)),
        IncrementalRequest(
          semantics = IncrementalSemantics.Upsert,
          keyFields = Seq("id"),
          opColumn = "__op",
          start = Some(begin),
          end = None,
          initial = "none"))

      incremental.dataFrame.select("id", "name").collect()
        .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
        Set((1, "tom_v2"), (4, "new"))
      incremental.fromCursor shouldBe Some(begin)
    }
  }

  private def withCatalogSession(testBody: (SparkSession, String) => Unit): Unit = {
    val warehouse = Files.createTempDirectory("hudi-source-catalog-warehouse")
    val metastore = Files.createTempDirectory("hudi-source-catalog-metastore")
    val tableName = s"hudi_catalog_orders_${System.nanoTime()}"
    var session: SparkSession = null
    try {
      session = try {
        SparkSession.builder()
          .appName("test for hudi catalog source")
          .master("local[2]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
          .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
          .config("spark.sql.warehouse.dir", warehouse.toString)
          .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            s"jdbc:derby:;databaseName=$metastore/metastore_db;create=true")
          .enableHiveSupport()
          .getOrCreate()
      } catch {
        case error: Throwable =>
          cancel(s"local Hudi catalog is not available: ${error.getMessage}")
      }
      if (session != null) {
        testBody(session, tableName)
      }
    } finally {
      if (session != null) {
        Try {
          val location = session.sql(s"DESCRIBE EXTENDED default.$tableName")
            .where("col_name = 'Location'")
            .select("data_type")
            .head()
            .getString(0)
          session.sql(s"DROP TABLE IF EXISTS default.$tableName")
          val fileSystem = FileSystem.get(URI.create(location), session.sparkContext.hadoopConfiguration)
          fileSystem.delete(new Path(location), true)
        }
        session.close()
      }
      Seq(warehouse, metastore).foreach { root =>
        val stream = Files.walk(root)
        try {
          stream.iterator().asScala.toVector
            .sortBy(_.getNameCount)(Ordering.Int.reverse)
            .foreach(path => Try(Files.deleteIfExists(path)))
        } finally {
          stream.close()
        }
      }
    }
  }
}
