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

package org.pudding.source

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.fs.{FileIO, Path => PaimonPath}
import org.apache.paimon.options.Options
import org.apache.paimon.table.FileStoreTableFactory
import org.apache.spark.sql.SparkSession
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
 * Tests for Paimon catalog reads.
 */
class PaimonSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private var warehouse: Path = _

  private val catalog = ResolvedCatalog(
    "paimon",
    "paimon",
    "paimon",
    sessionCatalog = false,
    sparkConf = Map.empty,
    properties = Map.empty)

  override def beforeAll(): Unit = {
    warehouse = Files.createTempDirectory("paimon-source-test")
    sparkSession = SparkSession.builder()
      .appName("test for paimon source")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", warehouse.toString)
      .getOrCreate()

    sparkSession.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02"),
      (3, "spark", "2026-01-01")
    ).toDF("id", "name", "dt")
      .write.format("paimon")
      .option("primary-key", "id,dt")
      .option("bucket", "1")
      .partitionBy("dt")
      .mode("append")
      .saveAsTable("paimon.default.orders")
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      Seq("orders", "orders_append", "orders_cdc").foreach { table =>
        Try(sparkSession.sql(s"DROP TABLE IF EXISTS paimon.default.$table"))
      }
      sparkSession.close()
    }
    if (warehouse != null) {
      val stream = Files.walk(warehouse)
      try {
        stream.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(path => Try(Files.deleteIfExists(path)))
      } finally {
        stream.close()
      }
    }
  }

  private def createAppendTable(): Unit = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02")
    ).toDF("id", "name", "dt")
      .write.format("paimon")
      .partitionBy("dt")
      .mode("append")
      .saveAsTable("paimon.default.orders_append")
  }

  private def createCdcTable(): Unit = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02"),
      (3, "spark", "2026-01-01")
    ).toDF("id", "name", "dt")
      .write.format("paimon")
      .option("primary-key", "id,dt")
      .option("bucket", "1")
      .partitionBy("dt")
      .mode("append")
      .saveAsTable("paimon.default.orders_cdc")
  }

  private def latestSnapshotId(table: String): Long = {
    val path = warehouse.resolve("default.db").resolve(table)
    val paimonPath = new PaimonPath(path.toString)
    val catalogContext = CatalogContext.create(
      new Options(), sparkSession.sessionState.newHadoopConf())
    val fileIO = FileIO.get(paimonPath, catalogContext)
    val paimonTable = FileStoreTableFactory.create(fileIO, paimonPath)
    paimonTable.latestSnapshot().get().id()
  }

  test("declare lakehouse incremental source capabilities") {
    val source = new PaimonSource()
    source.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Cdc)
  }

  test("read an append-only Paimon table incrementally") {
    createAppendTable()
    val begin = latestSnapshotId("orders_append")
    val session = sparkSession
    import session.implicits._
    Seq(
      (3, "new", "2026-01-03"),
      (4, "later", "2026-01-04")
    ).toDF("id", "name", "dt")
      .write.format("paimon")
      .partitionBy("dt")
      .mode("append")
      .saveAsTable("paimon.default.orders_append")

    val read = new PaimonSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map("table" -> "default.orders_append")), catalog = Some(catalog)),
      IncrementalRequest(
        semantics = IncrementalSemantics.Append,
        keyFields = Seq.empty,
        opColumn = "__op",
        start = Some(begin.toString),
        end = None,
        initial = "none"))

    read.dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(3, 4)
    read.fromCursor shouldBe Some(begin.toString)
    read.toCursor shouldBe latestSnapshotId("orders_append").toString
    read.orderFields shouldBe empty
  }

  test("read a full Paimon primary-key table as cdc inserts") {
    val read = new PaimonSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map("table" -> "default.orders")), catalog = Some(catalog)),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id", "dt"),
        opColumn = "op",
        start = None,
        end = None,
        initial = "full"))

    read.dataFrame.count() shouldBe 3
    read.dataFrame.select("op").collect().map(_.getString(0)).toSet shouldBe Set("I")
    read.fromCursor shouldBe None
  }

  test("read Paimon changes as cdc with deletes") {
    createCdcTable()
    val begin = latestSnapshotId("orders_cdc")
    val session = sparkSession
    import session.implicits._
    Seq((1, "tom_v2", "2026-01-01")).toDF("id", "name", "dt")
      .write.format("paimon")
      .option("primary-key", "id,dt")
      .option("bucket", "1")
      .partitionBy("dt")
      .mode("append")
      .saveAsTable("paimon.default.orders_cdc")
    session.sql("DELETE FROM paimon.default.orders_cdc WHERE id = 2")

    val read = new PaimonSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map("table" -> "default.orders_cdc")), catalog = Some(catalog)),
      IncrementalRequest(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id", "dt"),
        opColumn = "op",
        start = Some(begin.toString),
        end = None,
        initial = "none"))

    val changes = read.dataFrame.select("id", "name", "op").collect()
      .map(row => (row.getInt(0), row.getString(1), row.getString(2))).toSet
    // Paimon's diff scan represents an updated row as an insert for its primary key; downstream
    // upsert sinks apply it idempotently.
    changes shouldBe Set((1, "tom_v2", "I"), (2, "jerry", "D"))
    read.dataFrame.columns.contains("_row_kind_") shouldBe false
    read.toCursor shouldBe latestSnapshotId("orders_cdc").toString
  }

  test("reject append semantics for a primary-key table") {
    val error = intercept[PuddingException] {
      new PaimonSource().readIncremental(
        sparkSession,
        SourceContext(Some(Map("table" -> "default.orders")), catalog = Some(catalog)),
        IncrementalRequest(
          semantics = IncrementalSemantics.Append,
          keyFields = Seq.empty,
          opColumn = "__op",
          start = Some("1"),
          end = None,
          initial = "none"))
    }
    error.getMessage should include ("append-only table")
  }

  test("reject cdc semantics for an append-only table without primary keys") {
    createAppendTable()
    val error = intercept[PuddingException] {
      new PaimonSource().readIncremental(
        sparkSession,
        SourceContext(Some(Map("table" -> "default.orders_append")), catalog = Some(catalog)),
        IncrementalRequest(
          semantics = IncrementalSemantics.Cdc,
          keyFields = Seq("id"),
          opColumn = "op",
          start = Some("1"),
          end = None,
          initial = "none"))
    }
    error.getMessage should include ("primary-key table")
  }

  test("read a Paimon table through a catalog") {
    val dataFrame = new PaimonSource().initData(
      sparkSession,
      SourceContext(
        Some(Map(
          "table" -> "default.orders",
          "scanMode" -> "latest-full",
          "filterExpr" -> "dt = '2026-01-01'",
          "numPartitions" -> 2)),
        catalog = Some(catalog)))

    dataFrame.count() shouldBe 2
    dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(1, 3)
    dataFrame.rdd.getNumPartitions shouldBe 2
  }

  test("create a streaming Paimon dataframe") {
    val dataFrame = new PaimonSource().initData(
      sparkSession,
      SourceContext(
        Some(Map(
          "table" -> "default.orders",
          "streamRead" -> true,
          "scanMode" -> "latest-full")),
        catalog = Some(catalog)))

    dataFrame.isStreaming shouldBe true
  }

  test("require a table or path") {
    val error = intercept[PuddingException] {
      new PaimonSource().initData(
        sparkSession,
        SourceContext(Some(Map.empty), catalog = Some(catalog)))
    }
    error.getMessage should include ("table or path")
  }

  test("reject a non-Paimon catalog") {
    val otherCatalog = catalog.copy(`type` = "iceberg")
    val error = intercept[PuddingException] {
      new PaimonSource().initData(
        sparkSession,
        SourceContext(
          Some(Map("table" -> "default.orders")),
          catalog = Some(otherCatalog)))
    }
    error.getMessage should include ("requires a paimon catalog")
  }
}
