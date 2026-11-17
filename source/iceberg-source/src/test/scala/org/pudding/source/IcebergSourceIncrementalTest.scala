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

import java.nio.file.{Files, Path => FilePath}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{
  IncrementalRequest,
  IncrementalSemantics,
  SourceContext
}
import org.pudding.core.incremental.IncrementalFrames
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Iceberg snapshot append and changelog incremental reads.
 */
class IcebergSourceIncrementalTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private var workspace: FilePath = _

  private val catalog = ResolvedCatalog(
    "iceberg",
    "iceberg",
    "local",
    sessionCatalog = false,
    sparkConf = Map.empty,
    properties = Map.empty)

  override def beforeAll(): Unit = {
    workspace = Files.createTempDirectory("iceberg-source-incremental")
    sparkSession = SparkSession.builder()
      .appName("test for iceberg incremental source")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", workspace.resolve("catalog").toString)
      .getOrCreate()

    sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS local.default")
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
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02")
    ).toDF("id", "name", "dt")
  }

  private def tableName(name: String): String = s"default.$name"

  private def fullTableName(name: String): String = s"local.default.$name"

  private def createTable(name: String): Unit = {
    sparkSession.sql(s"DROP TABLE IF EXISTS ${fullTableName(name)}")
    orders().write.format("iceberg")
      .mode("overwrite")
      .saveAsTable(fullTableName(name))
  }

  private def appendRows(name: String, rows: DataFrame): Unit = {
    rows.write.format("iceberg")
      .mode("append")
      .saveAsTable(fullTableName(name))
  }

  private def latestSnapshot(name: String): Long = {
    sparkSession.sql(
      s"SELECT snapshot_id FROM ${fullTableName(name)}.snapshots " +
        "ORDER BY committed_at DESC LIMIT 1")
      .head().getLong(0)
  }

  private def tableLocation(name: String): String = {
    sparkSession.sql(s"DESCRIBE EXTENDED ${fullTableName(name)}")
      .where("col_name = 'Location'")
      .select("data_type")
      .head().getString(0)
  }

  private def catalogSource(name: String): SourceContext = {
    SourceContext(
      Some(Map("table" -> tableName(name))),
      catalog = Some(catalog))
  }

  private def request(
      semantics: IncrementalSemantics,
      start: Option[String],
      initial: String = "none"): IncrementalRequest = {
    IncrementalRequest(
      semantics = semantics,
      keyFields = if (semantics == IncrementalSemantics.Append) Seq.empty else Seq("id"),
      opColumn = "op",
      start = start,
      end = None,
      initial = initial)
  }

  test("declare batch, streaming and all incremental source capabilities") {
    val source = new IcebergSource()
    source.supportedExecutionModes shouldBe Set(ExecutionMode.Batch, ExecutionMode.Streaming)
    source.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)
  }

  test("read appended snapshots through a path") {
    val name = "orders_append"
    createTable(name)
    val begin = latestSnapshot(name)
    val session = sparkSession
    import session.implicits._
    appendRows(name, Seq(
      (3, "spark", "2026-01-03"),
      (4, "new", "2026-01-04")
    ).toDF("id", "name", "dt"))

    val read = new IcebergSource().readIncremental(
      sparkSession,
      SourceContext(Some(Map("path" -> tableLocation(name)))),
      request(IncrementalSemantics.Append, Some(begin.toString)))

    read.dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(3, 4)
    read.fromCursor shouldBe Some(begin.toString)
    read.toCursor shouldBe latestSnapshot(name).toString
    read.orderFields shouldBe empty
  }

  test("read a full snapshot as cdc inserts") {
    val name = "orders_full"
    createTable(name)

    val read = new IcebergSource().readIncremental(
      sparkSession,
      catalogSource(name),
      request(IncrementalSemantics.Cdc, None, "full"))

    read.dataFrame.count() shouldBe 2
    read.dataFrame.select("op").collect().map(_.getString(0)).toSet shouldBe Set("I")
  }

  test("reject append semantics across an overwrite snapshot") {
    val name = "orders_overwrite"
    createTable(name)
    val begin = latestSnapshot(name)
    sparkSession.sql(s"DELETE FROM ${fullTableName(name)} WHERE id >= 0")

    val error = intercept[PuddingException] {
      new IcebergSource().readIncremental(
        sparkSession,
        catalogSource(name),
        request(IncrementalSemantics.Append, Some(begin.toString)))
    }
    error.getMessage should include ("non-append snapshots")
    error.getMessage should include ("use cdc semantics instead")
  }

  test("read catalog changelog changes as cdc") {
    val name = "orders_cdc"
    createTable(name)
    val begin = latestSnapshot(name)
    sparkSession.sql(s"UPDATE ${fullTableName(name)} SET name = 'tom_v2' WHERE id = 1")
    sparkSession.sql(s"DELETE FROM ${fullTableName(name)} WHERE id = 2")

    val read = new IcebergSource().readIncremental(
      sparkSession,
      catalogSource(name),
      request(IncrementalSemantics.Cdc, Some(begin.toString)))

    // Iceberg can encode an update as a delete/insert pair; sinks deduplicate by key using the
    // provided change ordering and keep the insert.
    val changes = read.dataFrame.select("id", "name", "op").collect()
      .map(row => (row.getInt(0), row.getString(1), row.getString(2))).toSet
    changes shouldBe Set((1, "tom", "D"), (1, "tom_v2", "I"), (2, "jerry", "D"))
    read.dataFrame.columns.contains("_change_type") shouldBe false
    read.orderFields.head should startWith ("_pudding_change_sequence")
    read.orderFields should contain ("_change_ordinal")
    read.orderFields should contain ("_pudding_change_priority")

    val latest = IncrementalFrames.latestByKey(read.dataFrame, Seq("id"), read.orderFields)
    latest.where("id = 1").select("op").head().getString(0) shouldBe "I"
  }

  test("order upsert changes by snapshot sequence rather than random snapshot id") {
    val name = "orders_upsert_sequence"
    createTable(name)
    val begin = latestSnapshot(name)
    sparkSession.sql(s"UPDATE ${fullTableName(name)} SET name = 'tom_v2' WHERE id = 1")
    sparkSession.sql(s"UPDATE ${fullTableName(name)} SET name = 'tom_v3' WHERE id = 1")

    val read = new IcebergSource().readIncremental(
      sparkSession,
      catalogSource(name),
      request(IncrementalSemantics.Upsert, Some(begin.toString)))

    read.dataFrame.where("id = 1").select("name").head().getString(0) shouldBe "tom_v3"
  }

  test("read catalog changelog changes as upsert rows") {
    val name = "orders_upsert"
    createTable(name)
    val begin = latestSnapshot(name)
    sparkSession.sql(s"UPDATE ${fullTableName(name)} SET name = 'tom_v2' WHERE id = 1")
    sparkSession.sql(
      s"INSERT INTO ${fullTableName(name)} VALUES (3, 'spark', '2026-01-03')")

    val read = new IcebergSource().readIncremental(
      sparkSession,
      catalogSource(name),
      request(IncrementalSemantics.Upsert, Some(begin.toString)))

    read.dataFrame.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((1, "tom_v2"), (3, "spark"))
    read.dataFrame.columns.contains("_change_type") shouldBe false
    read.orderFields shouldBe empty
  }

  test("read a full snapshot as upsert rows") {
    val name = "orders_upsert_full"
    createTable(name)

    val read = new IcebergSource().readIncremental(
      sparkSession,
      catalogSource(name),
      request(IncrementalSemantics.Upsert, None, "full"))

    read.dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(1, 2)
    read.dataFrame.columns.contains("__op") shouldBe false
  }

  test("reject a deleted key in upsert semantics") {
    val name = "orders_upsert_delete"
    createTable(name)
    val begin = latestSnapshot(name)
    sparkSession.sql(s"DELETE FROM ${fullTableName(name)} WHERE id = 2")

    val error = intercept[PuddingException] {
      new IcebergSource().readIncremental(
        sparkSession,
        catalogSource(name),
        request(IncrementalSemantics.Upsert, Some(begin.toString)))
    }
    error.getMessage should include ("use cdc semantics")
  }

  test("require a start or initial mode for an incremental read") {
    val name = "orders_missing_cursor"
    createTable(name)
    val error = intercept[PuddingException] {
      new IcebergSource().readIncremental(
        sparkSession,
        catalogSource(name),
        request(IncrementalSemantics.Append, None))
    }
    error.getMessage should include ("requires incremental.start")
  }
}
