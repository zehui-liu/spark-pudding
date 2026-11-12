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

import java.nio.file.{Files, Path => FilePath}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.definition.SourceContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for batch Iceberg reads through a catalog and a direct table path.
 */
class IcebergSourceBatchTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private var workspace: FilePath = _

  private val catalog = ResolvedCatalog(
    "iceberg",
    "iceberg",
    "local",
    sessionCatalog = false,
    sparkConf = Map.empty,
    properties = Map.empty)

  private val table = "local.default.batch_orders"

  override def beforeAll(): Unit = {
    workspace = Files.createTempDirectory("iceberg-source-batch")
    sparkSession = SparkSession.builder()
      .appName("test for iceberg batch source")
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
    sparkSession.sql(s"DROP TABLE IF EXISTS $table")
    sparkSession.sql(
      s"""CREATE TABLE $table (id INT, name STRING, region STRING)
         |USING iceberg PARTITIONED BY (region)""".stripMargin)
    sparkSession.sql(
      s"INSERT INTO $table VALUES (1, 'tom', 'east'), (2, 'jerry', 'west')")
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      Try(sparkSession.sql("DROP TABLE IF EXISTS local.default.batch_orders"))
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

  private def tableLocation: String = {
    sparkSession.sql(s"DESCRIBE EXTENDED $table")
      .where("col_name = 'Location'")
      .select("data_type")
      .head().getString(0)
  }

  test("read a table through a catalog with a filter and schema projection") {
    val dataFrame = new IcebergSource().initData(
      sparkSession,
      SourceContext(
        config = Some(Map(
          "table" -> "default.batch_orders",
          "filterExpr" -> "id > 0",
          "forceConvertSchema" -> "true",
          "schema" -> Map("id" -> "string", "name" -> "string"))),
        catalog = Some(catalog)))

    dataFrame.columns.toSeq shouldBe Seq("id", "name")
    dataFrame.schema("id").dataType shouldBe StringType
    dataFrame.orderBy("id").collect()
      .map(row => (row.getString(0), row.getString(1))).toSeq shouldBe
      Seq(("1", "tom"), ("2", "jerry"))
  }

  test("read a table by path") {
    val dataFrame = new IcebergSource().initData(
      sparkSession,
      Some(Map("path" -> tableLocation)))

    dataFrame.count() shouldBe 2
  }

  test("reject missing selectors and invalid config types") {
    intercept[PuddingException] {
      new IcebergSource().initData(sparkSession, Some(Map.empty[String, Any]))
    }
    intercept[PuddingException] {
      new IcebergSource().initData(
        sparkSession,
        Some(Map("table" -> 1, "path" -> tableLocation)))
    }
    intercept[PuddingException] {
      new IcebergSource().initData(
        sparkSession,
        Some(Map("table" -> "default.batch_orders", "branch" -> 42)))
    }
  }
}
