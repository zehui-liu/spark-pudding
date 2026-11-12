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
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.definition.SourceContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Iceberg Structured Streaming snapshot reads.
 */
class IcebergSourceStreamingTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

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
    workspace = Files.createTempDirectory("iceberg-source-streaming")
    sparkSession = SparkSession.builder()
      .appName("test for iceberg streaming source")
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

  private def createTable(name: String): Unit = {
    val session = sparkSession
    import session.implicits._
    sparkSession.sql(s"DROP TABLE IF EXISTS local.default.$name")
    Seq(
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02")
    ).toDF("id", "name", "dt")
      .write.format("iceberg")
      .mode("overwrite")
      .saveAsTable(s"local.default.$name")
  }

  test("create a streaming DataFrame from an Iceberg catalog table") {
    createTable("stream_orders")
    val dataFrame = new IcebergSource().initData(
      sparkSession,
      SourceContext(
        Some(Map(
          "table" -> "default.stream_orders",
          "streamRead" -> true)),
        catalog = Some(catalog)))

    dataFrame.isStreaming shouldBe true
    dataFrame.schema.fieldNames.toSeq shouldBe Seq("id", "name", "dt")
  }

  test("create a streaming DataFrame from an Iceberg table path") {
    createTable("stream_orders_path")
    val location = sparkSession.sql("DESCRIBE EXTENDED local.default.stream_orders_path")
      .where("col_name = 'Location'")
      .select("data_type")
      .head().getString(0)
    val dataFrame = new IcebergSource().initData(
      sparkSession,
      Some(Map(
        "path" -> location,
        "streamRead" -> true)))

    dataFrame.isStreaming shouldBe true
    dataFrame.schema.fieldNames.toSeq shouldBe Seq("id", "name", "dt")
  }

  test("reject batch snapshot selectors in streaming mode") {
    createTable("stream_orders_selector")
    val error = intercept[PuddingException] {
      new IcebergSource().initData(
        sparkSession,
        SourceContext(
          Some(Map(
            "table" -> "default.stream_orders_selector",
            "streamRead" -> true,
            "snapshotId" -> 1L)),
          catalog = Some(catalog)))
    }
    error.getMessage should include ("batch snapshot selectors")
  }

  test("read historical snapshots with stream-from-timestamp") {
    val name = "stream_orders_history"
    createTable(name)
    val earliest = sparkSession.sql(s"SELECT committed_at FROM local.default.$name.snapshots " +
      "ORDER BY committed_at ASC LIMIT 1")
      .head().getTimestamp(0).getTime
    val checkpoint = Files.createTempDirectory("iceberg-stream-history-checkpoint")
    val collected = ArrayBuffer.empty[(Int, String)]
    val stream = new IcebergSource().initData(
      sparkSession,
      SourceContext(
        Some(Map(
          "table" -> s"default.$name",
          "streamRead" -> true,
          "options" -> Map("stream-from-timestamp" -> (earliest - 1000L).toString))),
        catalog = Some(catalog)))

    val query = stream.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, _: Long) =>
        collected ++= batch.select("id", "name").collect()
          .map(row => (row.getInt(0), row.getString(1)))
        ()
      }
      .option("checkpointLocation", checkpoint.toString)
      .trigger(Trigger.AvailableNow())
      .start()

    try {
      query.awaitTermination(120000L)
    } finally {
      query.stop()
      val paths = Files.walk(checkpoint)
      try {
        paths.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(path => Try(Files.deleteIfExists(path)))
      } finally {
        paths.close()
      }
    }

    collected.toSet should contain allOf ((1, "tom"), (2, "jerry"))
  }
}
