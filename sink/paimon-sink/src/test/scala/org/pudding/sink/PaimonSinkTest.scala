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
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.max
import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{
  IncrementalSemantics,
  IncrementalWrite,
  SinkContext
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Paimon catalog writes.
 */
class PaimonSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

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
    warehouse = Files.createTempDirectory("paimon-sink-test")
    sparkSession = SparkSession.builder()
      .appName("test for paimon sink")
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
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      Try(sparkSession.sql("DROP TABLE IF EXISTS paimon.default.orders_pk"))
      sparkSession.sql("DROP TABLE IF EXISTS paimon.default.orders_overwrite")
      sparkSession.sql("DROP TABLE IF EXISTS paimon.default.orders_transaction")
      sparkSession.sql("DROP TABLE IF EXISTS paimon.default.orders_fencing")
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

  private def orders(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02"),
      (3, "spark", "2026-01-01")
    ).toDF("id", "name", "dt")
  }

  private def config(
      table: String,
      mode: String = "append"): Map[String, Any] = {
    Map(
      "table" -> table,
      "primaryKey" -> Seq("id", "dt"),
      "partition" -> Seq("dt"),
      "bucket" -> 1,
      "mode" -> mode,
      "tableOptions" -> Map(
        "merge-engine" -> "deduplicate",
        "file.format" -> "parquet"))
  }

  test("stream cdc operations into a Paimon path table") {
    val table = "default.orders_stream_cdc"
    val checkpoint = warehouse.resolve("orders_stream_cdc_checkpoint")
    val input = warehouse.resolve("orders_stream_cdc_input")
    Files.createDirectories(input)
    sparkSession.sql(
      s"""CREATE TABLE paimon.$table (
         |  id INT,
         |  name STRING,
         |  dt STRING
         |) PARTITIONED BY (dt)
         |TBLPROPERTIES ('primary-key' = 'id,dt', 'bucket' = '1')""".stripMargin)
    val path = sparkSession.sql(s"DESCRIBE EXTENDED paimon.$table")
      .where("upper(col_name) = 'LOCATION'")
      .select("data_type")
      .collect()
      .headOption
      .map(_.getString(0))
      .getOrElse {
        throw new PuddingException(s"Paimon table $table has no location")
      }
    new PaimonSink().writer(
      orders(),
      SinkContext(Some(Map("path" -> path, "mode" -> "append"))))

    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","dt":"2026-01-01","__op":"U","_kafka_offset":1}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":2,"name":"jerry","dt":"2026-01-02","__op":"D","_kafka_offset":2}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("3.json"),
      """{"id":4,"name":"spark","dt":"2026-01-04","__op":"I","_kafka_offset":3}"""
        .getBytes(StandardCharsets.UTF_8))

    val stream = sparkSession.readStream
      .schema("id int, name string, dt string, __op string, _kafka_offset long")
      .json(input.toString)
    val streamConfig = Map[String, Any](
      "path" -> path,
      "primaryKey" -> Seq("id", "dt"),
      "partition" -> Seq("dt"),
      "bucket" -> 1,
      "checkpointLocation" -> checkpoint.toString,
      "outputMode" -> "append",
      "cdcOpColumn" -> "__op",
      "cdcOrderFields" -> Seq("_kafka_offset"))

    new PaimonSink().writer(
      stream,
      SinkContext(
        Some(streamConfig),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    val result = sparkSession.read.format("paimon").load(path)
    result.count() shouldBe 3
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
    result.where("id = 2").count() shouldBe 0
    result.where("id = 4").select("name").collect().head.getString(0) shouldBe "spark"
  }

  test("merge update output into a Paimon primary-key table") {
    val table = "default.orders_stream_update"
    sparkSession.sql(
      s"""CREATE TABLE paimon.$table (
         |  id INT,
         |  name STRING,
         |  dt STRING
         |) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
    val path = sparkSession.sql(s"DESCRIBE EXTENDED paimon.$table")
      .where("upper(col_name) = 'LOCATION'")
      .select("data_type")
      .collect()
      .head
      .getString(0)
    sparkSession.sql(
      s"""INSERT INTO paimon.$table VALUES
         |(1, 'tom', '2026-01-01'),
         |(2, 'jerry', '2026-01-02')""".stripMargin)

    val checkpoint = warehouse.resolve("orders_stream_update_checkpoint")
    val input = warehouse.resolve("orders_stream_update_input")
    Files.createDirectories(input)
    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","dt":"2026-01-01"}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":3,"name":"spark","dt":"2026-01-04"}"""
        .getBytes(StandardCharsets.UTF_8))
    val stream = sparkSession.readStream
      .schema("id int, name string, dt string")
      .json(input.toString)
      .groupBy("id", "dt")
      .agg(max("name").as("name"))
    val streamConfig = Map[String, Any](
      "path" -> path,
      "checkpointLocation" -> checkpoint.toString,
      "outputMode" -> "update")

    new PaimonSink().writer(
      stream,
      SinkContext(
        Some(streamConfig),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    val result = sparkSession.read.format("paimon").load(path)
    result.count() shouldBe 3
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
    result.where("id = 2").select("name").collect().head.getString(0) shouldBe "jerry"
    result.where("id = 3").select("name").collect().head.getString(0) shouldBe "spark"
  }

  test("stream append into a Paimon catalog table through its location") {
    val table = "default.orders_catalog_append_stream"
    sparkSession.sql(s"DROP TABLE IF EXISTS paimon.$table")
    sparkSession.sql(
      s"""CREATE TABLE paimon.$table (
         |  id INT,
         |  name STRING,
         |  dt STRING
         |) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
    sparkSession.sql(
      s"INSERT INTO paimon.$table VALUES (1, 'tom', '2026-01-01'), " +
        "(2, 'jerry', '2026-01-02')")
    val input = warehouse.resolve("orders_catalog_append_stream_input")
    Files.createDirectories(input)
    Files.write(
      input.resolve("1.json"),
      """{"id":4,"name":"spark","dt":"2026-01-04"}""".getBytes(StandardCharsets.UTF_8))
    val checkpoint = warehouse.resolve("orders_catalog_append_stream_checkpoint")

    new PaimonSink().writer(
      sparkSession.readStream.schema("id int, name string, dt string").json(input.toString),
      SinkContext(
        Some(Map[String, Any](
          "table" -> table,
          "checkpointLocation" -> checkpoint.toString,
          "outputMode" -> "append")),
        catalog = Some(catalog),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    sparkSession.table(s"paimon.$table").count() shouldBe 3
    sparkSession.table(s"paimon.$table").where("id = 4").count() shouldBe 1
  }

  test("stream update into a Paimon catalog table without a path") {
    val table = "default.orders_catalog_update_stream"
    sparkSession.sql(s"DROP TABLE IF EXISTS paimon.$table")
    sparkSession.sql(
      s"""CREATE TABLE paimon.$table (
         |  id INT,
         |  name STRING,
         |  dt STRING
         |) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
    sparkSession.sql(
      s"INSERT INTO paimon.$table VALUES (1, 'tom', '2026-01-01'), " +
        "(2, 'jerry', '2026-01-02')")
    val input = warehouse.resolve("orders_catalog_update_stream_input")
    Files.createDirectories(input)
    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","dt":"2026-01-01"}""".getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":3,"name":"spark","dt":"2026-01-03"}""".getBytes(StandardCharsets.UTF_8))
    val checkpoint = warehouse.resolve("orders_catalog_update_stream_checkpoint")
    val stream = sparkSession.readStream
      .schema("id int, name string, dt string")
      .json(input.toString)
      .groupBy("id", "dt")
      .agg(max("name").as("name"))

    new PaimonSink().writer(
      stream,
      SinkContext(
        Some(Map[String, Any](
          "table" -> table,
          "checkpointLocation" -> checkpoint.toString,
          "outputMode" -> "update")),
        catalog = Some(catalog),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    val result = sparkSession.table(s"paimon.$table")
    result.count() shouldBe 3
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
    result.where("id = 3").select("name").collect().head.getString(0) shouldBe "spark"
  }

  test("reject Paimon catalog streaming when the target table is missing") {
    val stream = sparkSession.readStream.format("rate").load()
    val streamConfig = Map[String, Any](
      "table" -> "default.orders_missing_stream",
      "checkpointLocation" -> warehouse.resolve("paimon_missing_stream_checkpoint").toString,
      "outputMode" -> "append")
    val error = intercept[PuddingException] {
      new PaimonSink().writer(
        stream,
        SinkContext(
          Some(streamConfig),
          catalog = Some(catalog),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    }
    error.getMessage should include ("existing table")
  }

  test("declare lakehouse incremental and full-refresh capabilities") {
    val sink = new PaimonSink()
    sink.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)
    sink.isIdempotentIncremental(IncrementalSemantics.Append) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Upsert) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Cdc) shouldBe true
    sink.supportsFullRefresh shouldBe true
    sink.supportsAtomicReplace shouldBe false
    sink.supportedStreamingOutputModes shouldBe Set("append", "update")
  }

  test("write a Paimon primary-key table") {
    new PaimonSink().writer(
      orders(),
      SinkContext(Some(config("default.orders_pk")), catalog = Some(catalog)))

    val result = sparkSession.table("paimon.default.orders_pk")
    result.count() shouldBe 3
    result.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((1, "tom"), (2, "jerry"), (3, "spark"))
  }

  test("skip an append transaction that is already in the Paimon timeline") {
    val table = "default.orders_transaction"
    orders().limit(0).write.format("paimon")
      .partitionBy("dt")
      .mode("overwrite")
      .saveAsTable("paimon.default.orders_transaction")
    val sink = new PaimonSink()
    val config = Map[String, Any](
      "table" -> table,
      "partition" -> Seq("dt"),
      "bucket" -> 1,
      "mode" -> "append")
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "snapshot_id",
      transactionId = "txn-paimon-append")

    sink.writeIncremental(orders(), SinkContext(Some(config), catalog = Some(catalog)), write)
    sparkSession.table("paimon.default.orders_transaction").count() shouldBe 3

    sink.writeIncremental(orders(), SinkContext(Some(config), catalog = Some(catalog)), write)
    sparkSession.table("paimon.default.orders_transaction").count() shouldBe 3

    sink.writeIncremental(
      orders(),
      SinkContext(Some(config), catalog = Some(catalog)),
      write.copy(transactionId = "txn-paimon-append-2"))
    sparkSession.table("paimon.default.orders_transaction").count() shouldBe 6
  }

  test("reject an incremental Paimon writer with an older fencing token") {
    val table = "default.orders_fencing"
    orders().limit(0).write.format("paimon")
      .partitionBy("dt")
      .mode("overwrite")
      .saveAsTable(s"paimon.$table")
    val sink = new PaimonSink()
    sink.supportsIncrementalFencing shouldBe true
    val config = Map[String, Any](
      "table" -> table,
      "partition" -> Seq("dt"),
      "bucket" -> 1,
      "mode" -> "append")
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "snapshot_id",
      transactionId = "txn-paimon-fence-1",
      sourceId = "source-paimon",
      fencingToken = 1L)

    sink.writeIncremental(orders(), SinkContext(Some(config), catalog = Some(catalog)), write)
    sink.writeIncremental(
      orders(),
      SinkContext(Some(config), catalog = Some(catalog)),
      write.copy(transactionId = "txn-paimon-fence-2", fencingToken = 2L))
    sparkSession.table(s"paimon.$table").count() shouldBe 6

    val error = intercept[PuddingException] {
      sink.writeIncremental(
        orders(),
        SinkContext(Some(config), catalog = Some(catalog)),
        write.copy(transactionId = "txn-paimon-fence-3", fencingToken = 1L))
    }
    error.getMessage should include ("stale incremental writer")
    sparkSession.table(s"paimon.$table").count() shouldBe 6

    // Replaying the already committed transaction with the same generation must stay a no-op.
    sink.writeIncremental(
      orders(),
      SinkContext(Some(config), catalog = Some(catalog)),
      write.copy(transactionId = "txn-paimon-fence-2", fencingToken = 2L))
    sparkSession.table(s"paimon.$table").count() shouldBe 6

    // An equal generation with a different transaction belongs to a stale writer.
    val equal = intercept[PuddingException] {
      sink.writeIncremental(
        orders(),
        SinkContext(Some(config), catalog = Some(catalog)),
        write.copy(transactionId = "txn-paimon-fence-equal", fencingToken = 2L))
    }
    equal.getMessage should include ("stale incremental writer")
    sparkSession.table(s"paimon.$table").count() shouldBe 6
  }

  test("merge a Paimon primary-key table") {
    val session = sparkSession
    import session.implicits._
    new PaimonSink().writer(
      Seq((1, "tom_v2", "2026-01-01")).toDF("id", "name", "dt"),
      SinkContext(Some(config("default.orders_pk")), catalog = Some(catalog)))

    val result = sparkSession.table("paimon.default.orders_pk")
    result.count() shouldBe 3
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
  }

  test("replace a Paimon table through the full-refresh contract") {
    val sink = new PaimonSink()
    sink.writer(
      orders(),
      SinkContext(Some(config("default.orders_atomic")), catalog = Some(catalog)))
    val session = sparkSession
    import session.implicits._
    sink.writeFullRefresh(
      Seq((9, "replacement", "2026-03-01")).toDF("id", "name", "dt"),
      SinkContext(
        Some(config("default.orders_atomic", mode = "overwrite")),
        catalog = Some(catalog)))

    val result = sparkSession.table("paimon.default.orders_atomic")
    result.count() shouldBe 1
    result.select("id", "name").collect().head.getInt(0) shouldBe 9
    sink.supportsAtomicReplace shouldBe false
  }

  test("overwrite a Paimon table") {
    new PaimonSink().writer(
      orders(),
      SinkContext(
        Some(config("default.orders_overwrite", mode = "overwrite")),
        catalog = Some(catalog)))

    val session = sparkSession
    import session.implicits._
    new PaimonSink().writer(
      Seq((4, "new", "2026-02-01")).toDF("id", "name", "dt"),
      SinkContext(
        Some(config("default.orders_overwrite", mode = "overwrite")),
        catalog = Some(catalog)))

    sparkSession.table("paimon.default.orders_overwrite").count() shouldBe 1
  }

  test("apply upsert incremental semantics as a Paimon merge") {
    val sink = new PaimonSink()
    sink.writer(
      orders(),
      SinkContext(Some(config("default.orders_incremental_upsert")), catalog = Some(catalog)))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01"),
        (4, "new", "2026-02-01")
      ).toDF("id", "name", "dt"),
      SinkContext(Some(config("default.orders_incremental_upsert")), catalog = Some(catalog)),
      IncrementalWrite(
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id", "dt"),
        opColumn = "__op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id"))

    val result = sparkSession.table("paimon.default.orders_incremental_upsert")
    result.count() shouldBe 4
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
  }

  test("reject upsert key fields that differ from the target primary keys") {
    val sink = new PaimonSink()
    val sinkContext = SinkContext(
      Some(config("default.orders_incremental_upsert")),
      catalog = Some(catalog))
    sink.writer(orders(), sinkContext)

    val error = intercept[PuddingException] {
      sink.writeIncremental(
        orders(),
        sinkContext,
        IncrementalWrite(
          semantics = IncrementalSemantics.Upsert,
          keyFields = Seq("id"),
          opColumn = "__op",
          fromCursor = Some("1"),
          toCursor = "2",
          cursorType = "snapshot_id"))
    }
    error.getMessage should include ("must match the target table primary keys")
  }

  test("apply cdc incremental semantics with deletes") {
    val sink = new PaimonSink()
    sink.writer(
      orders(),
      SinkContext(Some(config("default.orders_incremental_cdc")), catalog = Some(catalog)))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", "U"),
        (2, "jerry", "2026-01-02", "D")
      ).toDF("id", "name", "dt", "op"),
      SinkContext(Some(config("default.orders_incremental_cdc")), catalog = Some(catalog)),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id", "dt"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id"))

    val result = sparkSession.table("paimon.default.orders_incremental_cdc")
    result.count() shouldBe 2
    result.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((1, "tom_v2"), (3, "spark"))
    result.columns.contains("_row_kind_") shouldBe false
  }

  test("keep only the last cdc operation per key when ordering is available") {
    val sink = new PaimonSink()
    sink.writer(
      orders(),
      SinkContext(Some(config("default.orders_incremental_cdc_ordered")), catalog = Some(catalog)))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", "I", 1L),
        (1, "tom_v2", "2026-01-01", "D", 2L)
      ).toDF("id", "name", "dt", "op", "change_order"),
      SinkContext(
        Some(config("default.orders_incremental_cdc_ordered")),
        catalog = Some(catalog)),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id", "dt"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id",
        orderFields = Seq("change_order")))

    val result = sparkSession.table("paimon.default.orders_incremental_cdc_ordered")
    result.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2, 3)
    result.columns.contains("change_order") shouldBe false
  }

  test("order cdc changes using the sink cdcOrderFields") {
    val sink = new PaimonSink()
    val sinkContext = SinkContext(
      Some(config("default.orders_incremental_cdc_config_order") ++
        Map("cdcOrderFields" -> Seq("change_order"))),
      catalog = Some(catalog))
    sink.writer(orders(), sinkContext)

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", "I", 1L),
        (1, "tom_v2", "2026-01-01", "D", 2L)
      ).toDF("id", "name", "dt", "op", "change_order"),
      sinkContext,
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id", "dt"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id"))

    val result = sparkSession.table("paimon.default.orders_incremental_cdc_config_order")
    result.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2, 3)
    result.columns.contains("change_order") shouldBe false
  }

  test("reject streaming incremental writes") {
    val error = intercept[PuddingException] {
      new PaimonSink().writeIncremental(
        orders(),
        SinkContext(
          Some(config("default.orders_stream") ++ Map(
            "isStreaming" -> false,
            "streamWrite" -> true)),
          catalog = Some(catalog)),
        IncrementalWrite(
          semantics = IncrementalSemantics.Upsert,
          keyFields = Seq("id", "dt"),
          opColumn = "__op",
          fromCursor = None,
          toCursor = "2",
          cursorType = "snapshot_id"))
    }
    error.getMessage should include ("does not support streaming incremental writes")
  }

  test("require a table or path") {
    val error = intercept[PuddingException] {
      new PaimonSink().writer(
        orders(),
        SinkContext(Some(Map.empty), catalog = Some(catalog)))
    }
    error.getMessage should include ("table or path")
  }

  test("reject a non-Paimon catalog") {
    val otherCatalog = catalog.copy(`type` = "hudi")
    val error = intercept[PuddingException] {
      new PaimonSink().writer(
        orders(),
        SinkContext(Some(config("default.orders_pk")), catalog = Some(otherCatalog)))
    }
    error.getMessage should include ("requires a paimon catalog")
  }
}
