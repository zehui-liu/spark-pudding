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
import java.util.Comparator

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{IncrementalSemantics, IncrementalWrite, SinkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * test for IcebergSink
 */
class IcebergSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val catalog = "pudding_test"

  private val database = "db"

  private var warehouse: Path = _

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    warehouse = Files.createTempDirectory("iceberg-sink-test")
    sparkSession = SparkSession.builder()
      .appName("test for iceberg sink")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(s"spark.sql.catalog.$catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$catalog.type", "hadoop")
      .config(s"spark.sql.catalog.$catalog.warehouse", warehouse.toString)
      .getOrCreate()
    sparkSession.sql(s"create namespace if not exists $catalog.$database")
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      Try(sparkSession.sql(s"drop namespace if exists $catalog.$database cascade"))
      sparkSession.close()
    }
    deleteRecursively(warehouse)
  }

  private def newTable(name: String): String =
    s"`$catalog`.`$database`.`$name`"

  private def rows(table: String): Set[(Int, String)] =
    sparkSession.sql(s"select id, name from $table")
      .collect()
      .map(row => (row.getInt(0), row.getString(1)))
      .toSet

  test("stream append rows into an Iceberg table") {
    val table = newTable("sink_stream_append")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    val catalog = ResolvedCatalog(
      "pudding_test", "iceberg", "pudding_test", sessionCatalog = false,
      sparkConf = Map.empty, properties = Map.empty)
    sink.writer(
      Seq((1, "tom")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))

    val input = Files.createTempDirectory("iceberg-stream-append")
    val checkpoint = Files.createTempDirectory("iceberg-stream-append-checkpoint")
    Files.write(
      input.resolve("1.json"),
      """{"id":2,"name":"jerry"}""".getBytes(StandardCharsets.UTF_8))
    val stream = sparkSession.readStream.schema("id int, name string").json(input.toString)
    sink.writer(
      stream,
      SinkContext(
        Some(Map(
          "table" -> table,
          "outputMode" -> "append",
          "checkpointLocation" -> checkpoint.toString)),
        catalog = Some(catalog),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("stream cdc operations into an Iceberg table") {
    val table = newTable("sink_stream_cdc")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    val catalog = ResolvedCatalog(
      "pudding_test", "iceberg", "pudding_test", sessionCatalog = false,
      sparkConf = Map.empty, properties = Map.empty)
    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))

    val input = Files.createTempDirectory("iceberg-stream-cdc")
    val checkpoint = Files.createTempDirectory("iceberg-stream-cdc-checkpoint")
    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","__op":"U","_kafka_offset":1}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":2,"name":"jerry","__op":"D","_kafka_offset":2}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("3.json"),
      """{"id":3,"name":"spark","__op":"I","_kafka_offset":3}"""
        .getBytes(StandardCharsets.UTF_8))
    val stream = sparkSession.readStream
      .schema("id int, name string, __op string, _kafka_offset long")
      .json(input.toString)
    sink.writer(
      stream,
      SinkContext(
        Some(Map(
          "table" -> table,
          "outputMode" -> "append",
          "checkpointLocation" -> checkpoint.toString,
          "cdcOpColumn" -> "__op",
          "recordKeyFields" -> Seq("id"),
          "cdcOrderFields" -> Seq("_kafka_offset"))),
        catalog = Some(catalog),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    sparkSession.catalog.refreshTable(table)
    rows(table) shouldBe Set((1, "tom_v2"), (3, "spark"))
  }

  test("stream upsert full rows without an operation column") {
    val table = newTable("sink_stream_upsert")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))

    val input = Files.createTempDirectory("iceberg-stream-upsert")
    val checkpoint = Files.createTempDirectory("iceberg-stream-upsert-checkpoint")
    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","_kafka_offset":1}""".getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":3,"name":"spark","_kafka_offset":2}""".getBytes(StandardCharsets.UTF_8))
    val stream = sparkSession.readStream
      .schema("id int, name string, _kafka_offset long")
      .json(input.toString)
    val catalog = ResolvedCatalog(
      "pudding_test", "iceberg", "pudding_test", sessionCatalog = false,
      sparkConf = Map.empty, properties = Map.empty)
    sink.writer(
      stream,
      SinkContext(
        Some(Map(
          "table" -> table,
          "outputMode" -> "append",
          "checkpointLocation" -> checkpoint.toString,
          "operation" -> "upsert",
          "recordKeyFields" -> Seq("id"),
          "cdcOrderFields" -> Seq("_kafka_offset"))),
        catalog = Some(catalog),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    sparkSession.catalog.refreshTable(table)
    rows(table) shouldBe Set((1, "tom_v2"), (2, "jerry"), (3, "spark"))
  }

  test("merge update output into an Iceberg table") {
    val table = newTable("sink_stream_update")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    val catalog = ResolvedCatalog(
      "pudding_test", "iceberg", "pudding_test", sessionCatalog = false,
      sparkConf = Map.empty, properties = Map.empty)
    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))

    val input = Files.createTempDirectory("iceberg-stream-update")
    val checkpoint = Files.createTempDirectory("iceberg-stream-update-checkpoint")
    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2"}""".getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":3,"name":"spark"}""".getBytes(StandardCharsets.UTF_8))
    val stream = sparkSession.readStream
      .schema("id int, name string")
      .json(input.toString)
      .groupBy("id")
      .agg(max("name").as("name"))
    sink.writer(
      stream,
      SinkContext(
        Some(Map(
          "table" -> table,
          "outputMode" -> "update",
          "checkpointLocation" -> checkpoint.toString,
          "recordKeyFields" -> Seq("id"))),
        catalog = Some(catalog),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    sparkSession.catalog.refreshTable(table)
    rows(table) shouldBe Set((1, "tom_v2"), (2, "jerry"), (3, "spark"))
  }

  test("reject invalid Iceberg streaming options") {
    val stream = sparkSession.readStream.format("rate").load()
    val table = newTable("sink_stream_invalid")
    val session = sparkSession
    import session.implicits._
    sparkSession.sql(s"drop table if exists $table")
    new IcebergSink().writer(
      Seq((1, "tom")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))
    val catalog = ResolvedCatalog(
      "pudding_test", "iceberg", "pudding_test", sessionCatalog = false,
      sparkConf = Map.empty, properties = Map.empty)
    val missingCheckpoint = intercept[PuddingException] {
      new IcebergSink().writer(
        stream,
        SinkContext(
          Some(Map("table" -> table, "outputMode" -> "append")),
          catalog = Some(catalog),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    }
    missingCheckpoint.getMessage should include ("checkpointLocation")

    val missingCatalog = intercept[PuddingException] {
      new IcebergSink().writer(
        stream,
        SinkContext(
          Some(Map(
            "table" -> table,
            "checkpointLocation" -> Files.createTempDirectory("iceberg-checkpoint").toString)),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    }
    missingCatalog.getMessage should include ("catalog")
  }

  test("have correct identifier") {
    new IcebergSink().identifier shouldBe "iceberg-sink"
    new IcebergSink().supportsCatalog shouldBe true
  }

  test("support append, upsert and cdc incremental semantics") {
    val sink = new IcebergSink()
    sink.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)
    sink.isIdempotentIncremental(IncrementalSemantics.Append) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Upsert) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Cdc) shouldBe true
  }

  test("apply upsert incremental semantics to an existing Iceberg table") {
    val table = newTable("sink_incremental_upsert")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      context)
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Upsert,
      keyFields = Seq("id"),
      opColumn = "__op",
      fromCursor = Some("1"),
      toCursor = "2",
      cursorType = "snapshot_id")

    sink.writeIncremental(
      Seq((1, "tom_v2"), (3, "spark")).toDF("id", "name"),
      context,
      write)
    rows(table) shouldBe Set((1, "tom_v2"), (2, "jerry"), (3, "spark"))

    sink.writeIncremental(
      Seq((1, "tom_v2"), (3, "spark")).toDF("id", "name"),
      context,
      write)
    rows(table) shouldBe Set((1, "tom_v2"), (2, "jerry"), (3, "spark"))
  }

  test("create the target from an upsert incremental batch") {
    val table = newTable("sink_incremental_upsert_create")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val context = SinkContext(Some(Map("table" -> table)))

    sink.writeIncremental(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      context,
      IncrementalWrite(
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id"),
        opColumn = "__op",
        fromCursor = None,
        toCursor = "1",
        cursorType = "snapshot_id"))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("order upsert rows using the sink cdcOrderFields") {
    val table = newTable("sink_incremental_upsert_config_order")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(Seq((1, "tom")).toDF("id", "name"), context)

    val orderedContext = SinkContext(Some(Map(
      "table" -> table,
      "mode" -> "overwrite",
      "cdcOrderFields" -> Seq("change_order"))))
    sink.writeIncremental(
      Seq(
        (1, "first", 1L),
        (1, "second", 2L)).toDF("id", "name", "change_order"),
      orderedContext,
      IncrementalWrite(
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id"),
        opColumn = "__op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id"))

    rows(table) shouldBe Set((1, "second"))
  }

  test("apply cdc incremental semantics with deletes") {
    val table = newTable("sink_incremental_cdc")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(
      Seq((1, "tom"), (2, "jerry"), (3, "spark")).toDF("id", "name"),
      context)
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Cdc,
      keyFields = Seq("id"),
      opColumn = "op",
      fromCursor = Some("1"),
      toCursor = "2",
      cursorType = "snapshot_id",
      orderFields = Seq("ord"))
    val batch = Seq(
      (1, "tom_v2", "U", 1L),
      (2, "jerry", "D", 2L),
      (4, "new", "I", 3L)).toDF("id", "name", "op", "ord")

    sink.writeIncremental(batch, context, write)
    rows(table) shouldBe Set((1, "tom_v2"), (3, "spark"), (4, "new"))

    sink.writeIncremental(batch, context, write)
    rows(table) shouldBe Set((1, "tom_v2"), (3, "spark"), (4, "new"))
  }

  test("apply cdc incremental semantics without ordering when each key changes once") {
    val table = newTable("sink_incremental_cdc_unordered")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      context)

    sink.writeIncremental(
      Seq((1, "tom_v2", "U"), (3, "spark", "I")).toDF("id", "name", "op"),
      context,
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id"))

    rows(table) shouldBe Set((1, "tom_v2"), (2, "jerry"), (3, "spark"))
  }

  test("reject unordered cdc changes for one key") {
    val table = newTable("sink_incremental_cdc_ambiguous")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(Seq((1, "tom")).toDF("id", "name"), context)

    val error = intercept[PuddingException] {
      sink.writeIncremental(
        Seq((1, "first", "U"), (1, "second", "I")).toDF("id", "name", "op"),
        context,
        IncrementalWrite(
          semantics = IncrementalSemantics.Cdc,
          keyFields = Seq("id"),
          opColumn = "op",
          fromCursor = Some("1"),
          toCursor = "2",
          cursorType = "snapshot_id"))
    }
    error.getMessage should include ("no ordering metadata")
    rows(table) shouldBe Set((1, "tom"))
  }

  test("apply a full cdc snapshot to a missing Iceberg table") {
    val table = newTable("sink_incremental_cdc_create")
    sparkSession.sql(s"drop table if exists $table")
    val sink = new IcebergSink()
    val session = sparkSession
    import session.implicits._
    val batch = Seq(
      (1, "tom", "I"),
      (2, "jerry", "I")).toDF("id", "name", "op")

    sink.writeIncremental(
      batch,
      SinkContext(Some(Map("table" -> table))),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        fromCursor = None,
        toCursor = "1",
        cursorType = "snapshot_id"))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("force append for incremental writes") {
    val table = newTable("sink_incremental_append")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()

    sink.writer(
      Seq((1, "tom")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writeIncremental(
      Seq((2, "jerry")).toDF("id", "name"),
      org.pudding.core.definition.SinkContext(Some(
        Map("table" -> table, "mode" -> "overwrite"))),
      IncrementalWrite(
        semantics = IncrementalSemantics.Append,
        keyFields = Seq.empty,
        opColumn = "__op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id"))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("skip an append transaction already in the Iceberg snapshot summary") {
    val table = newTable("sink_transaction")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "snapshot_id",
      transactionId = "txn-iceberg-append")

    sink.writeIncremental(
      Seq((1, "tom")).toDF("id", "name"),
      SinkContext(Some(Map("table" -> table, "mode" -> "append"))),
      write)
    rows(table) shouldBe Set((1, "tom"))

    sink.writeIncremental(
      Seq((1, "tom")).toDF("id", "name"),
      SinkContext(Some(Map("table" -> table, "mode" -> "append"))),
      write)
    rows(table) shouldBe Set((1, "tom"))

    sink.writeIncremental(
      Seq((2, "jerry")).toDF("id", "name"),
      SinkContext(Some(Map("table" -> table, "mode" -> "append"))),
      write.copy(transactionId = "txn-iceberg-append-2"))
    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("reject an incremental Iceberg writer with an older fencing token") {
    val table = newTable("sink_fencing")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    sink.supportsIncrementalFencing shouldBe true
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "snapshot_id",
      transactionId = "txn-iceberg-fence-1",
      sourceId = "source-iceberg",
      fencingToken = 1L)
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "append")))

    sink.writeIncremental(Seq((1, "tom")).toDF("id", "name"), context, write)
    sink.writeIncremental(
      Seq((2, "jerry")).toDF("id", "name"),
      context,
      write.copy(transactionId = "txn-iceberg-fence-2", fencingToken = 2L))
    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))

    val error = intercept[PuddingException] {
      sink.writeIncremental(
        Seq((3, "spark")).toDF("id", "name"),
        context,
        write.copy(transactionId = "txn-iceberg-fence-3", fencingToken = 1L))
    }
    error.getMessage should include ("stale incremental writer")
    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))

    // Replaying the already committed transaction with the same generation is a no-op.
    sink.writeIncremental(
      Seq((2, "jerry")).toDF("id", "name"),
      context,
      write.copy(transactionId = "txn-iceberg-fence-2", fencingToken = 2L))
    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))

    // An equal generation with a different transaction belongs to a stale writer.
    val equal = intercept[PuddingException] {
      sink.writeIncremental(
        Seq((3, "spark")).toDF("id", "name"),
        context,
        write.copy(transactionId = "txn-iceberg-fence-equal", fencingToken = 2L))
    }
    equal.getMessage should include ("stale incremental writer")
    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("enforce fencing on incremental upsert merges") {
    val table = newTable("sink_merge_fencing")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "append")))
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Upsert,
      keyFields = Seq("id"),
      opColumn = "__op",
      fromCursor = Some("0"),
      toCursor = "1",
      cursorType = "snapshot_id",
      transactionId = "txn-iceberg-merge-1",
      sourceId = "source-iceberg-merge",
      fencingToken = 2L)

    sink.writeIncremental(Seq((1, "tom")).toDF("id", "name"), context, write)
    rows(table) shouldBe Set((1, "tom"))

    val error = intercept[PuddingException] {
      sink.writeIncremental(
        Seq((2, "jerry")).toDF("id", "name"),
        context,
        write.copy(transactionId = "txn-iceberg-merge-2", fencingToken = 1L))
    }
    error.getMessage should include ("stale incremental writer")
    rows(table) shouldBe Set((1, "tom"))

    // Equal generation is the same writer replaying an idempotent MERGE.
    sink.writeIncremental(
      Seq((1, "tom")).toDF("id", "name"),
      context,
      write.copy(transactionId = "txn-iceberg-merge-1", fencingToken = 2L))
    rows(table) shouldBe Set((1, "tom"))

    // Equal generation from a different transaction is stale.
    val equalDifferent = intercept[PuddingException] {
      sink.writeIncremental(
        Seq((3, "spark")).toDF("id", "name"),
        context,
        write.copy(transactionId = "txn-iceberg-merge-other", fencingToken = 2L))
    }
    equalDifferent.getMessage should include ("stale incremental writer")
    rows(table) shouldBe Set((1, "tom"))
  }

  test("qualify a table with a configured catalog") {
    IcebergSink.qualify("db.orders", "lake", "lake") shouldBe "lake.db.orders"
    IcebergSink.qualify("lake.db.orders", "lake", "lake") shouldBe "lake.db.orders"
    IcebergSink.qualify("db.orders", "spark_catalog", "hive_prod") shouldBe "db.orders"
    intercept[PuddingException] {
      IcebergSink.qualify("orders", "lake", "lake")
    }
  }

  test("create an iceberg table when it does not exist") {
    val table = newTable("sink_create")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val dataFrame = Seq((1, "tom"), (2, "jerry")).toDF("id", "name")

    new IcebergSink().writer(
      dataFrame,
      Some(Map("table" -> table, "mode" -> "overwrite")))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
    val provider = sparkSession.sql(s"describe extended $table")
      .collect()
      .find(row => row.getString(0) == "Provider")
      .map(_.getString(1))
    provider shouldBe Some("iceberg")
  }

  test("append to an existing iceberg table") {
    val table = newTable("sink_append")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()

    sink.writer(
      Seq((1, "tom")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(
      Seq((2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "append")))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("accept a case-insensitive save mode") {
    val table = newTable("sink_mode_case")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()

    sink.writer(
      Seq((1, "tom")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> " OVERWRITE ")))
    sink.writer(
      Seq((2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "Append")))

    rows(table) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("replace an Iceberg table through the full-refresh contract") {
    val table = newTable("sink_atomic_replace")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()
    val context = SinkContext(Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      context)
    sink.writeFullRefresh(
      Seq((9, "replacement")).toDF("id", "name"),
      context)

    rows(table) shouldBe Set((9, "replacement"))
    sink.supportsAtomicReplace shouldBe true
  }

  test("overwrite an existing iceberg table without changing its schema") {
    val table = newTable("sink_overwrite")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val sink = new IcebergSink()

    sink.writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))
    sink.writer(
      Seq((3, "spark")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))

    rows(table) shouldBe Set((3, "spark"))
  }

  test("partition a table created by the sink") {
    val table = newTable("sink_partitioned")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val dataFrame = Seq(
      (1, "tom", "2024-01-01"),
      (2, "jerry", "2024-01-02"),
      (3, "spark", "2024-01-01")
    ).toDF("id", "name", "dt")

    new IcebergSink().writer(dataFrame, Some(Map(
      "table" -> table,
      "mode" -> "overwrite",
      "partitionBy" -> Seq("dt"))))

    sparkSession.sql(s"select * from $table").count() shouldBe 3
    sparkSession.sql(s"select * from $table.partitions").count() shouldBe 2
  }

  test("ignore an existing table without writing") {
    val table = newTable("sink_ignore")
    sparkSession.sql(s"drop table if exists $table")
    sparkSession.sql(s"create table $table (id bigint, name string) using iceberg")
    val session = sparkSession
    import session.implicits._

    new IcebergSink().writer(
      Seq((1, "tom")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "ignore")))

    rows(table) shouldBe Set.empty
  }

  test("error mode fails when the table exists") {
    val table = newTable("sink_error")
    sparkSession.sql(s"drop table if exists $table")
    sparkSession.sql(s"create table $table (id bigint, name string) using iceberg")
    val session = sparkSession
    import session.implicits._

    intercept[PuddingException] {
      new IcebergSink().writer(
        Seq((1, "tom")).toDF("id", "name"),
        Some(Map("table" -> table, "mode" -> "error")))
    }
  }

  test("null config throws PuddingException") {
    val session = sparkSession
    import session.implicits._
    val dataFrame = Seq((1, "tom")).toDF("id", "name")

    intercept[PuddingException] {
      new IcebergSink().writer(dataFrame, None)
    }
  }

  test("missing table config throws PuddingException") {
    val session = sparkSession
    import session.implicits._
    val dataFrame = Seq((1, "tom")).toDF("id", "name")

    intercept[PuddingException] {
      new IcebergSink().writer(dataFrame, Some(Map("mode" -> "append")))
    }
  }

  test("unsupported mode throws PuddingException") {
    val table = newTable("sink_bad_mode")
    sparkSession.sql(s"drop table if exists $table")
    val session = sparkSession
    import session.implicits._
    val dataFrame = Seq((1, "tom")).toDF("id", "name")

    intercept[PuddingException] {
      new IcebergSink().writer(
        dataFrame,
        Some(Map("table" -> table, "mode" -> "bad")))
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (path != null && Files.exists(path)) {
      val paths = Files.walk(path)
      try {
        paths.sorted(Comparator.reverseOrder()).forEach(candidate => Files.delete(candidate))
      } finally {
        paths.close()
      }
    }
  }
}
