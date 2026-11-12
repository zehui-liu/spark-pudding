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

package org.pudding.sink

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => FilePath}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.table.HoodieTableMetaClient
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
 * Tests for Hudi path writes and write option validation.
 */
class HudiSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private var workspace: FilePath = _

  override def beforeAll(): Unit = {
    workspace = Files.createTempDirectory("hudi-sink-test")
    sparkSession = SparkSession.builder()
      .appName("test for hudi sink")
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
      (2, "jerry", "2026-01-02", 1L)
    ).toDF("id", "name", "dt", "ts")
  }

  private def readTable(path: FilePath): DataFrame = {
    sparkSession.read.format("org.apache.hudi").load(path.toString)
  }

  private def baseConfig(
      path: FilePath,
      operation: String,
      mode: String = "overwrite",
      recordKeys: Boolean = true): Map[String, Any] = {
    var config = Map[String, Any](
      "table" -> path.getFileName.toString,
      "tablePath" -> path.toString,
      "operation" -> operation,
      "tableType" -> "COPY_ON_WRITE",
      "partitionPathField" -> "dt",
      "preCombineField" -> "ts",
      "mode" -> mode)
    if (recordKeys) {
      config += "recordKeyFields" -> Seq("id")
    }
    config
  }

  test("skip empty streaming microbatches instead of failing Hudi metadata") {
    val stream = sparkSession.readStream.format("rate")
      .option("rowsPerSecond", 10)
      .option("numPartitions", 1)
      .load()
      .filter("value < 0")
    val path = workspace.resolve("orders_stream_empty")
    val checkpoint = workspace.resolve("orders_stream_empty_checkpoint").toString
    val config = baseConfig(path, "bulk_insert", mode = "append", recordKeys = false) ++ Map(
      "checkpointLocation" -> checkpoint,
      "outputMode" -> "append")

    noException should be thrownBy new HudiSink().writer(
      stream,
      SinkContext(
        Some(config),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))
    Files.exists(path.resolve(".hoodie")) shouldBe false
  }

  test("stream cdc operations into a Hudi table") {
    val path = workspace.resolve("orders_stream_cdc")
    val checkpoint = workspace.resolve("orders_stream_cdc_checkpoint")
    val input = workspace.resolve("orders_stream_cdc_input")
    Files.createDirectories(input)
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert", mode = "overwrite")))

    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","dt":"2026-01-01","ts":2,"__op":"U","_kafka_offset":1}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":2,"name":"jerry","dt":"2026-01-02","ts":3,"__op":"D","_kafka_offset":2}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("3.json"),
      """{"id":4,"name":"spark","dt":"2026-01-04","ts":4,"__op":"I","_kafka_offset":3}"""
        .getBytes(StandardCharsets.UTF_8))

    val stream = sparkSession.readStream
      .schema("id int, name string, dt string, ts long, __op string, _kafka_offset long")
      .json(input.toString)
    val config = baseConfig(path, "upsert", mode = "append") ++ Map(
      "checkpointLocation" -> checkpoint.toString,
      "outputMode" -> "append",
      "cdcOpColumn" -> "__op",
      "cdcOrderFields" -> Seq("_kafka_offset"))

    sink.writer(
      stream,
      SinkContext(
        Some(config),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    val result = readTable(path)
    result.count() shouldBe 2
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
    result.where("id = 2").count() shouldBe 0
    result.where("id = 4").select("name").collect().head.getString(0) shouldBe "spark"
  }

  test("upsert update output into a Hudi table") {
    val path = workspace.resolve("orders_stream_update")
    val checkpoint = workspace.resolve("orders_stream_update_checkpoint")
    val input = workspace.resolve("orders_stream_update_input")
    Files.createDirectories(input)
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert", mode = "overwrite")))

    Files.write(
      input.resolve("1.json"),
      """{"id":1,"name":"tom_v2","dt":"2026-01-01","ts":2}"""
        .getBytes(StandardCharsets.UTF_8))
    Files.write(
      input.resolve("2.json"),
      """{"id":4,"name":"spark","dt":"2026-01-04","ts":4}"""
        .getBytes(StandardCharsets.UTF_8))
    val stream = sparkSession.readStream
      .schema("id int, name string, dt string, ts long")
      .json(input.toString)
      .groupBy("id", "dt")
      .agg(max("name").as("name"), max("ts").as("ts"))
    val config = baseConfig(path, "upsert", mode = "append") ++ Map(
      "checkpointLocation" -> checkpoint.toString,
      "outputMode" -> "update")

    sink.writer(
      stream,
      SinkContext(
        Some(config),
        executionMode = Some(ExecutionMode.Streaming),
        trigger = Some("available-now")))

    val result = readTable(path)
    result.count() shouldBe 3
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
    result.where("id = 2").select("name").collect().head.getString(0) shouldBe "jerry"
    result.where("id = 4").select("name").collect().head.getString(0) shouldBe "spark"
  }

  test("reject invalid Hudi streaming options") {
    val stream = sparkSession.readStream.format("rate").load()
    val path = workspace.resolve("orders_stream_options")
    val checkpoint = workspace.resolve("orders_stream_options_checkpoint").toString
    val config = baseConfig(path, "upsert", mode = "append") ++ Map(
      "checkpointLocation" -> checkpoint,
      "outputMode" -> "complete")
    val outputMode = intercept[PuddingException] {
      new HudiSink().writer(
        stream,
        SinkContext(
          Some(config),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    }
    outputMode.getMessage should include ("does not support streaming outputMode=complete")

    val missingCheckpoint = intercept[PuddingException] {
      new HudiSink().writer(
        stream,
        SinkContext(
          Some(baseConfig(path, "upsert", mode = "append")),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    }
    missingCheckpoint.getMessage should include ("checkpointLocation")
  }

  test("declare lakehouse incremental and full-refresh capabilities") {
    val sink = new HudiSink()
    sink.supportedIncrementalSemantics shouldBe Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)
    sink.isIdempotentIncremental(IncrementalSemantics.Append) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Upsert) shouldBe true
    sink.isIdempotentIncremental(IncrementalSemantics.Cdc) shouldBe true
    sink.supportsFullRefresh shouldBe true
    sink.supportsAtomicReplace shouldBe true
    sink.supportedStreamingOutputModes shouldBe Set("append", "update")
  }

  test("replace a Hudi table through the full-refresh contract") {
    val path = workspace.resolve("orders_atomic_replace")
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert", mode = "overwrite")))
    val session = sparkSession
    import session.implicits._

    sink.writeFullRefresh(
      Seq((9, "replacement", "2026-03-01", 9L)).toDF("id", "name", "dt", "ts"),
      SinkContext(Some(baseConfig(path, "insert", mode = "overwrite"))))

    readTable(path).select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((9, "replacement"))
    sink.supportsAtomicReplace shouldBe true

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(sparkSession.sessionState.newHadoopConf())
      .setBasePath(path.toString)
      .build()
    val latest = metaClient.getActiveTimeline
      .getCommitsTimeline
      .filterCompletedInstants
      .lastInstant()
      .get()
    latest.getAction shouldBe "replacecommit"
  }

  test("create a missing Hudi table through the full-refresh contract") {
    val path = workspace.resolve("orders_atomic_create")
    val sink = new HudiSink()

    sink.writeFullRefresh(
      orders(),
      SinkContext(Some(baseConfig(path, "insert", mode = "overwrite"))))

    readTable(path).count() shouldBe 2
  }

  test("replace a MERGE_ON_READ Hudi table through the full-refresh contract") {
    val path = workspace.resolve("orders_atomic_replace_mor")
    val sink = new HudiSink()
    val morConfig = baseConfig(path, "insert", mode = "overwrite") ++
      Map("tableType" -> "MERGE_ON_READ")
    sink.writer(orders(), Some(morConfig))
    val session = sparkSession
    import session.implicits._

    sink.writeFullRefresh(
      Seq((9, "replacement", "2026-03-01", 9L)).toDF("id", "name", "dt", "ts"),
      SinkContext(Some(morConfig)))

    readTable(path).select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((9, "replacement"))
  }

  test("keep the old Hudi snapshot when a full refresh fails") {
    val path = workspace.resolve("orders_atomic_replace_failure")
    val sink = new HudiSink()
    val config = baseConfig(path, "insert", mode = "overwrite")
    sink.writer(orders(), Some(config))
    val session = sparkSession
    import session.implicits._

    intercept[Exception] {
      sink.writeFullRefresh(
        Seq((9, 9)).toDF("id", "bad"),
        SinkContext(Some(config)))
    }

    readTable(path).select("id").collect().map(_.getInt(0)).toSet shouldBe Set(1, 2)
  }

  test("write a Hudi table by path") {
    val path = workspace.resolve("orders_write")
    new HudiSink().writer(orders(), Some(baseConfig(path, "insert")))

    val result = readTable(path)
    result.count() shouldBe 2
    result.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((1, "tom"), (2, "jerry"))
  }

  test("upsert a Hudi table by record key") {
    val path = workspace.resolve("orders_upsert")
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert")))

    val session = sparkSession
    import session.implicits._
    sink.writer(
      Seq((1, "tom_v2", "2026-01-01", 2L)).toDF("id", "name", "dt", "ts"),
      Some(baseConfig(path, "upsert", mode = "append")))

    val result = readTable(path)
    result.count() shouldBe 2
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
  }

  test("skip an append transaction that is already in Hudi commit metadata") {
    val path = workspace.resolve("orders_transaction")
    val sink = new HudiSink()
    val config = baseConfig(path, "insert", mode = "append", recordKeys = false)
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "commit_time",
      transactionId = "txn-hudi-append")

    sink.writeIncremental(orders(), SinkContext(Some(config)), write)
    readTable(path).count() shouldBe 2

    sink.writeIncremental(orders(), SinkContext(Some(config)), write)
    readTable(path).count() shouldBe 2

    sink.writeIncremental(
      orders(),
      SinkContext(Some(config)),
      write.copy(transactionId = "txn-hudi-append-2"))
    readTable(path).count() shouldBe 4
  }

  test("write an append transaction that is not in the retained timeline") {
    val path = workspace.resolve("orders_transaction_missing")
    val sink = new HudiSink()
    val config = baseConfig(path, "insert", mode = "append", recordKeys = false)
    val initial = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "commit_time",
      transactionId = "txn-hudi-initial")
    sink.writeIncremental(orders(), SinkContext(Some(config)), initial)

    // A new batch also has a cursor, so absence from the timeline must not be treated as a
    // failed replay: only a transaction found in the active or archived timeline is skipped.
    val next = initial.copy(
      fromCursor = Some("1"),
      toCursor = "2",
      transactionId = "txn-hudi-new-batch")
    sink.writeIncremental(orders(), SinkContext(Some(config)), next)

    readTable(path).count() shouldBe 4
  }

  test("skip an append transaction that was archived from the active timeline") {
    val path = workspace.resolve("orders_transaction_archived")
    val sink = new HudiSink()
    val config = baseConfig(path, "insert", mode = "append", recordKeys = false) +
      ("options" -> Map(
        "hoodie.keep.min.commits" -> "2",
        "hoodie.keep.max.commits" -> "3"))
    val writes = (0 until 6).map { index =>
      IncrementalWrite(
        semantics = IncrementalSemantics.Append,
        keyFields = Seq.empty,
        opColumn = "__op",
        fromCursor = if (index == 0) None else Some(index.toString),
        toCursor = (index + 1).toString,
        cursorType = "commit_time",
        transactionId = s"txn-hudi-archive-$index")
    }
    writes.foreach(write => sink.writeIncremental(orders(), SinkContext(Some(config)), write))
    val before = readTable(path).count()
    before shouldBe 12

    // The oldest transaction has been archived by the retention window. The replay must still
    // be recognized through the archived timeline and must not append a duplicate batch. The
    // cursor is deliberately not a Hudi instant: a source cursor must not bound the scan.
    sink.writeIncremental(
      orders(),
      SinkContext(Some(config)),
      writes.head.copy(fromCursor = Some("9223372036854775807")))

    readTable(path).count() shouldBe before
  }

  test("reject an incremental writer with an older fencing token") {
    val path = workspace.resolve("orders_fencing")
    val sink = new HudiSink()
    sink.supportsIncrementalFencing shouldBe true
    val config = baseConfig(path, "insert", mode = "append", recordKeys = false)
    val write = IncrementalWrite(
      semantics = IncrementalSemantics.Append,
      keyFields = Seq.empty,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "1",
      cursorType = "commit_time",
      transactionId = "txn-hudi-fence-1",
      sourceId = "source-hudi",
      fencingToken = 1L)

    sink.writeIncremental(orders(), SinkContext(Some(config)), write)
    sink.writeIncremental(
      orders(),
      SinkContext(Some(config)),
      write.copy(transactionId = "txn-hudi-fence-2", fencingToken = 2L))
    readTable(path).count() shouldBe 4

    val error = intercept[PuddingException] {
      sink.writeIncremental(
        orders(),
        SinkContext(Some(config)),
        write.copy(transactionId = "txn-hudi-fence-3", fencingToken = 1L))
    }
    error.getMessage should include ("stale incremental writer")
    error.getMessage should include ("fencing token 1")
    readTable(path).count() shouldBe 4

    // Replaying the already committed transaction with the same generation must stay a no-op.
    sink.writeIncremental(
      orders(),
      SinkContext(Some(config)),
      write.copy(transactionId = "txn-hudi-fence-2", fencingToken = 2L))
    readTable(path).count() shouldBe 4

    // An equal generation with a different transaction belongs to a stale writer.
    val equal = intercept[PuddingException] {
      sink.writeIncremental(
        orders(),
        SinkContext(Some(config)),
        write.copy(transactionId = "txn-hudi-fence-equal", fencingToken = 2L))
    }
    equal.getMessage should include ("stale incremental writer")
    readTable(path).count() shouldBe 4
  }

  test("apply upsert incremental semantics as a Hudi upsert") {
    val path = workspace.resolve("orders_incremental_upsert")
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert")))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", 2L),
        (3, "spark", "2026-01-03", 1L)
      ).toDF("id", "name", "dt", "ts"),
      SinkContext(Some(baseConfig(path, "insert", mode = "append"))),
      IncrementalWrite(
        semantics = IncrementalSemantics.Upsert,
        keyFields = Seq("id"),
        opColumn = "__op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "commit_time"))

    val result = readTable(path)
    result.count() shouldBe 3
    result.where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"
  }

  test("apply cdc incremental semantics as deletes and upserts") {
    val path = workspace.resolve("orders_incremental_cdc")
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert")))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", 2L, "U"),
        (2, "jerry", "2026-01-02", 1L, "D")
      ).toDF("id", "name", "dt", "ts", "op"),
      SinkContext(Some(baseConfig(path, "insert", mode = "append"))),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "commit_time"))

    val result = readTable(path)
    result.count() shouldBe 1
    result.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe Set((1, "tom_v2"))
    result.columns.contains("op") shouldBe false
  }

  test("keep only the last cdc operation per key when ordering is available") {
    val path = workspace.resolve("orders_incremental_cdc_ordered")
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert")))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", 2L, "I", 1L),
        (1, "tom_v2", "2026-01-01", 3L, "D", 2L)
      ).toDF("id", "name", "dt", "ts", "op", "change_order"),
      SinkContext(Some(baseConfig(path, "insert", mode = "append"))),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "commit_time",
        orderFields = Seq("change_order")))

    val result = readTable(path)
    result.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2)
    result.columns.contains("change_order") shouldBe false
  }

  test("order cdc changes using the sink cdcOrderFields") {
    val path = workspace.resolve("orders_incremental_cdc_config_order")
    val sink = new HudiSink()
    sink.writer(orders(), Some(baseConfig(path, "insert")))

    val session = sparkSession
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", 2L, "I", 1L),
        (1, "tom_v2", "2026-01-01", 3L, "D", 2L)
      ).toDF("id", "name", "dt", "ts", "op", "change_order"),
      SinkContext(Some(baseConfig(path, "insert", mode = "append") ++
        Map("cdcOrderFields" -> Seq("change_order")))),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "commit_time"))

    val result = readTable(path)
    result.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2)
    result.columns.contains("change_order") shouldBe false
  }

  test("reject an unknown cdc operation before writing") {
    val path = workspace.resolve("orders_incremental_bad_op")
    val session = sparkSession
    import session.implicits._
    val error = intercept[PuddingException] {
      new HudiSink().writeIncremental(
        Seq((1, "tom", "2026-01-01", 1L, "X")).toDF("id", "name", "dt", "ts", "op"),
        SinkContext(Some(baseConfig(path, "insert", mode = "append"))),
        IncrementalWrite(
          semantics = IncrementalSemantics.Cdc,
          keyFields = Seq("id"),
          opColumn = "op",
          fromCursor = None,
          toCursor = "2",
          cursorType = "commit_time"))
    }
    error.getMessage should include ("must contain I, U or D")
  }

  test("reject streaming incremental writes") {
    val path = workspace.resolve("orders_incremental_stream")
    val error = intercept[PuddingException] {
      new HudiSink().writeIncremental(
        orders(),
        SinkContext(Some(baseConfig(path, "insert", mode = "append") ++ Map(
          "isStreaming" -> false,
          "streamWrite" -> true))),
        IncrementalWrite(
          semantics = IncrementalSemantics.Upsert,
          keyFields = Seq("id"),
          opColumn = "__op",
          fromCursor = None,
          toCursor = "2",
          cursorType = "commit_time"))
    }
    error.getMessage should include ("does not support streaming incremental writes")
  }

  test("require a table path when no catalog is configured") {
    val error = intercept[PuddingException] {
      new HudiSink().writer(orders(), Some(Map(
        "table" -> "orders",
        "operation" -> "insert")))
    }
    error.getMessage should include ("tablePath is required")
  }

  test("upsert requires record key fields") {
    val path = workspace.resolve("orders_no_keys")
    val error = intercept[PuddingException] {
      new HudiSink().writer(
        orders(),
        Some(baseConfig(path, "upsert", recordKeys = false)))
    }
    error.getMessage should include ("recordKeyFields")
  }

  test("reject an unsupported table type") {
    val path = workspace.resolve("orders_bad_type")
    val config = baseConfig(path, "insert") + ("tableType" -> "ROW_STORE")
    val error = intercept[PuddingException] {
      new HudiSink().writer(orders(), Some(config))
    }
    error.getMessage should include ("unsupported hudi tableType")
  }

  test("reject a non-Hudi catalog") {
    val path = workspace.resolve("orders_catalog_type")
    val catalog = ResolvedCatalog(
      "lake",
      "iceberg",
      "lake",
      sessionCatalog = false,
      sparkConf = Map.empty,
      properties = Map.empty)
    val error = intercept[PuddingException] {
      new HudiSink().writer(
        orders(),
        SinkContext(Some(baseConfig(path, "insert")), catalog = Some(catalog)))
    }
    error.getMessage should include ("requires a hudi catalog")
  }

  test("write a Hudi table through a catalog") {
    withCatalogSession { (session, tableName) =>
      val catalog = ResolvedCatalog(
        "hudi_prod",
        "hudi",
        "spark_catalog",
        sessionCatalog = true,
        sparkConf = Map.empty,
        properties = Map.empty)
      val config = Map[String, Any](
        "table" -> s"default.$tableName",
        "operation" -> "insert",
        "recordKeyFields" -> Seq("id"),
        "partitionBy" -> Seq("dt"),
        "preCombineField" -> "ts",
        "tableType" -> "COPY_ON_WRITE",
        "mode" -> "overwrite")

      val currentSession = session
      import currentSession.implicits._
      new HudiSink().writer(
        Seq(
          (1, "tom", "2026-01-01", 1L),
          (2, "jerry", "2026-01-02", 1L)
        ).toDF("id", "name", "dt", "ts"),
        SinkContext(Some(config), catalog = Some(catalog)))

      val result = session.table(s"default.$tableName")
      result.count() shouldBe 2
      result.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(1, 2)

      val appendInput = workspace.resolve("hudi_catalog_append_input")
      Files.createDirectories(appendInput)
      Files.write(
        appendInput.resolve("1.json"),
        """{"id":3,"name":"spark","dt":"2026-01-03","ts":3}"""
          .getBytes(StandardCharsets.UTF_8))
      val appendConfig = Map[String, Any](
        "table" -> s"default.$tableName",
        "operation" -> "upsert",
        "recordKeyFields" -> Seq("id"),
        "partitionBy" -> Seq("dt"),
        "preCombineField" -> "ts",
        "tableType" -> "COPY_ON_WRITE",
        "checkpointLocation" -> workspace.resolve("hudi_catalog_append_checkpoint").toString,
        "outputMode" -> "append")
      val appendStream = session.readStream
        .schema("id int, name string, dt string, ts long")
        .json(appendInput.toString)
      new HudiSink().writer(
        appendStream,
        SinkContext(
          Some(appendConfig),
          catalog = Some(catalog),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
      session.catalog.refreshTable(s"default.$tableName")
      session.table(s"default.$tableName").count() shouldBe 3

      val updateInput = workspace.resolve("hudi_catalog_update_input")
      Files.createDirectories(updateInput)
      Files.write(
        updateInput.resolve("1.json"),
        """{"id":1,"name":"tom_v2","dt":"2026-01-01","ts":4}"""
          .getBytes(StandardCharsets.UTF_8))
      val updateConfig = appendConfig ++ Map(
        "checkpointLocation" -> workspace.resolve("hudi_catalog_update_checkpoint").toString,
        "outputMode" -> "update")
      val updateStream = session.readStream
        .schema("id int, name string, dt string, ts long")
        .json(updateInput.toString)
        .groupBy("id", "dt")
        .agg(max("name").as("name"), max("ts").as("ts"))
      new HudiSink().writer(
        updateStream,
        SinkContext(
          Some(updateConfig),
          catalog = Some(catalog),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
      session.catalog.refreshTable(s"default.$tableName")
      session.table(s"default.$tableName")
        .where("id = 1").select("name").collect().head.getString(0) shouldBe "tom_v2"

      new HudiSink().writeFullRefresh(
        Seq((9, "replacement", "2026-03-01", 9L)).toDF("id", "name", "dt", "ts"),
        SinkContext(
          Some(Map[String, Any](
            "table" -> s"default.$tableName",
            "recordKeyFields" -> Seq("id"),
            "partitionBy" -> Seq("dt"),
            "preCombineField" -> "ts",
            "tableType" -> "COPY_ON_WRITE",
            "mode" -> "overwrite")),
          catalog = Some(catalog)))
      session.catalog.refreshTable(s"default.$tableName")
      session.table(s"default.$tableName").select("id").collect()
        .map(_.getInt(0)).toSet shouldBe Set(9)

      val createdTable = s"${tableName}_created"
      new HudiSink().writeFullRefresh(
        Seq(
          (1, "tom", "2026-01-01", 1L),
          (2, "jerry", "2026-01-02", 1L)
        ).toDF("id", "name", "dt", "ts"),
        SinkContext(
          Some(Map[String, Any](
            "table" -> s"default.$createdTable",
            "recordKeyFields" -> Seq("id"),
            "partitionBy" -> Seq("dt"),
            "preCombineField" -> "ts",
            "tableType" -> "COPY_ON_WRITE",
            "mode" -> "overwrite")),
          catalog = Some(catalog)))
      session.catalog.refreshTable(s"default.$createdTable")
      session.table(s"default.$createdTable").count() shouldBe 2
      session.sql(s"DROP TABLE IF EXISTS default.$createdTable")
    }
  }

  private def withCatalogSession(testBody: (SparkSession, String) => Unit): Unit = {
    val warehouse = Files.createTempDirectory("hudi-catalog-warehouse")
    val metastore = Files.createTempDirectory("hudi-catalog-metastore")
    val tableName = s"hudi_catalog_orders_${System.nanoTime()}"
    var session: SparkSession = null
    try {
      session = try {
        SparkSession.builder()
          .appName("test for hudi catalog sink")
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
