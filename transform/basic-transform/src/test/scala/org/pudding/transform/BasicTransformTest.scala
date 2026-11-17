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

package org.pudding.transform

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.DataTransform
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for filter, aggregate, union, column, limit and join transforms.
 */
class BasicTransformTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("test for basic transform")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
    }
  }

  private def orders(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1001, 1, 10.0, "beijing"),
      (1002, 2, 20.0, "shanghai"),
      (1003, 1, 30.0, "beijing"),
      (1004, 3, 99.0, "beijing"),
      (1005, 4, 5.0, "shenzhen")
    ).toDF("order_id", "user_id", "amount", "order_city")
  }

  private def users(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "beijing", 20),
      (2, "jerry", "shanghai", 25),
      (3, "kid", "beijing", 16),
      (5, "ghost", "hangzhou", 40)
    ).toDF("user_id", "name", "city", "age")
  }

  private def pairRows(dataFrame: DataFrame): Set[(Int, String)] = {
    dataFrame.collect().map(row => (row.getInt(0), row.getString(1))).toSet
  }

  private def toDf[A <: Product : scala.reflect.runtime.universe.TypeTag](
      values: Seq[A],
      columns: String*): DataFrame = {
    val session = sparkSession
    import session.implicits._
    values.toDF(columns: _*)
  }

  private def writeJson(directory: Path, name: String, lines: String*): Unit = {
    Files.write(
      directory.resolve(name),
      lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
  }

  private def orderStream(directory: Path): DataFrame = {
    sparkSession.readStream
      .schema("event_time timestamp, order_id int, order_city string, amount double")
      .json(directory.toString)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val children = Files.list(path)
      try children.iterator().asScala.foreach(this.deleteRecursively)
      finally children.close()
    }
    Files.deleteIfExists(path)
  }

  test("register all basic transform identifiers") {
    DataTransform.identifiers should contain allOf (
      "filter-transform",
      "aggregate-transform",
      "union-transform",
      "column-transform",
      "limit-transform",
      "join-transform",
      "validate-transform",
      "dedup-transform",
      "schema-contract-transform")
  }

  test("filter rows with a SQL condition") {
    val result = new FilterTransform().handle(
      Map("source-1" -> orders()),
      Some(Map("condition" -> "amount >= 20", "numPartitions" -> "3")))

    result.count() shouldBe 3
    result.rdd.getNumPartitions shouldBe 3
    result.select("order_id").collect().map(_.getInt(0)).toSet shouldBe
      Set(1002, 1003, 1004)
  }

  test("filter requires one dependency and a condition") {
    intercept[PuddingException] {
      new FilterTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map("condition" -> "amount >= 20")))
    }

    intercept[PuddingException] {
      new FilterTransform().handle(Map("source-1" -> orders()), Some(Map.empty))
    }

    intercept[PuddingException] {
      new FilterTransform().handle(
        Map("source-1" -> orders()), Some(Map("condition" -> " ")))
    }
  }

  test("aggregate by a group column") {
    val result = new AggregateTransform().handle(
      Map("source-1" -> orders()),
      Some(Map(
        "groupBy" -> Seq("order_city"),
        "aggregations" -> Map(
          "order_count" -> "count(*)",
          "total_amount" -> "sum(amount)"))))

    val rows = result.select("order_city", "order_count", "total_amount")
      .collect()
      .map(row => (row.getString(0), row.getLong(1), row.getDouble(2)))
      .toSet

    rows shouldBe Set(
      ("beijing", 3L, 139.0),
      ("shanghai", 1L, 20.0),
      ("shenzhen", 1L, 5.0))
  }

  test("aggregate globally with an ordered list of expressions") {
    val result = new AggregateTransform().handle(
      Map("source-1" -> orders()),
      Some(Map("aggregations" -> Seq(
        Map("name" -> "row_count", "expr" -> "count(*)"),
        Map("name" -> "total", "expr" -> "sum(amount)")))))

    result.columns.toSeq shouldBe Seq("row_count", "total")
    result.collect().head.getLong(0) shouldBe 5L
    result.collect().head.getDouble(1) shouldBe 164.0
  }

  test("aggregate accepts a comma-separated groupBy") {
    val result = new AggregateTransform().handle(
      Map("source-1" -> orders()),
      Some(Map(
        "groupBy" -> "user_id,order_city",
        "aggregations" -> Map("order_count" -> "count(*)"))))

    result.count() shouldBe 4
  }

  test("aggregate rejects missing and invalid expressions") {
    intercept[PuddingException] {
      new AggregateTransform().handle(Map("source-1" -> orders()), Some(Map.empty))
    }

    intercept[PuddingException] {
      new AggregateTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("aggregations" -> Seq(
          Map("name" -> "same", "expr" -> "count(*)"),
          Map("name" -> "same", "expr" -> "sum(amount)")))))
    }

    intercept[Exception] {
      new AggregateTransform().handle(
        Map("source-1" -> orders()),
        Some(Map(
          "groupBy" -> Seq("order_city"),
          "aggregations" -> Map("bad" -> "not_a_real_aggregate(amount)"))))
        .collect()
    }
  }

  test("aggregate streaming rows into bounded event-time windows") {
    val input = Files.createTempDirectory("pudding-stream-aggregate-input")
    val checkpoint = Files.createTempDirectory("pudding-stream-aggregate-checkpoint")
    val queryName = "stream_aggregate_" + UUID.randomUUID().toString.replace("-", "")
    try {
      this.writeJson(input, "orders.json",
        """{"event_time":"2024-01-01T00:00:10","order_id":1,"order_city":"bj","amount":10.0}""",
        """{"event_time":"2024-01-01T00:00:12","order_id":2,"order_city":"bj","amount":5.0}""",
        """{"event_time":"2024-01-01T00:00:14","order_id":3,"order_city":"sh","amount":7.0}""",
        """{"event_time":"2024-01-01T00:01:00","order_id":4,"order_city":"bj","amount":1.0}""")
      val result = new AggregateTransform().handle(
        Map("source-1" -> this.orderStream(input)),
        Some(Map(
          "eventTime" -> "event_time",
          "watermark" -> "10 seconds",
          "window" -> "10 seconds",
          "groupBy" -> Seq("order_city"),
          "aggregations" -> Map(
            "order_count" -> "count(*)",
            "total_amount" -> "sum(amount)"))))

      result.isStreaming shouldBe true
      result.columns.toSeq shouldBe Seq(
        "window_start", "window_end", "order_city", "order_count", "total_amount")

      val query = result.writeStream
        .format("memory")
        .queryName(queryName)
        .option("checkpointLocation", checkpoint.toString)
        .outputMode("append")
        .start()
      try {
        query.processAllAvailable()
        val rows = sparkSession.table(queryName).collect().map { row =>
          (row.getString(2), row.getLong(3), row.getDouble(4))
        }.toSet
        // The 00:01:00 window is still open: the watermark is 00:00:50. Append mode emits only
        // windows whose end has passed the watermark.
        rows shouldBe Set(("bj", 2L, 15.0), ("sh", 1L, 7.0))
      } finally {
        query.stop()
      }
    } finally {
      this.deleteRecursively(input)
      this.deleteRecursively(checkpoint)
    }
  }

  test("streaming aggregate requires eventTime, watermark and a bounded window") {
    val stream = sparkSession.readStream.format("rate").load()

    val missingEventTime = intercept[PuddingException] {
      new AggregateTransform().handle(
        Map("source-1" -> stream),
        Some(Map(
          "groupBy" -> Seq("value"),
          "aggregations" -> Map("n" -> "count(*)"))))
    }
    missingEventTime.getMessage should include ("eventTime")

    val missingWatermark = intercept[PuddingException] {
      new AggregateTransform().handle(
        Map("source-1" -> stream),
        Some(Map(
          "eventTime" -> "timestamp",
          "window" -> "5 seconds",
          "aggregations" -> Map("n" -> "count(*)"))))
    }
    missingWatermark.getMessage should include ("watermark")

    val missingWindow = intercept[PuddingException] {
      new AggregateTransform().handle(
        Map("source-1" -> stream),
        Some(Map(
          "eventTime" -> "timestamp",
          "watermark" -> "10 seconds",
          "aggregations" -> Map("n" -> "count(*)"))))
    }
    missingWindow.getMessage should include ("window")
  }

  test("aggregate accepts a streaming window duration and slide map") {
    new AggregateTransform().validateConfig(
      Some(Map(
        "eventTime" -> "timestamp",
        "watermark" -> "10 seconds",
        "window" -> Map("duration" -> "10 seconds", "slide" -> "5 seconds"),
        "aggregations" -> Map("n" -> "count(*)"))),
      ExecutionMode.Streaming)

    val error = intercept[PuddingException] {
      new AggregateTransform().validateConfig(
        Some(Map(
          "eventTime" -> "timestamp",
          "watermark" -> "10 seconds",
          "window" -> Map("duration" -> "10 seconds", "size" -> "5 seconds"),
          "aggregations" -> Map("n" -> "count(*)"))),
        ExecutionMode.Streaming)
    }
    error.getMessage should include ("unknown keys")
  }

  test("aggregate rejects streaming state settings in batch execution") {
    val error = intercept[PuddingException] {
      new AggregateTransform().handle(
        Map("source-1" -> orders()),
        Some(Map(
          "eventTime" -> "event_time",
          "watermark" -> "10 seconds",
          "aggregations" -> Map("n" -> "count(*)"))))
    }
    error.getMessage should include ("can only be used in streaming")
  }

  test("streaming aggregate rejects a non-timestamp event-time column") {
    val stream = sparkSession.readStream.format("rate").load()
      .selectExpr("cast(value as string) as event_time", "value")
    val error = intercept[PuddingException] {
      new AggregateTransform().handle(
        Map("source-1" -> stream),
        Some(Map(
          "eventTime" -> "event_time",
          "watermark" -> "10 seconds",
          "window" -> "10 seconds",
          "aggregations" -> Map("n" -> "count(*)"))))
    }
    error.getMessage should include ("must be a timestamp")
  }

  test("deduplicate batch rows by key") {
    val result = new DedupTransform().handle(
      Map("source-1" -> this.toDf(Seq((1, "a"), (1, "b"), (2, "c")), "id", "value")),
      Some(Map("keys" -> Seq("id"))))

    result.count() shouldBe 2
    result.select("id").distinct().count() shouldBe 2
  }

  test("dedup does not preserve incremental operation ordering") {
    new DedupTransform().preservesIncrementalMetadata shouldBe false
  }

  test("deduplicate streaming rows with a bounded watermark") {
    val input = Files.createTempDirectory("pudding-stream-dedup-input")
    val checkpoint = Files.createTempDirectory("pudding-stream-dedup-checkpoint")
    val queryName = "stream_dedup_" + UUID.randomUUID().toString.replace("-", "")
    try {
      this.writeJson(input, "orders.json",
        """{"event_time":"2024-01-01T00:00:10","order_id":1,"order_city":"bj","amount":10.0}""",
        """{"event_time":"2024-01-01T00:00:11","order_id":1,"order_city":"bj","amount":10.0}""",
        """{"event_time":"2024-01-01T00:00:12","order_id":2,"order_city":"sh","amount":7.0}""")
      val result = new DedupTransform().handle(
        Map("source-1" -> this.orderStream(input)),
        Some(Map(
          "keys" -> Seq("order_id"),
          "eventTime" -> "event_time",
          "watermark" -> "10 minutes")))

      result.isStreaming shouldBe true
      result.columns.toSeq shouldBe Seq("event_time", "order_id", "order_city", "amount")

      val query = result.writeStream
        .format("memory")
        .queryName(queryName)
        .option("checkpointLocation", checkpoint.toString)
        .start()
      try {
        query.processAllAvailable()
        val ids = sparkSession.table(queryName).collect()
          .map(_.getInt(1)).sorted.toSeq
        ids shouldBe Seq(1, 2)
      } finally {
        query.stop()
      }
    } finally {
      this.deleteRecursively(input)
      this.deleteRecursively(checkpoint)
    }
  }

  test("streaming deduplication requires keys, eventTime and a watermark") {
    val stream = sparkSession.readStream.format("rate").load()

    val missingKeys = intercept[PuddingException] {
      new DedupTransform().handle(
        Map("source-1" -> orders()), Some(Map.empty))
    }
    missingKeys.getMessage should include ("requires keys")

    val missingEventTime = intercept[PuddingException] {
      new DedupTransform().handle(
        Map("source-1" -> stream), Some(Map("keys" -> Seq("value"))))
    }
    missingEventTime.getMessage should include ("eventTime")

    val missingWatermark = intercept[PuddingException] {
      new DedupTransform().handle(
        Map("source-1" -> stream),
        Some(Map("keys" -> Seq("value"), "eventTime" -> "timestamp")))
    }
    missingWatermark.getMessage should include ("watermark")

    val batchState = intercept[PuddingException] {
      new DedupTransform().handle(
        Map("source-1" -> orders()),
        Some(Map(
          "keys" -> Seq("order_id"),
          "eventTime" -> "event_time",
          "watermark" -> "10 minutes")))
    }
    batchState.getMessage should include ("can only be used in streaming")

    val duplicateKeys = intercept[PuddingException] {
      new DedupTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("keys" -> Seq("order_id", "ORDER_ID"))))
    }
    duplicateKeys.getMessage should include ("duplicate columns")
  }

  test("enforce a schema contract without changing the data") {
    val result = new SchemaContractTransform().handle(
      Map("source-1" -> orders()),
      Some(Map(
        "columns" -> Map(
          "order_id" -> "int",
          "amount" -> "double",
          "order_city" -> "string",
          "user_id" -> "int"))))
    result.count() shouldBe 5

    val extra = intercept[PuddingException] {
      new SchemaContractTransform().handle(
        Map("source-1" -> orders()),
        Some(Map(
          "columns" -> Map("order_id" -> "int"),
          "allowExtraColumns" -> false)))
    }
    extra.getMessage should include ("unexpected columns")

    val missing = intercept[PuddingException] {
      new SchemaContractTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("columns" -> Map("order_id" -> "int", "missing" -> "string"))))
    }
    missing.getMessage should include ("missing columns")

    val mismatch = intercept[PuddingException] {
      new SchemaContractTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("columns" -> Map("order_id" -> "string"))))
    }
    mismatch.getMessage should include ("type mismatches")
  }

  test("allow extra columns when configured") {
    val result = new SchemaContractTransform().handle(
      Map("source-1" -> orders()),
      Some(Map(
        "columns" -> Map("order_id" -> "int"),
        "allowExtraColumns" -> true)))
    result.count() shouldBe 5
  }

  test("enforce a schema contract on a streaming dependency") {
    val stream = sparkSession.readStream.format("rate").load()
    val result = new SchemaContractTransform().handle(
      Map("source-1" -> stream),
      Some(Map("columns" -> Map("timestamp" -> "timestamp", "value" -> "long"))))

    result.isStreaming shouldBe true
    result.columns.toSeq shouldBe Seq("timestamp", "value")
  }

  test("validate rows, quarantine invalid rows and drop them") {
    val directory = Files.createTempDirectory("pudding-validation")
    val quarantine = directory.resolve("invalid.json")
    try {
      val result = new ValidateTransform().handle(
        Map("source-1" -> orders()),
        Some(Map(
          "requiredColumns" -> Seq("order_id", "amount"),
          "rules" -> Map("positive_amount" -> "amount > 10"),
          "mode" -> "drop",
          "quarantinePath" -> quarantine.toString)))

      result.count() shouldBe 3
      sparkSession.read.json(quarantine.toString).count() shouldBe 2
    } finally {
      val stream = Files.walk(directory)
      try stream.iterator().asScala.toVector.reverse.foreach(path => Files.deleteIfExists(path))
      finally stream.close()
    }
  }

  test("validate rows can fail a batch on invalid data") {
    val error = intercept[PuddingException] {
      new ValidateTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("condition" -> "amount > 10", "mode" -> "error")))
    }
    error.getMessage should include ("rejected 2 row")
  }

  test("validate rows supports streaming drop mode") {
    val stream = sparkSession.readStream.format("rate").load()
    val result = new ValidateTransform().handle(
      Map("source-1" -> stream),
      Some(Map("condition" -> "value >= 0", "mode" -> "drop")))

    result.isStreaming shouldBe true
    intercept[PuddingException] {
      new ValidateTransform().handle(
        Map("source-1" -> stream),
        Some(Map("condition" -> "value >= 0", "mode" -> "error")))
    }
  }

  test("validate rows reject missing required columns") {
    intercept[PuddingException] {
      new ValidateTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("requiredColumns" -> Seq("missing"), "condition" -> "true")))
    }.getMessage should include ("required columns are missing")
  }

  test("union by name aligns columns regardless of order") {
    val left = toDf(Seq((1, "a"), (2, "b")), "id", "name")
    val right = toDf(Seq(("c", 3), ("d", 2)), "name", "id")

    val result = new UnionTransform().handle(
      Map("source-1" -> left, "source-2" -> right),
      Some(Map("inputs" -> Seq("source-1", "source-2"), "mode" -> "byName")))

    result.columns.toSeq shouldBe Seq("id", "name")
    pairRows(result) shouldBe Set((1, "a"), (2, "b"), (3, "c"), (2, "d"))
  }

  test("union by position uses the configured input order") {
    val left = toDf(Seq((1, "a"), (2, "b")), "id", "name")
    val right = toDf(Seq((3, "c"), (4, "d")), "code", "label")

    val result = new UnionTransform().handle(
      Map("source-1" -> left, "source-2" -> right),
      Some(Map("inputs" -> Seq("source-1", "source-2"), "mode" -> "byPosition")))

    result.columns.toSeq shouldBe Seq("id", "name")
    pairRows(result) shouldBe Set((1, "a"), (2, "b"), (3, "c"), (4, "d"))
  }

  test("union by name can allow missing columns") {
    val left = toDf(Seq((1, "a"), (2, "b")), "id", "name")
    val right = toDf(Seq((3, "x")), "id", "city")

    val result = new UnionTransform().handle(
      Map("source-1" -> left, "source-2" -> right),
      Some(Map(
        "inputs" -> Seq("source-1", "source-2"),
        "mode" -> "byName",
        "allowMissingColumns" -> true)))

    result.columns.toSeq shouldBe Seq("id", "name", "city")
    result.where("id = 3").select("name", "city").collect().head
      .isNullAt(0) shouldBe true
  }

  test("union distinct removes duplicates") {
    val data = toDf(Seq((1, "a"), (1, "a")), "id", "name")

    val result = new UnionTransform().handle(
      Map("source-1" -> data, "source-2" -> data),
      Some(Map(
        "inputs" -> Seq("source-1", "source-2"),
        "mode" -> "byName",
        "distinct" -> true)))

    result.count() shouldBe 1
  }

  test("union rejects invalid input and mode combinations") {
    val left = toDf(Seq((1, "a")), "id", "name")
    val right = toDf(Seq((2, "b")), "id", "name")

    intercept[PuddingException] {
      new UnionTransform().handle(
        Map("source-1" -> left),
        Some(Map("mode" -> "byName")))
    }

    intercept[PuddingException] {
      new UnionTransform().handle(
        Map("source-1" -> left, "source-2" -> right),
        Some(Map("mode" -> "byPosition")))
    }

    intercept[PuddingException] {
      new UnionTransform().handle(
        Map("source-1" -> left, "source-2" -> right),
        Some(Map("inputs" -> Seq("source-1", "missing"), "mode" -> "byName")))
    }

    intercept[PuddingException] {
      new UnionTransform().handle(
        Map("source-1" -> left, "source-2" -> right),
        Some(Map(
          "inputs" -> Seq("source-1", "source-2"),
          "mode" -> "byPosition",
          "allowMissingColumns" -> true)))
    }
  }

  test("column transform selects, renames and casts fields") {
    val result = new ColumnTransform().handle(
      Map("source-1" -> orders()),
      Some(Map("fields" -> Seq(
        "order_id",
        Map("name" -> "user", "source" -> "user_id", "type" -> "string"),
        Map("name" -> "amount_text", "source" -> "amount", "type" -> "string"),
        Map("name" -> "amount_int", "source" -> "amount", "type" -> "int")))))

    result.columns.toSeq shouldBe Seq("order_id", "user", "amount_text", "amount_int")
    val first = result.orderBy("order_id").collect().head
    first.getInt(0) shouldBe 1001
    first.getString(1) shouldBe "1"
    first.getString(2) shouldBe "10.0"
    first.getInt(3) shouldBe 10
  }

  test("column transform rejects invalid fields") {
    intercept[PuddingException] {
      new ColumnTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("fields" -> Seq(
          Map("name" -> "same", "source" -> "order_id"),
          Map("name" -> "same", "source" -> "user_id")))))
    }

    intercept[PuddingException] {
      new ColumnTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("fields" -> Seq(Map("name" -> "amount", "type" -> "not_a_type")))))
    }

    intercept[PuddingException] {
      new ColumnTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("fields" -> Seq(Map("name" -> "missing")))))
    }

    intercept[PuddingException] {
      new ColumnTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("fields" -> Seq(Map("name" -> "id", "unknown" -> "x")))))
    }
  }

  test("limit rows") {
    val result = new LimitTransform().handle(
      Map("source-1" -> orders()),
      Some(Map("limit" -> 2)))

    result.count() shouldBe 2

    val empty = new LimitTransform().handle(
      Map("source-1" -> orders()),
      Some(Map("limit" -> 0)))
    empty.count() shouldBe 0
  }

  test("limit rejects invalid values") {
    intercept[PuddingException] {
      new LimitTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("limit" -> -1)))
    }

    intercept[PuddingException] {
      new LimitTransform().handle(Map("source-1" -> orders()), Some(Map.empty))
    }
  }

  test("inner join on common columns keeps one copy of the key") {
    val result = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "inner",
        "on" -> Seq("user_id"))))

    result.columns.toSeq shouldBe Seq(
      "order_id", "user_id", "amount", "order_city", "name", "city", "age")
    result.count() shouldBe 4
    val row = result.where("order_id = 1001").select("user_id", "name").collect().head
    row.getInt(0) shouldBe 1
    row.getString(1) shouldBe "tom"
  }

  test("join supports different left and right key names") {
    val renamedUsers = users().withColumnRenamed("user_id", "uid")
    val result = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> renamedUsers),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "on" -> Map("user_id" -> "uid"))))

    result.columns.toSeq shouldBe Seq(
      "order_id", "user_id", "amount", "order_city", "name", "city", "age")
    result.count() shouldBe 4
    result.where("order_id = 1001").select("user_id").collect().head.getInt(0) shouldBe 1
  }

  test("left join keeps unmatched left rows") {
    val result = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "left",
        "on" -> Seq("user_id"))))

    result.count() shouldBe 5
    result.where("order_id = 1005").select("user_id", "name").collect().head
      .isNullAt(1) shouldBe true
  }

  test("right and full joins take the key from the right side") {
    val rightJoin = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "right",
        "on" -> Seq("user_id"))))

    rightJoin.count() shouldBe 5
    val unmatchedUser = rightJoin.where("order_id is null")
      .select("user_id", "name").collect().head
    unmatchedUser.getInt(0) shouldBe 5
    unmatchedUser.getString(1) shouldBe "ghost"

    val fullJoin = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "full",
        "on" -> Seq("user_id"))))

    fullJoin.count() shouldBe 6
    fullJoin.select("user_id").collect().map(_.getInt(0)).toSet shouldBe
      Set(1, 2, 3, 4, 5)
  }

  test("semi and anti joins return only left columns") {
    val semi = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "left_semi",
        "on" -> Seq("user_id"))))

    semi.columns.toSeq shouldBe Seq("order_id", "user_id", "amount", "order_city")
    semi.count() shouldBe 4

    val anti = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "left_anti",
        "on" -> Seq("user_id"))))

    anti.count() shouldBe 1
    anti.collect().head.getInt(0) shouldBe 1005
  }

  test("join condition is applied on top of key equality") {
    val result = new JoinTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      Some(Map(
        "left" -> "source-1",
        "right" -> "source-2",
        "joinType" -> "inner",
        "on" -> Seq("user_id"),
        "condition" -> "l.amount >= 20",
        "numPartitions" -> 3)))

    result.count() shouldBe 3
    result.select("order_id").collect().map(_.getInt(0)).toSet shouldBe
      Set(1002, 1003, 1004)
    result.rdd.getNumPartitions shouldBe 3
  }

  test("join rejects invalid configuration") {
    intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders()),
        Some(Map("left" -> "source-1", "right" -> "source-1", "on" -> Seq("user_id"))))
    }

    intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map("left" -> "source-1", "right" -> "source-1", "on" -> Seq("user_id"))))
    }

    intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map("left" -> "source-1", "right" -> "source-2")))
    }

    intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map(
          "left" -> "source-1",
          "right" -> "source-2",
          "joinType" -> "sideways",
          "on" -> Seq("user_id"))))
    }

    intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map(
          "left" -> "source-1",
          "right" -> "source-2",
          "on" -> Seq("missing_column"))))
    }

    intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map(
          "left" -> "source-1",
          "right" -> "source-2",
          "on" -> Map.empty[String, String])))
    }
  }

  test("streaming join requires both event times and watermarks") {
    val left = sparkSession.readStream.format("rate").load()
    val right = sparkSession.readStream.format("rate").load()

    val missing = intercept[PuddingException] {
      new JoinTransform().handle(
        Map("left-stream" -> left, "right-stream" -> right),
        Some(Map(
          "left" -> "left-stream",
          "right" -> "right-stream",
          "on" -> Seq("value"))))
    }
    missing.getMessage should include ("leftEventTime")

    val outer = intercept[PuddingException] {
      new JoinTransform().validateConfig(
        Some(Map(
          "left" -> "left-stream",
          "right" -> "right-stream",
          "on" -> Seq("value"),
          "joinType" -> "left",
          "leftEventTime" -> "timestamp",
          "rightEventTime" -> "timestamp",
          "leftWatermark" -> "1 minute",
          "rightWatermark" -> "1 minute")),
        ExecutionMode.Streaming)
    }
    outer.getMessage should include ("only joinType=inner")

    val batch = intercept[PuddingException] {
      new JoinTransform().handle(
        Map("source-1" -> orders(), "source-2" -> users()),
        Some(Map(
          "left" -> "source-1",
          "right" -> "source-2",
          "on" -> Seq("user_id"),
          "leftEventTime" -> "order_time",
          "rightEventTime" -> "event_time",
          "leftWatermark" -> "1 minute",
          "rightWatermark" -> "1 minute")))
    }
    batch.getMessage should include ("can only be used in streaming")
  }

  test("join two streaming dependencies with event-time watermarks") {
    val leftInput = Files.createTempDirectory("pudding-stream-join-left")
    val rightInput = Files.createTempDirectory("pudding-stream-join-right")
    val checkpoint = Files.createTempDirectory("pudding-stream-join-checkpoint")
    val queryName = "stream_join_" + UUID.randomUUID().toString.replace("-", "")
    try {
      this.writeJson(leftInput, "left.json",
        """{"user_id":1,"order_time":"2026-01-01T00:00:10","amount":10.0}""",
        """{"user_id":2,"order_time":"2026-01-01T00:00:11","amount":20.0}""")
      this.writeJson(rightInput, "right.json",
        """{"user_id":1,"payment_time":"2026-01-01T00:00:10","name":"tom"}""",
        """{"user_id":2,"payment_time":"2026-01-01T00:00:11","name":"jerry"}""")
      val left = sparkSession.readStream
        .schema("user_id int, order_time timestamp, amount double")
        .json(leftInput.toString)
      val right = sparkSession.readStream
        .schema("user_id int, payment_time timestamp, name string")
        .json(rightInput.toString)
      val result = new JoinTransform().handle(
        Map("left-stream" -> left, "right-stream" -> right),
        Some(Map(
          "left" -> "left-stream",
          "right" -> "right-stream",
          "on" -> Seq("user_id"),
          "joinType" -> "inner",
          "leftEventTime" -> "order_time",
          "rightEventTime" -> "payment_time",
          "leftWatermark" -> "10 minutes",
          "rightWatermark" -> "10 minutes")))

      result.isStreaming shouldBe true
      result.columns.toSeq shouldBe
        Seq("user_id", "order_time", "amount", "payment_time", "name")

      val query = result.writeStream
        .format("memory")
        .queryName(queryName)
        .option("checkpointLocation", checkpoint.toString)
        .outputMode("append")
        .start()
      try {
        query.processAllAvailable()
        val rows = sparkSession.table(queryName).collect()
          .map(row => (row.getInt(0), row.getString(4))).toSet
        rows shouldBe Set((1, "tom"), (2, "jerry"))
      } finally {
        query.stop()
      }
    } finally {
      this.deleteRecursively(leftInput)
      this.deleteRecursively(rightInput)
      this.deleteRecursively(checkpoint)
    }
  }
}
