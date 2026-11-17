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

package org.pudding.integration

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.max
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{IncrementalSemantics, IncrementalWrite, SinkContext}
import org.pudding.sink.PaimonSink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Paimon data-path regression on an S3-compatible object store.
 *
 * Paimon uses HadoopFileIO in this runtime; the suite validates table creation, snapshots,
 * changelog/CDC writes, full refresh and streaming checkpoints on S3A.
 */
class PaimonObjectStoreTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var context: ObjectStoreTestSupport.Context = _

  override def beforeAll(): Unit = {
    this.context = ObjectStoreTestSupport.open("paimon object store regression").orNull
  }

  override def afterAll(): Unit = {
    if (this.context != null) {
      this.context.close()
      this.context = null
    }
  }

  test("batch write, cdc and full refresh on S3A") {
    assume(this.context != null, "object-store test environment is not available")
    val spark = this.context.spark
    val path = this.createTable("paimon_incremental_" + System.nanoTime())
    val sink = new PaimonSink()
    val config = this.baseConfig(path)
    sink.writer(this.orders(), SinkContext(Some(config)))
    this.readRows(path) shouldBe Set((1, "tom"), (2, "jerry"))

    val session = spark
    import session.implicits._
    sink.writeIncremental(
      Seq(
        (1, "tom_v2", "2026-01-01", "U", 1L),
        (2, "jerry", "2026-01-02", "D", 2L),
        (3, "spark", "2026-01-03", "I", 3L)
      ).toDF("id", "name", "dt", "op", "ord"),
      SinkContext(Some(config)),
      IncrementalWrite(
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "op",
        fromCursor = Some("1"),
        toCursor = "2",
        cursorType = "snapshot_id",
        orderFields = Seq("ord")))
    this.readRows(path) shouldBe Set((1, "tom_v2"), (3, "spark"))

    sink.writeFullRefresh(
      Seq((9, "replacement", "2026-03-01")).toDF("id", "name", "dt"),
      SinkContext(Some(config ++ Map("mode" -> "overwrite"))))
    this.readRows(path) shouldBe Set((9, "replacement"))
  }

  test("streaming update with an S3A checkpoint") {
    assume(this.context != null, "object-store test environment is not available")
    val spark = this.context.spark
    val table = "paimon_streaming_" + System.nanoTime()
    val path = this.createTable(table)
    val sink = new PaimonSink()
    sink.writer(this.orders(), SinkContext(Some(this.baseConfig(path))))
    val input = ObjectStoreTestSupport.localInput(Seq(
      """{"id":1,"name":"tom_v2","dt":"2026-01-01"}""",
      """{"id":4,"name":"spark","dt":"2026-01-04"}"""))
    val checkpoint = this.context.s3a(s"checkpoints/paimon_$table")
    try {
      val stream = spark.readStream
        .schema("id int, name string, dt string")
        .json(input.toString)
        .groupBy("id", "dt")
        .agg(max("name").as("name"))
      sink.writer(
        stream,
        SinkContext(
          Some(this.baseConfig(path) ++ Map(
            "checkpointLocation" -> checkpoint,
            "outputMode" -> "update")),
          executionMode = Some(ExecutionMode.Streaming),
          trigger = Some("available-now")))
    } finally {
      ObjectStoreTestSupport.deleteLocal(input)
    }

    this.readRows(path) shouldBe Set((1, "tom_v2"), (2, "jerry"), (4, "spark"))
  }

  private def orders(): DataFrame = {
    val session = this.context.spark
    import session.implicits._
    Seq(
      (1, "tom", "2026-01-01"),
      (2, "jerry", "2026-01-02")
    ).toDF("id", "name", "dt")
  }

  private def baseConfig(path: String): Map[String, Any] = {
    Map("path" -> path)
  }

  private def createTable(name: String): String = {
    val spark = this.context.spark
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
    spark.sql(s"DROP TABLE IF EXISTS paimon.default.$name")
    spark.sql(
      s"""CREATE TABLE paimon.default.$name (
         |  id INT,
         |  name STRING,
         |  dt STRING
         |) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
    spark.sql(s"DESCRIBE EXTENDED paimon.default.$name")
      .where("upper(col_name) = 'LOCATION'")
      .select("data_type")
      .head().getString(0)
  }

  private def readRows(path: String): Set[(Int, String)] = {
    this.context.spark.read.format("paimon").load(path)
      .select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet
  }
}
