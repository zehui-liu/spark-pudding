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

package org.pudding.integration

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.max
import org.pudding.core.definition.{
  IncrementalRequest,
  IncrementalSemantics,
  SinkContext,
  SourceContext
}
import org.pudding.core.configure.ExecutionMode
import org.pudding.sink.HudiSink
import org.pudding.source.HudiSource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Hudi data-path regression on an S3-compatible object store.
 *
 * This suite writes table files, timeline commits and streaming checkpoints to S3A. It is not a
 * replacement for the local Hudi unit tests; it catches object-store-specific behavior such as
 * rename-free commits, S3A credentials, file listing and checkpoint writes.
 */
class HudiObjectStoreTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var context: ObjectStoreTestSupport.Context = _

  override def beforeAll(): Unit = {
    this.context = ObjectStoreTestSupport.open("hudi object store regression").orNull
  }

  override def afterAll(): Unit = {
    if (this.context != null) {
      this.context.close()
      this.context = null
    }
  }

  test("batch upsert, incremental read and full refresh on S3A") {
    assume(this.context != null, "object-store test environment is not available")
    val spark = this.context.spark
    val suffix = "hudi/incremental_" + System.nanoTime()
    val path = this.context.s3a(suffix)
    val table = "hudi_object_store_" + System.nanoTime()
    val sink = new HudiSink()
    val base = this.baseConfig(table, path, "insert", "overwrite")
    sink.writer(this.orders(), Some(base))
    this.readRows(path) shouldBe Set((1, "tom"), (2, "jerry"))

    val begin = this.latestCommit(path)
    val session = spark
    import session.implicits._
    sink.writer(
      Seq(
        (1, "tom_v2", "2026-01-01", 2L),
        (4, "new", "2026-01-03", 2L)
      ).toDF("id", "name", "dt", "ts"),
      Some(this.baseConfig(table, path, "upsert", "append")))

    val read = new HudiSource().readIncremental(
      spark,
      SourceContext(Some(Map("table" -> table, "tablePath" -> path))),
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

    sink.writeFullRefresh(
      Seq((9, "replacement", "2026-03-01", 9L)).toDF("id", "name", "dt", "ts"),
      SinkContext(Some(this.baseConfig(table, path, "insert", "overwrite"))))
    this.readRows(path) shouldBe Set((9, "replacement"))
  }

  test("failed full refresh keeps the previous S3A snapshot") {
    assume(this.context != null, "object-store test environment is not available")
    val path = this.context.s3a("hudi/failed_refresh_" + System.nanoTime())
    val table = "hudi_failed_" + System.nanoTime()
    val sink = new HudiSink()
    val config = this.baseConfig(table, path, "insert", "overwrite")
    sink.writer(this.orders(), Some(config))
    val session = this.context.spark
    import session.implicits._

    intercept[Exception] {
      sink.writeFullRefresh(
        Seq((9, 9)).toDF("id", "bad"),
        SinkContext(Some(config)))
    }

    this.readRows(path) shouldBe Set((1, "tom"), (2, "jerry"))
  }

  test("streaming update with an S3A checkpoint") {
    assume(this.context != null, "object-store test environment is not available")
    val spark = this.context.spark
    val suffix = "hudi/streaming_" + System.nanoTime()
    val path = this.context.s3a(suffix)
    val table = "hudi_stream_" + System.nanoTime()
    val sink = new HudiSink()
    sink.writer(this.orders(), Some(this.baseConfig(table, path, "insert", "overwrite")))
    val input = ObjectStoreTestSupport.localInput(Seq(
      """{"id":1,"name":"tom_v2","dt":"2026-01-01","ts":2}""",
      """{"id":4,"name":"spark","dt":"2026-01-04","ts":4}"""))
    val checkpoint = this.context.s3a(s"checkpoints/hudi_$table")
    try {
      val stream = spark.readStream
        .schema("id int, name string, dt string, ts long")
        .json(input.toString)
        .groupBy("id", "dt")
        .agg(max("name").as("name"), max("ts").as("ts"))
      sink.writer(
        stream,
        SinkContext(
          Some(this.baseConfig(table, path, "upsert", "append") ++ Map(
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
      (1, "tom", "2026-01-01", 1L),
      (2, "jerry", "2026-01-02", 1L)
    ).toDF("id", "name", "dt", "ts")
  }

  private def baseConfig(
      table: String,
      path: String,
      operation: String,
      mode: String): Map[String, Any] = {
    Map(
      "table" -> table,
      "tablePath" -> path,
      "operation" -> operation,
      "tableType" -> "COPY_ON_WRITE",
      "recordKeyFields" -> Seq("id"),
      "partitionPathField" -> "dt",
      "preCombineField" -> "ts",
      "mode" -> mode)
  }

  private def readRows(path: String): Set[(Int, String)] = {
    this.context.spark.read.format("org.apache.hudi").load(path)
      .select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet
  }

  private def latestCommit(path: String): String = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(this.context.spark.sessionState.newHadoopConf())
      .setBasePath(path)
      .build()
    metaClient.getCommitsTimeline.filterCompletedInstants.lastInstant().get().getTimestamp
  }
}
