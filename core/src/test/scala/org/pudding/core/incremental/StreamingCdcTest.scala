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

package org.pudding.core.incremental

import org.apache.spark.sql.SparkSession
import org.pudding.core.PuddingException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamingCdcTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("test for streaming cdc helpers")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
    }
  }

  private def source(): org.apache.spark.sql.DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "i", 1L, "topic", 0, 100L),
      (1, "tom_v2", "u", 2L, "topic", 0, 200L),
      (2, "jerry", "i", 1L, "topic", 0, 100L),
      (2, "jerry", "d", 2L, "topic", 0, 200L),
      (3, "spark", "c", 1L, "topic", 0, 100L)
    ).toDF("id", "name", "__op", "_kafka_offset", "_kafka_topic", "_kafka_partition",
      "_kafka_timestamp")
  }

  test("normalize and keep the latest operation per key") {
    val prepared = StreamingCdc.prepare(
      source(), Seq("id"), "__op", Seq("_kafka_offset"))
    prepared.columns should not contain "_kafka_offset"
    prepared.select("id", "name", "__op").collect()
      .map(row => (row.getInt(0), row.getString(1), row.getString(2))).toSet shouldBe
      Set((1, "tom_v2", "U"), (2, "jerry", "D"), (3, "spark", "I"))
  }

  test("resolve Kafka metadata automatically and drop internal columns") {
    val prepared = StreamingCdc.prepare(source(), Seq("id"), "__op", Seq.empty)
    prepared.select("__op").collect().map(_.getString(0)).toSet shouldBe Set("I", "U", "D")
    StreamingCdc.dropInternal(prepared).columns shouldBe Seq("id", "name", "__op")
  }

  test("prefer broker position over event time for Kafka ordering") {
    val session = sparkSession
    import session.implicits._
    val rows = Seq(
      (1, "old", "I", 0, 100L, 200L),
      (1, "deleted", "D", 0, 200L, 100L)
    ).toDF("id", "name", "__op", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")

    val prepared = StreamingCdc.prepare(rows, Seq("id"), "__op", Seq.empty)
    prepared.select("name", "__op").head() shouldBe
      org.apache.spark.sql.Row("deleted", "D")
  }

  test("reject an unknown operation and missing ordering") {
    val session = sparkSession
    import session.implicits._
    val invalid = Seq((1, "x", "unknown", 1L))
      .toDF("id", "name", "__op", "_kafka_offset")
    val error = intercept[Exception] {
      StreamingCdc.prepare(invalid, Seq("id"), "__op", Seq("_kafka_offset")).collect()
    }
    Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      .map(cause => Option(cause.getMessage).getOrElse(""))
      .mkString(" | ") should include ("unsupported cdc operation")

    val noOrder = Seq((1, "x", "I")).toDF("id", "name", "__op")
    intercept[PuddingException] {
      StreamingCdc.prepare(noOrder, Seq("id"), "__op", Seq.empty)
    }.getMessage should include ("cdcOrderFields")
  }

  test("require keys for cdc") {
    val error = intercept[PuddingException] {
      StreamingCdc.prepare(source(), Seq.empty, "__op", Seq("_kafka_offset"))
    }
    error.getMessage should include ("keyFields")
  }
}
