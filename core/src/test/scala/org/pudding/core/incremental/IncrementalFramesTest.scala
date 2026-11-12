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

import org.pudding.core.CommonTestUtils
import org.pudding.core.PuddingException
import org.pudding.core.definition.{IncrementalSemantics, IncrementalWrite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for keeping the final change of every key in one CDC batch.
 */
class IncrementalFramesTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for incremental frames")

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    spark = utils.sparkSession
  }

  override def afterAll(): Unit = {
    utils.closeSparkSession()
  }

  private def write(
      keyFields: Seq[String] = Seq("id"),
      orderFields: Seq[String] = Seq("sequence")): IncrementalWrite = {
    IncrementalWrite(
      semantics = IncrementalSemantics.Cdc,
      keyFields = keyFields,
      opColumn = "__op",
      fromCursor = None,
      toCursor = "20",
      cursorType = "test",
      orderFields = orderFields)
  }

  test("keep only the last change for every key") {
    val session = spark
    import session.implicits._

    val input = Seq(
      (1, "old", 1L, "U"),
      (1, "new", 2L, "U"),
      (1, "deleted", 3L, "D"),
      (2, "only", 1L, "I")).toDF("id", "value", "sequence", "__op")

    val result = IncrementalFrames.latestByKey(input, write())
      .orderBy("id")
      .collect()
      .map(row => (row.getInt(0), row.getString(1), row.getString(3)))

    result.toSeq shouldBe Seq((1, "deleted", "D"), (2, "only", "I"))
    // `sequence` is a real payload column, so it is kept; sinks align to their target schema.
    IncrementalFrames.latestByKey(input, write()).columns.toSet shouldBe
      Set("id", "value", "sequence", "__op")
  }

  test("drop internal ordering metadata columns") {
    val session = spark
    import session.implicits._

    val input = Seq(
      (1, "old", 1L, 10L, "U"),
      (1, "new", 2L, 11L, "U")).toDF("id", "value", "sequence", "_cdc_sequence", "__op")
    val result = IncrementalFrames.latestByKey(input, write(orderFields = Seq("_cdc_sequence")))

    result.columns.toSet shouldBe Set("id", "value", "sequence", "__op")
  }

  test("return the frame unchanged when the source has no ordering") {
    val session = spark
    import session.implicits._

    val input = Seq((1, "U"), (1, "U")).toDF("id", "__op")
    val result = IncrementalFrames.latestByKey(input, write(orderFields = Seq.empty))

    result.collect().length shouldBe 2
    result.columns.toSeq shouldBe Seq("id", "__op")
  }

  test("keep order fields that are also key fields") {
    val session = spark
    import session.implicits._

    val input = Seq(
      (1, "a", 1L),
      (1, "b", 2L)).toDF("id", "sequence", "value")
    val result = IncrementalFrames.latestByKey(
      input,
      write(keyFields = Seq("id", "sequence")))

    result.count() shouldBe 2
    result.columns.toSet shouldBe Set("id", "sequence", "value")
  }

  test("avoid colliding with a user column named like the temporary rank") {
    val session = spark
    import session.implicits._

    val input = Seq(
      (1, "first", 1L, "keep-1"),
      (1, "second", 2L, "keep-2")).toDF("id", "value", "sequence", "__pudding_cdc_rank")
    val result = IncrementalFrames.latestByKey(input, write())

    result.columns.toSet shouldBe Set("id", "value", "sequence", "__pudding_cdc_rank")
    result.collect().head.getString(3) shouldBe "keep-2"
  }

  test("reject missing key fields and ordering columns") {
    val session = spark
    import session.implicits._

    val input = Seq((1, "a", 1L)).toDF("id", "value", "sequence")

    intercept[PuddingException] {
      IncrementalFrames.latestByKey(input, write(keyFields = Seq.empty))
    }
    val missing = intercept[PuddingException] {
      IncrementalFrames.latestByKey(input, write(orderFields = Seq("missing")))
    }
    missing.getMessage should include ("missing change ordering columns: missing")

    val missingKey = intercept[PuddingException] {
      IncrementalFrames.latestByKey(input, write(keyFields = Seq("missing_key")))
    }
    missingKey.getMessage should include ("missing key columns: missing_key")
  }
}
