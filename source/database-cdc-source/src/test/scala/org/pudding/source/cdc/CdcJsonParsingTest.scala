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

package org.pudding.source.cdc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The source writes strings and parses them with `from_json`; verify that the value encoding
 * (date, timestamp, decimal, binary) agrees with Spark's JSON parser.
 */
class CdcJsonParsingTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    this.spark = SparkSession.builder()
      .appName("cdc-json-parsing-test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (this.spark != null) {
      this.spark.stop()
      this.spark = null
    }
  }

  test("from_json accepts the canonical CDC value encoding") {
    val schema = CdcSchema.parse(Map("schema" ->
      "id int, amount decimal(12,2), created date, updated timestamp, payload binary"))
    val envelope = CdcSchema.envelope(schema, "__op")
    val json =
      """{"id":1,"amount":"12.50","created":"2024-02-03",
        |"updated":"2024-02-03T04:05:06Z","payload":"AQID",
        |"__op":"I","_cdc_timestamp":123,"_cdc_sequence":7,"_cdc_table":"inventory.orders"}""".stripMargin
    val spark = this.spark
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    val input = spark.createDataFrame(
      java.util.Arrays.asList(Row(json)),
      StructType(Seq(StructField("value", StringType, nullable = false))))
    val parsed = input
      .select(from_json(org.apache.spark.sql.functions.col("value"), envelope,
        Map("mode" -> "FAILFAST")).as("row"))
      .select("row.*")
      .collect()
      .head
    parsed.getAs[Int]("id") shouldBe 1
    parsed.getAs[java.math.BigDecimal]("amount").toPlainString shouldBe "12.50"
    parsed.getAs[java.sql.Date]("created").toString shouldBe "2024-02-03"
    parsed.getAs[java.sql.Timestamp]("updated").getTime shouldBe
      java.time.Instant.parse("2024-02-03T04:05:06Z").toEpochMilli
    parsed.getAs[Array[Byte]]("payload").toSeq shouldBe Seq[Byte](1, 2, 3)
    parsed.getAs[String]("__op") shouldBe "I"
    parsed.getAs[Long]("_cdc_sequence") shouldBe 7L
  }
}
