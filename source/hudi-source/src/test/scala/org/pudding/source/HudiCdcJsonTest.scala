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

import org.apache.avro.Schema
import org.apache.hudi.AvroConversionUtils
import org.apache.spark.sql.types._
import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Decodes both Hudi CDC image encodings without starting a Spark session.
 */
class HudiCdcJsonTest extends AnyFunSuite with Matchers {

  test("decode Avro bytes DECIMAL text even when the unscaled bytes look numeric") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("amount", DecimalType(10, 2))))
    val avro = new Schema.Parser().parse(
      """{"type":"record","name":"Record","namespace":"test","fields":[
        |{"name":"id","type":["null","int"],"default":null},
        |{"name":"amount","type":["null",
        |{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}],"default":null}]}
        |""".stripMargin)

    // 49 = 0x31 = the ASCII character "1"; it must decode as 0.49, not 1.00.
    val first = HudiCdcJson.decodeRecord(
      """{"id":1,"amount":"1"}""", avro, schema, populateMetaFields = true)
    first.getDecimal(1).compareTo(new java.math.BigDecimal("0.49")) shouldBe 0

    // 12345 = 0x3039 = the ASCII characters "09"; it must decode as 123.45.
    val second = HudiCdcJson.decodeRecord(
      """{"id":1,"amount":"09"}""", avro, schema, populateMetaFields = true)
    second.getDecimal(1).compareTo(new java.math.BigDecimal("123.45")) shouldBe 0

    // Two's-complement 0xCF is -49; it must decode as -0.49.
    val negative = HudiCdcJson.decodeRecord(
      "{\"id\":1,\"amount\":\"Ï\"}", avro, schema, populateMetaFields = true)
    negative.getDecimal(1).compareTo(new java.math.BigDecimal("-0.49")) shouldBe 0
  }

  test("decode Avro nested struct, array and map") {
    val schema = StructType(Seq(
      StructField("nested", new StructType()
        .add("x", StringType)
        .add("y", IntegerType)),
      StructField("arr", ArrayType(IntegerType)),
      StructField("m", MapType(StringType, IntegerType))))
    val avro = AvroConversionUtils.convertStructTypeToAvroSchema(schema, "Record", "test")

    val row = HudiCdcJson.decodeRecord(
      """{"nested":{"x":"x","y":7},"arr":[1,2,3],"m":{"a":9}}""",
      avro,
      schema,
      populateMetaFields = true)

    row.getStruct(0).getString(0) shouldBe "x"
    row.getStruct(0).getInt(1) shouldBe 7
    row.getSeq[Int](1) shouldBe Seq(1, 2, 3)
    row.getAs[Map[String, Int]](2) shouldBe Map("a" -> 9)
  }

  test("decode Jackson base64 binary, UTF8String beans, struct, array and map") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("bin", BinaryType),
      StructField("nested", new StructType()
        .add("x", StringType)
        .add("y", IntegerType)),
      StructField("arr", ArrayType(IntegerType)),
      StructField("m", MapType(StringType, IntegerType))))
    val avro = AvroConversionUtils.convertStructTypeToAvroSchema(schema, "Record", "test")
    val json =
      """{"_hoodie_record_key":"1","_hoodie_commit_time":"20260101000000",
        |"id":1,"bin":"CQgH",
        |"nested":{"values":[{"baseOffset":16,"bytes":"eA==","byteBuffer":"eA==",
        |"prefix":0,"baseObject":"eA=="},7]},
        |"arr":{"array":[1,2,3]},
        |"m":{"keyArray":{"array":[{"bytes":"YQ==","baseOffset":16,
        |"byteBuffer":"YQ==","prefix":0,"baseObject":"YQ=="}]},"valueArray":{"array":[9]}}}
        |""".stripMargin

    val row = HudiCdcJson.decodeRecord(
      json, avro, schema, populateMetaFields = true)

    row.getAs[Array[Byte]](1).toSeq shouldBe Seq[Byte](9, 8, 7)
    row.getStruct(2).getString(0) shouldBe "x"
    row.getStruct(2).getInt(1) shouldBe 7
    row.getSeq[Int](3) shouldBe Seq(1, 2, 3)
    row.getAs[Map[String, Int]](4) shouldBe Map("a" -> 9)
  }

  test("fail closed on a Jackson DECIMAL image instead of writing a wrong value") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("amount", DecimalType(10, 2))))
    val avro = AvroConversionUtils.convertStructTypeToAvroSchema(schema, "Record", "test")
    val json = """{"_hoodie_record_key":"1","id":1,"amount":{"zero":false}}"""

    val error = intercept[PuddingException] {
      HudiCdcJson.decodeRecord(json, avro, schema, populateMetaFields = true)
    }

    error.getMessage should include ("amount")
    error.getMessage should include ("DECIMAL")
  }

  test("fail closed when a binary encoding is ambiguous") {
    val schema = StructType(Seq(StructField("bin", BinaryType)))
    val avro = AvroConversionUtils.convertStructTypeToAvroSchema(schema, "Record", "test")

    val error = intercept[PuddingException] {
      HudiCdcJson.decodeRecord(
        """{"bin":"CQgH"}""", avro, schema, populateMetaFields = false)
    }

    error.getMessage should include ("ambiguous")
    error.getMessage should include ("bin")
  }
}
