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

package org.pudding.source.cdc

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException

class CdcSchemaTest extends AnyFunSuite with Matchers {

  test("parse DDL and map forms and force nullable fields") {
    val ddl = CdcSchema.parse(Map("schema" -> "id int, name string"))
    ddl.fieldNames.toSeq shouldBe Seq("id", "name")
    ddl.fields.foreach(_.nullable shouldBe true)

    val mapped = CdcSchema.parse(Map("schema" -> Map("id" -> "int", "name" -> "string")))
    mapped("id").dataType shouldBe IntegerType
    mapped("name").dataType shouldBe StringType
  }

  test("JSON round-trip remains a validated struct") {
    val schema = CdcSchema.parse(Map("schema" -> "id int, name string"))
    CdcSchema.fromJson(schema.json) shouldBe schema
    CdcSchema.fromJson(CdcSchema.fromJson(schema.json).json) shouldBe schema
  }

  test("reject reserved names, duplicates and map types from JSON") {
    an[PuddingException] should be thrownBy
      CdcSchema.parse(Map("schema" -> "id int, _cdc_timestamp long"))
    an[PuddingException] should be thrownBy
      CdcSchema.parse(Map("schema" -> "id int, id string"))
    an[PuddingException] should be thrownBy
      CdcSchema.parse(Map("schema" -> "id int, tags map<int,string>"))
  }

  test("operation column can not collide with a data column") {
    val schema = CdcSchema.parse(Map("schema" -> "id int, operation string"))
    an[PuddingException] should be thrownBy CdcSchema.envelope(schema, "operation")
    an[PuddingException] should be thrownBy CdcSchema.envelope(schema, "_cdc_op")
  }
}
