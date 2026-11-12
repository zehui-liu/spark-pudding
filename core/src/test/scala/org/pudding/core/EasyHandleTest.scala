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

package org.pudding.core

import java.sql.{Date, Timestamp}
import java.util.Collections

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the shared configuration and row helpers used by every connector.
 */
class EasyHandleTest extends AnyFunSuite with Matchers {

  private object handle extends EasyHandle

  test("read required values and reject missing or null entries") {
    handle.getMapValueThrow[String](Map("name" -> "orders"), "name") shouldBe "orders"

    val missing = intercept[PuddingException] {
      handle.getMapValueThrow[String](Map.empty, "name")
    }
    missing.getMessage should include ("can not find param: name")

    intercept[PuddingException] {
      handle.getMapValueThrow[String](Map("name" -> null), "name")
    }
  }

  test("treat missing or null optional values as absent") {
    handle.getMapValue[String](Map("name" -> "orders"), "name") shouldBe Some("orders")
    handle.getMapValue[String](Map("name" -> null), "name") shouldBe None
    handle.getMapValue[String](Map.empty, "name") shouldBe None
  }

  test("detect null values in a configuration map") {
    noException should be thrownBy handle.checkMapValueNull(Map("name" -> "orders"))
    val error = intercept[PuddingException] {
      handle.checkMapValueNull(Map("first" -> null, "second" -> "value", "third" -> null))
    }
    error.getMessage should include ("first")
    error.getMessage should include ("third")
  }

  test("normalize Scala and Java option maps to strings") {
    handle.getStringOptions(
      Map("options" -> Map("batchSize" -> 100, "enabled" -> true)),
      "options") shouldBe Map("batchSize" -> "100", "enabled" -> "true")

    val javaOptions = new java.util.HashMap[String, Any]()
    javaOptions.put("timeout", 30L)
    handle.getStringOptions(Map("options" -> javaOptions), "options") shouldBe
      Map("timeout" -> "30")

    handle.getStringOptions(Map.empty, "options") shouldBe Map.empty
    handle.getStringOptions(Map("options" -> Map.empty), "options") shouldBe Map.empty
    handle.getStringOptions(Map("options" -> null), "options") shouldBe Map.empty
  }

  test("flatten nested HOCON option maps to dotted keys") {
    val options = handle.getStringOptions(
      Map("options" -> Map(
        "kafka" -> Map(
          "security" -> Map("protocol" -> "SASL_SSL"),
          "bootstrap" -> Map("servers" -> "broker:9092")),
        "request.timeout.ms" -> 1000)),
      "options")

    options shouldBe Map(
      "kafka.security.protocol" -> "SASL_SSL",
      "kafka.bootstrap.servers" -> "broker:9092",
      "request.timeout.ms" -> "1000")
  }

  test("reject empty nested option maps") {
    val error = intercept[PuddingException] {
      handle.getStringOptions(
        Map("options" -> Map("kafka" -> Map.empty[String, String])),
        "options")
    }
    error.getMessage should include ("empty map")
  }

  test("reject invalid, null or empty option maps") {
    val nullEntry = intercept[PuddingException] {
      handle.getStringOptions(Map("options" -> Map("key" -> null)), "options")
    }
    nullEntry.getMessage should include ("null key or value")

    val notMap = intercept[PuddingException] {
      handle.getStringOptions(Map("options" -> "key=value"), "options")
    }
    notMap.getMessage should include ("must be a map")

    val emptyJavaMap = Collections.emptyMap[String, String]()
    handle.getStringOptions(Map("options" -> emptyJavaMap), "options") shouldBe Map.empty
  }

  test("create a Spark schema from DDL types") {
    val schema = handle.createStructType(Map("name" -> "string", "age" -> "int"))
    schema.fieldNames.toSeq should contain theSameElementsAs Seq("name", "age")
    schema("name").dataType shouldBe StringType
    schema("age").dataType shouldBe IntegerType
  }

  test("convert row values according to their declared type") {
    handle.buildRow("10", "int") shouldBe 10
    handle.buildRow("10", " INTEGER ") shouldBe 10
    handle.buildRow("true", "boolean") shouldBe true
    handle.buildRow("2026-10-27", "date") shouldBe Date.valueOf("2026-10-27")
    handle.buildRow("2026-10-27 10:00:00", "timestamp") shouldBe
      Timestamp.valueOf("2026-10-27 10:00:00")
    handle.buildRow("text", "string") shouldBe "text"
  }

  test("reject invalid row values and types") {
    val invalidValue = intercept[PuddingException] {
      handle.buildRow("not-a-number", "int")
    }
    invalidValue.getMessage should include ("is not a valid int")

    val unsupportedType = intercept[PuddingException] {
      handle.buildRow("value", "decimal")
    }
    unsupportedType.getMessage should include ("Unsupported type: decimal")

    intercept[PuddingException] {
      handle.buildRow(null, "string")
    }
    intercept[PuddingException] {
      handle.buildRow("value", null)
    }
  }
}
