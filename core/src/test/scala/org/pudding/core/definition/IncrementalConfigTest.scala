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

package org.pudding.core.definition

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the nested incremental source configuration.
 */
class IncrementalConfigTest extends AnyFunSuite with Matchers {

  private def config(values: Map[String, Any]): Option[Map[String, Any]] = {
    Some(Map("incremental" -> values))
  }

  test("parse an upsert source config with defaults") {
    val spec = IncrementalConfig.parse(
      config(Map(
        "semantics" -> "UPSERT",
        "keyFields" -> Seq("id", "dt"),
        "initial" -> "latest")),
      "orders").get

    spec.semantics shouldBe IncrementalSemantics.Upsert
    spec.keyFields shouldBe Seq("id", "dt")
    spec.opColumn shouldBe IncrementalConfig.DefaultOpColumn
    spec.initial shouldBe "latest"
    spec.start shouldBe None
    spec.statePath shouldBe None
    spec.recovery shouldBe IncrementalRecoveryPolicy.Auto
  }

  test("parse the object-store best-effort lock opt-in") {
    val spec = IncrementalConfig.parse(
      config(Map(
        "semantics" -> "append",
        "initial" -> "latest",
        "allowBestEffortLock" -> true)),
      "orders").get

    spec.allowBestEffortLock shouldBe true
  }

  test("parse an explicit recovery policy") {
    val spec = IncrementalConfig.parse(
      config(Map(
        "semantics" -> "append",
        "initial" -> "latest",
        "recovery" -> "retry")),
      "orders").get

    spec.recovery shouldBe IncrementalRecoveryPolicy.Retry
  }

  test("parse cdc with a custom operation column and state path") {
    val spec = IncrementalConfig.parse(
      config(Map(
        "semantics" -> "cdc",
        "keyFields" -> "id,dt",
        "opColumn" -> "change_type",
        "start" -> "100",
        "end" -> "200",
        "statePath" -> "hdfs:///pudding/state")),
      "orders").get

    spec.semantics shouldBe IncrementalSemantics.Cdc
    spec.keyFields shouldBe Seq("id", "dt")
    spec.opColumn shouldBe "change_type"
    spec.start shouldBe Some("100")
    spec.end shouldBe Some("200")
    spec.statePath shouldBe Some("hdfs:///pudding/state")
  }

  test("parse a java map and a disabled incremental block") {
    val javaConfig = new java.util.HashMap[String, Any]()
    javaConfig.put("enabled", false)
    IncrementalConfig.parse(Some(Map("incremental" -> javaConfig)), "orders") shouldBe None

    val javaValues = new java.util.HashMap[String, Any]()
    javaValues.put("semantics", "append")
    javaValues.put("initial", "full")
    val spec = IncrementalConfig.parse(
      Some(Map("incremental" -> javaValues)), "orders").get
    spec.semantics shouldBe IncrementalSemantics.Append
    spec.keyFields shouldBe Seq.empty
  }

  test("parse a JDBC state backend and its options") {
    val spec = IncrementalConfig.parse(
      config(Map(
        "semantics" -> "append",
        "initial" -> "latest",
        "state" -> Map(
          "backend" -> "jdbc",
          "url" -> "jdbc:h2:mem:test",
          "user" -> "sa",
          "password" -> "${secret:env/DB_PASSWORD}"))),
      "orders").get

    spec.state.map(_.backend) shouldBe Some("jdbc")
    spec.state.map(_.options("url")) shouldBe Some("jdbc:h2:mem:test")
    spec.state.map(_.options("user")) shouldBe Some("sa")
    spec.state.map(_.options("password")) shouldBe
      Some("${secret:env/DB_PASSWORD}")
  }

  test("reject a state block without a backend") {
    val error = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map(
          "semantics" -> "append",
          "initial" -> "latest",
          "state" -> Map("url" -> "jdbc:h2:mem:test"))),
        "orders")
    }
    error.getMessage should include ("state.backend")
  }

  test("reject an incremental block without semantics") {
    val error = intercept[PuddingException] {
      IncrementalConfig.parse(config(Map("initial" -> "latest")), "orders")
    }
    error.getMessage should include ("incremental.semantics")
  }

  test("reject upsert and cdc without key fields") {
    val upsert = intercept[PuddingException] {
      IncrementalConfig.parse(config(Map("semantics" -> "upsert")), "orders")
    }
    upsert.getMessage should include ("requires keyFields")

    val cdc = intercept[PuddingException] {
      IncrementalConfig.parse(config(Map("semantics" -> "cdc")), "orders")
    }
    cdc.getMessage should include ("requires keyFields")
  }

  test("reject start combined with an initial mode") {
    val error = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map(
          "semantics" -> "append",
          "start" -> "5",
          "initial" -> "full")),
        "orders")
    }
    error.getMessage should include ("can not be combined with initial=full")
  }

  test("reject start=latest instead of silently ignoring it") {
    val error = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map("semantics" -> "append", "start" -> "latest")), "orders")
    }
    error.getMessage should include ("use initial=latest")
  }

  test("reject duplicate key fields and an operation column used as a key") {
    val duplicates = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map(
          "semantics" -> "upsert",
          "keyFields" -> Seq("id", "id"),
          "initial" -> "latest")),
        "orders")
    }
    duplicates.getMessage should include ("contains duplicates")

    val operationKey = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map(
          "semantics" -> "cdc",
          "keyFields" -> Seq("id", "__op"),
          "initial" -> "latest")),
        "orders")
    }
    operationKey.getMessage should include ("must not contain opColumn")
  }

  test("reject unsupported semantics, modes and initial modes") {
    val semantics = intercept[PuddingException] {
      IncrementalConfig.parse(config(Map("semantics" -> "merge")), "orders")
    }
    semantics.getMessage should include ("unsupported")

    val mode = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map("semantics" -> "append", "mode" -> "streaming")), "orders")
    }
    mode.getMessage should include ("mode must be 'batch'")

    val initial = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map("semantics" -> "append", "initial" -> "yesterday")), "orders")
    }
    initial.getMessage should include ("initial must be one of")

    val recovery = intercept[PuddingException] {
      IncrementalConfig.parse(
        config(Map("semantics" -> "append", "recovery" -> "sometimes")), "orders")
    }
    recovery.getMessage should include ("recovery")
  }

  test("reject non-string values in typed fields") {
    val error = intercept[PuddingException] {
      IncrementalConfig.parse(config(Map("semantics" -> "append", "opColumn" -> 1)), "orders")
    }
    error.getMessage should include ("must be a string")
  }

  test("reject null keys and values in the incremental block") {
    val nullValue = intercept[PuddingException] {
      IncrementalConfig.parse(config(Map("semantics" -> null)), "orders")
    }
    nullValue.getMessage should include ("null key or value")

    val nullKey = intercept[PuddingException] {
      IncrementalConfig.parse(
        Some(Map("incremental" -> Map[Any, Any]((null: Any) -> "append"))), "orders")
    }
    nullKey.getMessage should include ("null key or value")
  }
}
