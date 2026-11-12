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

package org.pudding.connector.jdbc

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the shared JDBC connector configuration.
 */
class JdbcConnectorConfigTest extends AnyFunSuite with Matchers {

  test("builds Spark JDBC options and infers the PostgreSQL driver") {
    val options = JdbcConnectorConfig.connectionOptions(Map[String, Any](
      "url" -> "jdbc:postgresql://db.internal:5432/analytics?sslmode=require",
      "table" -> " public.orders ",
      "user" -> "pudding_ro",
      "password" -> "s3cret",
      "options" -> Map(
        "fetchsize" -> "500",
        "batchsize" -> 100,
        "truncate" -> true
      )
    ))

    options("url") shouldBe "jdbc:postgresql://db.internal:5432/analytics?sslmode=require"
    options("dbtable") shouldBe "public.orders"
    options("user") shouldBe "pudding_ro"
    options("password") shouldBe "s3cret"
    options("driver") shouldBe "org.postgresql.Driver"
    options("fetchsize") shouldBe "500"
    options("batchsize") shouldBe "100"
    options("truncate") shouldBe "true"
  }

  test("lets explicit connection keys win over the tuning options map") {
    val options = JdbcConnectorConfig.connectionOptions(Map[String, Any](
      "url" -> "jdbc:postgresql://db.internal:5432/analytics",
      "table" -> "public.orders",
      "user" -> "pudding_ro",
      "password" -> "s3cret",
      "options" -> Map(
        "url" -> "jdbc:postgresql://attacker.invalid:5432/analytics",
        "dbtable" -> "public.secrets",
        "user" -> "attacker",
        "password" -> "stolen",
        "driver" -> "evil.Driver",
        "fetchsize" -> "200"
      )
    ))

    options("url") shouldBe "jdbc:postgresql://db.internal:5432/analytics"
    options("dbtable") shouldBe "public.orders"
    options("user") shouldBe "pudding_ro"
    options("password") shouldBe "s3cret"
    options("driver") shouldBe "org.postgresql.Driver"
    options("fetchsize") shouldBe "200"
  }

  test("accepts an explicit driver for other JDBC databases") {
    val options = JdbcConnectorConfig.connectionOptions(Map[String, Any](
      "url" -> "jdbc:mysql://db.internal:3306/commerce",
      "table" -> "orders",
      "user" -> "pudding",
      "password" -> "",
      "driver" -> "com.mysql.cj.jdbc.Driver"
    ))

    options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
    options("password") shouldBe ""
  }

  test("requires a driver for JDBC urls that are not PostgreSQL") {
    val error = intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(Map[String, Any](
        "url" -> "jdbc:oracle:thin:@db.internal:1521/xe?user=url-user&password=url-secret",
        "table" -> "orders",
        "user" -> "pudding",
        "password" -> "top-secret"
      ))
    }

    error.getMessage should include("driver is required")
    error.getMessage should not include "url-secret"
    error.getMessage should not include "top-secret"
  }

  test("rejects invalid connection fields without echoing their values") {
    val base = Map[String, Any](
      "url" -> "jdbc:postgresql://db.internal:5432/analytics",
      "table" -> "public.orders",
      "user" -> "pudding_ro",
      "password" -> "top-secret"
    )

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base - "url")
    }.getMessage should include("url")

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base + ("url" -> "postgresql://db.internal/analytics"))
    }.getMessage should include("must start with jdbc")

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base + ("table" -> "   "))
    }.getMessage should include("table")

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base + ("user" -> " "))
    }.getMessage should include("user")

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base - "password")
    }.getMessage should include("password")

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base + ("url" -> 42))
    }.getMessage should include("url must be a string")

    intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(base + ("options" -> Seq("fetchsize" -> "10")))
    }.getMessage should include("options must be a map")

    val nested = intercept[PuddingException] {
      JdbcConnectorConfig.connectionOptions(
        base + ("options" -> Map("properties" -> Map("password" -> "nested-secret"))))
    }
    nested.getMessage should include("options keys")
    nested.getMessage should not include "nested-secret"
  }

  test("validates save modes and defaults to overwrite") {
    JdbcConnectorConfig.saveMode(Map.empty) shouldBe "overwrite"
    JdbcConnectorConfig.saveMode(Map("mode" -> "APPEND")) shouldBe "append"
    JdbcConnectorConfig.saveMode(Map("mode" -> "errorIfExists")) shouldBe "errorifexists"

    intercept[PuddingException] {
      JdbcConnectorConfig.saveMode(Map("mode" -> "merge"))
    }.getMessage shouldBe "unsupported save mode: merge"

    intercept[PuddingException] {
      JdbcConnectorConfig.saveMode(Map("mode" -> 1))
    }.getMessage shouldBe "mode must be a string"
  }
}
