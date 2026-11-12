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

import org.pudding.core.PuddingException
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.{DataSource, IncrementalSemantics, SourceContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for JDBC source option construction. No database is required.
 */
class JdbcSourceTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "analytics-db",
      `type` = "jdbc",
      endpoint = Some("postgres:5432"),
      auth = Map.empty,
      properties = Map.empty,
      options = Map(
        "url" -> "jdbc:postgresql://postgres:5432/analytics",
        "user" -> "pudding",
        "password" -> "secret",
        "fetchsize" -> "500"))
  }

  test("registers the jdbc-source identifier and incremental capabilities") {
    val source = new JdbcSource()
    source.identifier shouldBe "jdbc-source"
    source.supportsConnection shouldBe true
    source.supportsIncrementalSync shouldBe true
    source.supportedIncrementalSemantics shouldBe
      Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert)
    DataSource.identifiers should contain ("jdbc-source")
  }

  test("build reader options from a connection and infer the PostgreSQL driver") {
    val options = JdbcSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "table" -> "public.orders",
        "options" -> Map("fetchsize" -> "1000"))),
      connection = Some(connection)))

    options("url") shouldBe "jdbc:postgresql://postgres:5432/analytics"
    options("dbtable") shouldBe "public.orders"
    options("user") shouldBe "pudding"
    options("password") shouldBe "secret"
    options("driver") shouldBe "org.postgresql.Driver"
    options("fetchsize") shouldBe "1000"
  }

  test("build legacy options with an explicit driver") {
    val options = JdbcSource.readerOptions(SourceContext(Some(Map[String, Any](
      "url" -> "jdbc:mysql://mysql:3306/orders",
      "table" -> "orders",
      "user" -> "pudding",
      "password" -> "secret",
      "driver" -> "com.mysql.cj.jdbc.Driver"))))

    options("dbtable") shouldBe "orders"
    options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
  }

  test("reject a wrong connection type and missing configuration") {
    val wrongConnection = intercept[PuddingException] {
      JdbcSource.readerOptions(SourceContext(
        config = Some(Map[String, Any]("table" -> "orders")),
        connection = Some(connection.copy(`type` = "kafka"))))
    }
    wrongConnection.getMessage should include ("requires a jdbc connection")

    intercept[PuddingException] {
      JdbcSource.readerOptions(SourceContext(None))
    }
    intercept[PuddingException] {
      JdbcSource.readerOptions(SourceContext(Some(Map[String, Any](
        "url" -> "jdbc:postgresql://postgres:5432/analytics",
        "user" -> "pudding",
        "password" -> "secret"))))
    }
  }

  test("reject urls that are not JDBC urls") {
    val error = intercept[PuddingException] {
      JdbcSource.readerOptions(SourceContext(Some(Map[String, Any](
        "url" -> "postgresql://postgres:5432/analytics",
        "table" -> "orders",
        "user" -> "pudding",
        "password" -> "secret"))))
    }
    error.getMessage should include ("must start with jdbc:")
  }
}
