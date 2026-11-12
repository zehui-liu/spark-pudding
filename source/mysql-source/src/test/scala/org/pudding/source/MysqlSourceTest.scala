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
import org.pudding.core.definition.{DataSource, SourceContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for MySQL source option construction. No database is required.
 */
class MysqlSourceTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "orders-db",
      `type` = "jdbc",
      endpoint = Some("mysql:3306"),
      auth = Map.empty,
      properties = Map.empty,
      options = Map(
        "url" -> "jdbc:mysql://mysql:3306/orders",
        "user" -> "app",
        "password" -> "secret"))
  }

  test("register the mysql-source identifier") {
    new MysqlSource().identifier shouldBe "mysql-source"
    new MysqlSource().supportsConnection shouldBe true
    DataSource.identifiers should contain ("mysql-source")
  }

  test("build reader options from a jdbc connection") {
    val options = MysqlSource.readerOptions(SourceContext(
      config = Some(Map[String, Any]("table" -> "orders")),
      connection = Some(connection)))

    options("url") shouldBe "jdbc:mysql://mysql:3306/orders"
    options("dbtable") shouldBe "orders"
    options("user") shouldBe "app"
    options("password") shouldBe "secret"
    options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
  }

  test("build legacy options and default the MySQL driver") {
    val options = MysqlSource.readerOptions(SourceContext(Some(Map[String, Any](
      "url" -> "jdbc:mysql://localhost:3306/orders",
      "table" -> "orders",
      "user" -> "app",
      "password" -> "secret"))))

    options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
    options("password") shouldBe "secret"
  }

  test("keep a driver configured on the connection or in the job") {
    val fromConnection = MysqlSource.readerOptions(SourceContext(
      config = Some(Map[String, Any]("table" -> "orders")),
      connection = Some(connection.copy(
        options = connection.options + ("driver" -> "custom.Driver")))))
    fromConnection("driver") shouldBe "custom.Driver"

    val fromConfig = MysqlSource.readerOptions(SourceContext(Some(Map[String, Any](
      "url" -> "jdbc:mysql://localhost:3306/orders",
      "table" -> "orders",
      "user" -> "app",
      "password" -> "secret",
      "driver" -> "custom.Driver"))))
    fromConfig("driver") shouldBe "custom.Driver"
  }

  test("reject a non-jdbc connection and missing configuration") {
    val wrongConnection = intercept[PuddingException] {
      MysqlSource.readerOptions(SourceContext(
        config = Some(Map[String, Any]("table" -> "orders")),
        connection = Some(connection.copy(`type` = "kafka"))))
    }
    wrongConnection.getMessage should include ("requires a jdbc or mysql connection")

    val missingConfig = intercept[PuddingException] {
      MysqlSource.readerOptions(SourceContext(None))
    }
    missingConfig.getMessage should include ("config can not be null")

    intercept[PuddingException] {
      MysqlSource.readerOptions(SourceContext(Some(Map[String, Any](
        "url" -> "jdbc:mysql://localhost:3306/orders",
        "user" -> "app",
        "password" -> "secret"))))
    }
  }

  test("reject a url that is not a JDBC url") {
    val error = intercept[PuddingException] {
      MysqlSource.readerOptions(SourceContext(Some(Map[String, Any](
        "url" -> "mysql://localhost:3306/orders",
        "table" -> "orders",
        "user" -> "app",
        "password" -> "secret"))))
    }
    error.getMessage should include ("must start with jdbc:")
  }
}
