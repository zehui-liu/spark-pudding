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

package org.pudding.sink

import org.pudding.core.PuddingException
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.{DataSink, SinkContext}
import org.pudding.connector.jdbc.JdbcConnectorConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for MySQL sink option construction. No database is required.
 */
class MysqlSinkTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "orders-db",
      `type` = "mysql",
      endpoint = Some("mysql:3306"),
      auth = Map.empty,
      properties = Map.empty,
      options = Map(
        "url" -> "jdbc:mysql://mysql:3306/orders",
        "user" -> "app",
        "password" -> "secret",
        "rewriteBatchedStatements" -> "true"))
  }

  test("register the mysql-sink identifier") {
    new MysqlSink().identifier shouldBe "mysql-sink"
    new MysqlSink().supportsConnection shouldBe true
    DataSink.identifiers should contain ("mysql-sink")
  }

  test("build writer options from a connection") {
    val options = MysqlSink.writerOptions(SinkContext(
      config = Some(Map[String, Any]("table" -> "orders")),
      connection = Some(connection)))

    options("url") shouldBe "jdbc:mysql://mysql:3306/orders"
    options("dbtable") shouldBe "orders"
    options("user") shouldBe "app"
    options("password") shouldBe "secret"
    options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
    options("rewriteBatchedStatements") shouldBe "true"
  }

  test("build legacy options and default the MySQL driver") {
    val options = MysqlSink.writerOptions(SinkContext(Some(Map[String, Any](
      "url" -> "jdbc:mysql://localhost:3306/orders",
      "table" -> "orders",
      "user" -> "app",
      "password" -> "secret"))))

    options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
  }

  test("reject a non-jdbc connection and missing configuration") {
    val wrongConnection = intercept[PuddingException] {
      MysqlSink.writerOptions(SinkContext(
        config = Some(Map[String, Any]("table" -> "orders")),
        connection = Some(connection.copy(`type` = "elasticsearch"))))
    }
    wrongConnection.getMessage should include ("requires a jdbc or mysql connection")

    intercept[PuddingException] {
      MysqlSink.writerOptions(SinkContext(None))
    }
  }

  test("resolve save modes through the shared JDBC configuration") {
    JdbcConnectorConfig.saveMode(Map("mode" -> "Append")) shouldBe "append"
    JdbcConnectorConfig.saveMode(Map.empty) shouldBe "overwrite"
    intercept[PuddingException] {
      JdbcConnectorConfig.saveMode(Map("mode" -> "merge"))
    }
  }
}
