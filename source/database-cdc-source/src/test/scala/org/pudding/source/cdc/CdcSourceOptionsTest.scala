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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.SourceContext

class CdcSourceOptionsTest extends AnyFunSuite with Matchers {

  test("inline MySQL configuration builds normalized source options") {
    val (options, schema, opColumn) = CdcSourceOptions.resolve(
      SourceContext(Some(Map[String, Any](
        "host" -> "mysql",
        "port" -> 3306,
        "database" -> "inventory",
        "table" -> "orders",
        "user" -> "pudding",
        "password" -> "secret",
        "schema" -> "id int, name string"))),
      "mysql")

    schema.fieldNames.toSeq shouldBe Seq("id", "name")
    opColumn shouldBe "__op"
    options(CdcConfig.HostKey) shouldBe "mysql"
    options(CdcConfig.DatabaseKey) shouldBe "inventory"
    options(CdcConfig.TableNameKey) shouldBe "orders"
    options(CdcConfig.TableKey) shouldBe "inventory.orders"
    options(CdcConfig.PasswordKey) shouldBe "secret"
    options(CdcConfig.ServerIdKey).toLong should be > 0L
  }

  test("a MySQL connection URL supplies host, port and database") {
    val connection = ResolvedConnection(
      name = "mysql",
      `type` = "mysql",
      endpoint = Some("jdbc:mysql://mysql.internal:3307/inventory"),
      auth = Map("user" -> "pudding", "password" -> "secret"),
      properties = Map.empty,
      options = Map(
        "url" -> "jdbc:mysql://mysql.internal:3307/inventory",
        "user" -> "pudding",
        "password" -> "secret"))
    val (options, _, _) = CdcSourceOptions.resolve(
      SourceContext(
        Some(Map[String, Any](
          "table" -> "orders",
          "schema" -> "id int")),
        connection = Some(connection)),
      "mysql")

    options(CdcConfig.HostKey) shouldBe "mysql.internal"
    options(CdcConfig.PortKey) shouldBe "3307"
    options(CdcConfig.DatabaseKey) shouldBe "inventory"
  }

  test("PostgreSQL requires a postgres JDBC URL and accepts a schema-qualified table") {
    val connection = ResolvedConnection(
      name = "postgres",
      `type` = "jdbc",
      endpoint = Some("jdbc:postgresql://postgres:5432/inventory"),
      auth = Map("user" -> "pudding", "password" -> "secret"),
      properties = Map.empty,
      options = Map(
        "url" -> "jdbc:postgresql://postgres:5432/inventory",
        "user" -> "pudding",
        "password" -> "secret"))
    val (options, _, _) = CdcSourceOptions.resolve(
      SourceContext(
        Some(Map[String, Any](
          "table" -> "sales.orders",
          "schema" -> "id int")),
        connection = Some(connection)),
      "postgres")

    options(CdcConfig.DatabaseKey) shouldBe "inventory"
    options(CdcConfig.SchemaNameKey) shouldBe "sales"
    options(CdcConfig.TableNameKey) shouldBe "orders"
    options(CdcConfig.TableKey) shouldBe "inventory.sales.orders"
  }

  test("MongoDB requires _id in the declared schema") {
    val context = SourceContext(Some(Map[String, Any](
      "uri" -> "mongodb://mongo:27017/?replicaSet=rs0",
      "database" -> "inventory",
      "collection" -> "orders",
      "schema" -> "name string")))
    val error = intercept[PuddingException] {
      CdcSourceOptions.resolve(context, "mongodb")
    }
    error.getMessage should include ("_id")
  }

  test("missing or reserved schema columns fail with key names") {
    val missing = intercept[PuddingException] {
      CdcSourceOptions.resolve(SourceContext(Some(Map.empty)), "mysql")
    }
    missing.getMessage should include ("schema")

    val reserved = intercept[PuddingException] {
      CdcSourceOptions.resolve(
        SourceContext(Some(Map[String, Any](
          "host" -> "mysql",
          "database" -> "inventory",
          "table" -> "orders",
          "user" -> "pudding",
          "schema" -> "id int, _cdc_timestamp long"))),
        "mysql")
    }
    reserved.getMessage should include ("_cdc_timestamp")
  }

  test("queue capacity keeps headroom above the micro-batch limit") {
    CdcConfig.queueCapacity(1000, 10000) shouldBe 10000
    CdcConfig.queueCapacity(20000, 20000) shouldBe 20001
    CdcConfig.queueCapacity(20000, 50000) shouldBe 50000
    CdcConfig.queueCapacity(Int.MaxValue, Int.MaxValue) shouldBe Int.MaxValue
  }
}
