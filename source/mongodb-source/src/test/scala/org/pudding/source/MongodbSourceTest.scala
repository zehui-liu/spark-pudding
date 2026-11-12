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
 * Tests for MongoDB source option construction. No MongoDB server is required.
 */
class MongodbSourceTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "mongo",
      `type` = "mongodb",
      endpoint = Some("mongodb://mongo:27017"),
      auth = Map("username" -> "pudding", "password" -> "secret"),
      properties = Map.empty,
      options = Map(
        "connection.uri" -> "mongodb://pudding:secret@mongo:27017",
        "spark.mongodb.read.readPreference.name" -> "secondaryPreferred"))
  }

  test("register the mongodb-source identifier") {
    new MongodbSource().identifier shouldBe "mongodb-source"
    new MongodbSource().supportsConnection shouldBe true
    DataSource.identifiers should contain ("mongodb-source")
  }

  test("build reader options from a connection") {
    val options = MongodbSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "database" -> "app",
        "collection" -> "orders",
        "options" -> Map("partition.size" -> "2"))),
      connection = Some(connection)))

    options("spark.mongodb.read.connection.uri") shouldBe
      "mongodb://pudding:secret@mongo:27017"
    options("spark.mongodb.read.database") shouldBe "app"
    options("spark.mongodb.read.collection") shouldBe "orders"
    options("spark.mongodb.read.readPreference.name") shouldBe "secondaryPreferred"
    options("spark.mongodb.read.partition.size") shouldBe "2"
  }

  test("accept an inline URI when no connection is configured") {
    val options = MongodbSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "uri" -> "mongodb://localhost:27017",
        "database" -> "app",
        "collection" -> "orders"))))

    options("spark.mongodb.read.connection.uri") shouldBe "mongodb://localhost:27017"
  }

  test("reject a non-mongodb connection") {
    val error = intercept[PuddingException] {
      MongodbSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "database" -> "app",
          "collection" -> "orders")),
        connection = Some(connection.copy(`type` = "jdbc"))))
    }
    error.getMessage should include ("requires a mongodb connection")
  }

  test("reject invalid option maps") {
    val nullValue = intercept[PuddingException] {
      MongodbSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "uri" -> "mongodb://localhost:27017",
          "database" -> "app",
          "collection" -> "orders",
          "options" -> Map("readPreference.name" -> null)))))
    }
    nullValue.getMessage should include ("null key or value")

    val notMap = intercept[PuddingException] {
      MongodbSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "uri" -> "mongodb://localhost:27017",
          "database" -> "app",
          "collection" -> "orders",
          "options" -> "not-a-map"))))
    }
    notMap.getMessage should include ("must be a map")
  }

  test("require a URI, database and collection") {
    intercept[PuddingException] {
      MongodbSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "database" -> "app",
          "collection" -> "orders"))))
    }

    intercept[PuddingException] {
      MongodbSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "uri" -> "mongodb://localhost:27017",
          "collection" -> "orders"))))
    }

    intercept[PuddingException] {
      MongodbSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "uri" -> "mongodb://localhost:27017",
          "database" -> "app"))))
    }
  }
}
