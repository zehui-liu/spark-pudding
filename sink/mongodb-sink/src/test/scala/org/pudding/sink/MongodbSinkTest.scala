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

package org.pudding.sink

import org.apache.spark.sql.SaveMode

import org.pudding.core.PuddingException
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.{DataSink, SinkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for MongoDB sink option construction and validation. No MongoDB server is required.
 */
class MongodbSinkTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "mongo",
      `type` = "mongodb",
      endpoint = Some("mongodb://mongo:27017"),
      auth = Map("username" -> "pudding", "password" -> "secret"),
      properties = Map.empty,
      options = Map(
        "connection.uri" -> "mongodb://pudding:secret@mongo:27017",
        "spark.mongodb.read.readPreference.name" -> "secondaryPreferred",
        "spark.mongodb.write.writeConcern.w" -> "majority"))
  }

  test("register the mongodb-sink identifier") {
    new MongodbSink().identifier shouldBe "mongodb-sink"
    new MongodbSink().supportsConnection shouldBe true
    DataSink.identifiers should contain ("mongodb-sink")
  }

  test("build writer options from a connection") {
    val options = MongodbSink.writerOptions(SinkContext(
      config = Some(Map[String, Any](
        "database" -> "app",
        "collection" -> "orders",
        "operation" -> "update",
        "idFields" -> Seq("id", "dt"),
        "upsert" -> true,
        "options" -> Map(
          "maxBatchSize" -> "100",
          "ordered" -> "false"))),
      connection = Some(connection)))

    options("spark.mongodb.write.connection.uri") shouldBe
      "mongodb://pudding:secret@mongo:27017"
    options("spark.mongodb.write.database") shouldBe "app"
    options("spark.mongodb.write.collection") shouldBe "orders"
    options("spark.mongodb.write.operationType") shouldBe "update"
    options("spark.mongodb.write.idFieldList") shouldBe "id,dt"
    options("spark.mongodb.write.upsertDocument") shouldBe "true"
    options("spark.mongodb.write.maxBatchSize") shouldBe "100"
    options("spark.mongodb.write.ordered") shouldBe "false"
    options("spark.mongodb.write.writeConcern.w") shouldBe "majority"
    options should not contain key ("spark.mongodb.write.readPreference.name")
  }

  test("accept an inline URI when no connection is configured") {
    val options = MongodbSink.writerOptions(SinkContext(
      config = Some(Map[String, Any](
        "uri" -> "mongodb://localhost:27017",
        "database" -> "app",
        "collection" -> "orders"))))

    options("spark.mongodb.write.connection.uri") shouldBe "mongodb://localhost:27017"
  }

  test("validate required fields and connection type") {
    intercept[PuddingException] {
      MongodbSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "database" -> "app",
          "collection" -> "orders"))))
    }
    intercept[PuddingException] {
      MongodbSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "uri" -> "mongodb://localhost:27017",
          "collection" -> "orders"))))
    }
    intercept[PuddingException] {
      MongodbSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "uri" -> "mongodb://localhost:27017",
          "database" -> "app"))))
    }
    val error = intercept[PuddingException] {
      MongodbSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "database" -> "app",
          "collection" -> "orders")),
        connection = Some(connection.copy(`type` = "jdbc"))))
    }
    error.getMessage should include ("requires a mongodb connection")
  }

  test("validate operation, save modes and streaming settings") {
    intercept[PuddingException] {
      MongodbSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "database" -> "app",
          "collection" -> "orders",
          "operation" -> "upsert")),
        connection = Some(connection)))
    }
    MongodbSink.saveMode(Map("mode" -> "overwrite")) shouldBe SaveMode.Overwrite
    MongodbSink.saveMode(Map.empty) shouldBe SaveMode.Append
    intercept[PuddingException] {
      MongodbSink.saveMode(Map("mode" -> "bad"))
    }
    MongodbSink.streamingEnabled(Map("isStreaming" -> "true")) shouldBe true
    MongodbSink.outputMode(Map.empty) shouldBe "append"
    intercept[PuddingException] {
      MongodbSink.outputMode(Map("outputMode" -> "complete"))
    }
  }
}
