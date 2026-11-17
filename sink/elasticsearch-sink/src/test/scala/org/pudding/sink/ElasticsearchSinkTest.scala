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
 * Tests for Elasticsearch sink option construction and validation. No Elasticsearch cluster is
 * required.
 */
class ElasticsearchSinkTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "es",
      `type` = "elasticsearch",
      endpoint = Some("es:9200"),
      auth = Map.empty,
      properties = Map.empty,
      options = Map(
        "es.nodes" -> "es:9200",
        "es.net.ssl" -> "true",
        "es.batch.size.entries" -> "500"))
  }

  test("register the elasticsearch-sink identifier") {
    new ElasticsearchSink().identifier shouldBe "elasticsearch-sink"
    new ElasticsearchSink().supportsConnection shouldBe true
    DataSink.identifiers should contain ("elasticsearch-sink")
  }

  test("build options from an elasticsearch connection") {
    val options = ElasticsearchSink.writerOptions(
      SinkContext(
        config = Some(Map[String, Any](
          "esIndex" -> "logs-2026",
          "operation" -> "upsert",
          "idField" -> "id",
          "options" -> Map(
            "es.nodes" -> "ignored:9200",
            "es.batch.size.bytes" -> "1mb"))),
        connection = Some(connection)),
      "logs-2026")

    options("es.resource") shouldBe "logs-2026"
    options("es.nodes") shouldBe "es:9200"
    options("es.net.ssl") shouldBe "true"
    options("es.batch.size.entries") shouldBe "500"
    options("es.batch.size.bytes") shouldBe "1mb"
    options("es.write.operation") shouldBe "upsert"
    options("es.mapping.id") shouldBe "id"
  }

  test("build legacy connection options when no connection is configured") {
    val options = ElasticsearchSink.writerOptions(
      SinkContext(
        config = Some(Map[String, Any](
          "esIndex" -> "logs",
          "esNodes" -> "es-1,es-2",
          "esPort" -> 9201,
          "esUser" -> "elastic",
          "esPass" -> "secret"))),
      "logs")

    options("es.nodes") shouldBe "es-1,es-2"
    options("es.port") shouldBe "9201"
    options("es.net.http.auth.user") shouldBe "elastic"
    options("es.net.http.auth.pass") shouldBe "secret"
  }

  test("reject invalid option maps") {
    val nullValue = intercept[PuddingException] {
      ElasticsearchSink.writerOptions(
        SinkContext(Some(Map[String, Any](
          "esIndex" -> "logs",
          "esNodes" -> "es",
          "options" -> Map("es.nodes" -> null)))),
        "logs")
    }
    nullValue.getMessage should include ("null key or value")

    val notMap = intercept[PuddingException] {
      ElasticsearchSink.writerOptions(
        SinkContext(Some(Map[String, Any](
          "esIndex" -> "logs",
          "esNodes" -> "es",
          "options" -> "not-a-map"))),
        "logs")
    }
    notMap.getMessage should include ("must be a map")
  }

  test("reject a non-elasticsearch connection") {
    val error = intercept[PuddingException] {
      ElasticsearchSink.writerOptions(
        SinkContext(
          config = Some(Map[String, Any]("esIndex" -> "logs")),
          connection = Some(connection.copy(`type` = "jdbc"))),
        "logs")
    }
    error.getMessage should include ("requires an elasticsearch connection")
  }

  test("validate operations and save modes") {
    val error = intercept[PuddingException] {
      ElasticsearchSink.writerOptions(
        SinkContext(
          config = Some(Map[String, Any](
            "esIndex" -> "logs",
            "operation" -> "append")),
          connection = Some(connection)),
        "logs")
    }
    error.getMessage should include ("unsupported elasticsearch operation")

    ElasticsearchSink.saveMode(Map("mode" -> "overwrite")) shouldBe SaveMode.Overwrite
    ElasticsearchSink.saveMode(Map("mode" -> "errorifexists")) shouldBe SaveMode.ErrorIfExists
    ElasticsearchSink.saveMode(Map.empty) shouldBe SaveMode.Append
    intercept[PuddingException] {
      ElasticsearchSink.saveMode(Map("mode" -> "bad"))
    }
  }

  test("validate streaming settings") {
    ElasticsearchSink.streamingEnabled(Map("isStreaming" -> true)) shouldBe true
    ElasticsearchSink.streamingEnabled(Map("streamWrite" -> "false")) shouldBe false
    ElasticsearchSink.outputMode(Map.empty) shouldBe "append"
    ElasticsearchSink.outputMode(
      Map("outputMode" -> "update", "idField" -> "id")) shouldBe "update"
    intercept[PuddingException] {
      ElasticsearchSink.outputMode(Map("outputMode" -> "update"))
    }.getMessage should include ("requires idField")
    intercept[PuddingException] {
      ElasticsearchSink.outputMode(Map("outputMode" -> "complete"))
    }
  }
}
