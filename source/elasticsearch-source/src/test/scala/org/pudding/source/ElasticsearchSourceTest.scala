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
 * Tests for Elasticsearch source option construction. No cluster is required.
 */
class ElasticsearchSourceTest extends AnyFunSuite with Matchers {

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "es",
      `type` = "elasticsearch",
      endpoint = Some("https://es:9200"),
      auth = Map("username" -> "elastic", "password" -> "secret"),
      properties = Map.empty,
      options = Map(
        "es.nodes" -> "es-1,es-2",
        "es.port" -> "9200",
        "es.net.ssl" -> "true",
        "es.net.http.auth.user" -> "elastic",
        "es.net.http.auth.pass" -> "secret"))
  }

  test("register the elasticsearch-source identifier") {
    new ElasticsearchSource().identifier shouldBe "elasticsearch-source"
    new ElasticsearchSource().supportsConnection shouldBe true
    DataSource.identifiers should contain ("elasticsearch-source")
  }

  test("build reader options from a connection") {
    val options = ElasticsearchSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "esIndex" -> "logs-*",
        "fields" -> Seq("timestamp", "message"),
        "scrollSize" -> 500,
        "options" -> Map("es.nodes" -> "ignored", "es.read.metadata" -> "true"))),
      connection = Some(connection)))

    options("es.resource") shouldBe "logs-*"
    options("es.nodes") shouldBe "es-1,es-2"
    options("es.net.ssl") shouldBe "true"
    options("es.net.http.auth.user") shouldBe "elastic"
    options("es.net.http.auth.pass") shouldBe "secret"
    options("es.read.field.include") shouldBe "timestamp,message"
    options("es.scroll.size") shouldBe "500"
    options("es.read.metadata") shouldBe "true"
  }

  test("build legacy options without a connection") {
    val options = ElasticsearchSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "esIndex" -> "logs",
        "esNodes" -> "es-1,es-2",
        "esPort" -> 9201,
        "esUser" -> "elastic",
        "esPass" -> "secret",
        "fields" -> "a, b, c"))))

    options("es.nodes") shouldBe "es-1,es-2"
    options("es.port") shouldBe "9201"
    options("es.net.http.auth.user") shouldBe "elastic"
    options("es.net.http.auth.pass") shouldBe "secret"
    options("es.read.field.include") shouldBe "a,b,c"
  }

  test("reject null option values and invalid option maps") {
    val nullValue = intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "esIndex" -> "logs",
          "esNodes" -> "es",
          "options" -> Map("es.nodes" -> null)))))
    }
    nullValue.getMessage should include ("null key or value")

    val notMap = intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "esIndex" -> "logs",
          "esNodes" -> "es",
          "options" -> "not-a-map"))))
    }
    notMap.getMessage should include ("must be a map")
  }

  test("reject a non-elasticsearch connection and missing required values") {
    val error = intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(
        config = Some(Map("esIndex" -> "logs")),
        connection = Some(connection.copy(`type` = "jdbc"))))
    }
    error.getMessage should include ("requires an elasticsearch connection")

    intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(Some(Map("esNodes" -> "es"))))
    }
    intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(Some(Map("esIndex" -> "logs"))))
    }
  }

  test("validate the scroll size and field list types") {
    intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(Some(Map(
        "esIndex" -> "logs",
        "esNodes" -> "es",
        "scrollSize" -> "many"))))
    }
    intercept[PuddingException] {
      ElasticsearchSource.readerOptions(SourceContext(Some(Map(
        "esIndex" -> "logs",
        "esNodes" -> "es",
        "scrollSize" -> 0))))
    }

    val options = ElasticsearchSource.readerOptions(SourceContext(Some(Map(
      "esIndex" -> "logs",
      "esNodes" -> "es",
      "fields" -> Seq("a", 2, "b"),
      "scrollSize" -> "100"))))
    options("es.read.field.include") shouldBe "a,2,b"
    options("es.scroll.size") shouldBe "100"
  }
}
