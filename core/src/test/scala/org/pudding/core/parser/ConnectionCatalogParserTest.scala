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

package org.pudding.core.parser

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the shared connection and catalog sections.
 */
class ConnectionCatalogParserTest extends AnyFunSuite with Matchers {

  private val yaml =
    """
      |job:
      |  mode: batch
      |  appName: connection parser test
      |
      |connection:
      |  - name: kafka-prod
      |    type: kafka
      |    describe: production kafka
      |    endpoint: broker-1:9092,broker-2:9092
      |    auth:
      |      username: pudding
      |      password: ${secret:env/KAFKA_PASSWORD}
      |    properties:
      |      security.protocol: SASL_SSL
      |      retries: 3
      |
      |  - name: hms-prod
      |    type: hive-metastore
      |    endpoint: thrift://hms:9083
      |
      |catalog:
      |  - name: lake
      |    type: iceberg
      |    connection: hms-prod
      |    properties:
      |      warehouse: hdfs://nn/warehouse
      |      port: 9083
      |
      |source:
      |  - type: kafka-source
      |    id: events
      |    connection: kafka-prod
      |    config:
      |      subscribe: orders
      |
      |sink:
      |  - type: console-sink
      |    id: output
      |    dependency: events
      |    catalog: lake
      |""".stripMargin

  test("parse connection and catalog sections") {
    val pipeline = new YamlJobConfigureParser().parseFromString(yaml)

    pipeline.connections.map(_.name) shouldBe Seq("kafka-prod", "hms-prod")
    pipeline.catalogs.map(_.name) shouldBe Seq("lake")

    val kafka = pipeline.connections.head
    kafka.`type` shouldBe "kafka"
    kafka.describe shouldBe Some("production kafka")
    kafka.endpoint shouldBe Some("broker-1:9092,broker-2:9092")
    kafka.auth("username") shouldBe "pudding"
    kafka.auth("password") shouldBe "${secret:env/KAFKA_PASSWORD}"
    kafka.properties("security.protocol") shouldBe "SASL_SSL"
    kafka.properties("retries") shouldBe "3"

    val lake = pipeline.catalogs.head
    lake.`type` shouldBe "iceberg"
    lake.connection shouldBe Some("hms-prod")
    lake.properties("warehouse") shouldBe "hdfs://nn/warehouse"
    lake.properties("port") shouldBe "9083"
  }

  test("parse source and sink references") {
    val pipeline = new YamlJobConfigureParser().parseFromString(yaml)

    pipeline.sources.head.connection shouldBe Some("kafka-prod")
    pipeline.sources.head.catalog shouldBe None
    pipeline.sinks.head.catalog shouldBe Some("lake")
    pipeline.sinks.head.connection shouldBe None
  }

  test("connection and catalog sections are optional") {
    val minimal =
      """
        |job:
        |  mode: batch
        |  appName: minimal
        |source:
        |  - type: file-common-source
        |    id: source-1
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val pipeline = new YamlJobConfigureParser().parseFromString(minimal)

    pipeline.connections shouldBe empty
    pipeline.catalogs shouldBe empty
    pipeline.sources.head.connection shouldBe None
    pipeline.sources.head.catalog shouldBe None
    pipeline.sinks.head.connection shouldBe None
    pipeline.sinks.head.catalog shouldBe None
  }

  test("connection names may also use id") {
    val conf =
      """
        |job:
        |  mode: batch
        |  appName: connection id test
        |
        |connection:
        |  - id: es-logs
        |    type: elasticsearch
        |    endpoint: http://es:9200
        |catalog:
        |  - id: lake
        |    type: iceberg
        |    properties:
        |      type: hadoop
        |source:
        |  - type: elasticsearch-source
        |    id: logs
        |    connection: es-logs
        |sink:
        |  - type: console-sink
        |    id: output
        |    dependency: logs
        |""".stripMargin

    val pipeline = new YamlJobConfigureParser().parseFromString(conf)

    pipeline.connections.head.name shouldBe "es-logs"
    pipeline.catalogs.head.name shouldBe "lake"
  }

  test("reject a non-map properties section") {
    val conf =
      """
        |job:
        |  mode: batch
        |  appName: broken connection test
        |
        |connection:
        |  - name: broken
        |    type: kafka
        |    properties: not-a-map
        |source:
        |  - type: file-common-source
        |    id: source-1
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val error = intercept[RuntimeException] {
      new YamlJobConfigureParser().parseFromString(conf)
    }

    error.getMessage should include ("properties must be a map")
  }
}
