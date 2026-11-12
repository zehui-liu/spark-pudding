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

package org.pudding.client.submit

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests the Java/Scala bridge used for connector and transform jar selection.
 */
class ScalaInteropTest extends AnyFunSuite with Matchers {

  test("select the basic transform jar for a basic transform type") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: basic transform jar selection
        |source:
        |  - type: file-common-source
        |    id: source-1
        |transform:
        |  - type: filter-transform
        |    id: transform-1
        |    dependencies: [source-1]
        |    config:
        |      condition: "id > 0"
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: transform-1
        |""".stripMargin))

    ScalaInterop.connectorTypes(pipeline).asScala should contain ("basic-transform")
  }

  test("sql-transform does not select the basic transform jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: sql transform jar selection
        |source:
        |  - type: file-common-source
        |    id: source-1
        |transform:
        |  - type: sql-transform
        |    id: transform-1
        |    dependencies: [source-1]
        |    config:
        |      sql: "select * from source_1"
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: transform-1
        |""".stripMargin))

    ScalaInterop.connectorTypes(pipeline).asScala should not contain ("basic-transform")
  }

  test("always select built-in connection and catalog provider jars") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: provider jar selection
        |source:
        |  - type: file-common-source
        |    id: source-1
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain allOf ("connection-providers", "catalog-providers")
    types should not contain ("hudi-runtime")
    types should not contain ("paimon-runtime")
    types should not contain ("iceberg-runtime")
    types should not contain ("elasticsearch-runtime")
    types should not contain ("mongodb-runtime")
    types should not contain ("kafka-runtime")
  }

  test("JDBC and MySQL connectors select the shared jdbc-common jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: jdbc common jar selection
        |source:
        |  - type: jdbc-source
        |    id: jdbc-source
        |  - type: mysql-source
        |    id: mysql-source
        |sink:
        |  - type: jdbc-sink
        |    id: jdbc-sink
        |    dependency: jdbc-source
        |  - type: mysql-sink
        |    id: mysql-sink
        |    dependency: mysql-source
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain ("jdbc-common")
    types.count(_ == "jdbc-common") shouldBe 1
  }

  test("a JDBC catalog selects its driver jar and shared helper") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: jdbc catalog jar selection
        |catalog:
        |  - name: pg_catalog
        |    type: jdbc
        |source:
        |  - type: file-common-source
        |    id: source-1
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain allOf ("jdbc-source", "jdbc-common")
    types should not contain ("jdbc-sink")
  }

  test("a Hudi catalog does not add a second Hudi runtime jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: hudi catalog runtime selection
        |source:
        |  - type: file-common-source
        |    id: source-1
        |catalog:
        |  - name: hudi_lake
        |    type: hudi
        |sink:
        |  - type: hudi-sink
        |    id: sink-1
        |    catalog: hudi_lake
        |    config:
        |      table: orders
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain ("hudi-sink")
    types should contain ("hudi-runtime")
    types should not contain ("hudi-source")
  }

  test("a Paimon catalog does not add a second Paimon runtime jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: paimon catalog runtime selection
        |source:
        |  - type: file-common-source
        |    id: source-1
        |catalog:
        |  - name: paimon_lake
        |    type: paimon
        |    properties:
        |      warehouse: "file:/tmp/paimon-warehouse"
        |sink:
        |  - type: paimon-sink
        |    id: sink-1
        |    catalog: paimon_lake
        |    config:
        |      table: orders
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain ("paimon-sink")
    types should contain ("paimon-runtime")
    types should not contain ("paimon-source")
  }

  test("a catalog without a matching connector selects its runtime jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: hudi catalog runtime fallback
        |source:
        |  - type: file-common-source
        |    id: source-1
        |catalog:
        |  - name: hudi_lake
        |    type: hudi
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain ("hudi-runtime")
    types should not contain ("hudi-source")
  }

  test("Elasticsearch, MongoDB and Kafka connectors select their shared runtimes") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: external connector runtime selection
        |source:
        |  - type: kafka-source
        |    id: kafka-source
        |  - type: elasticsearch-source
        |    id: es-source
        |  - type: mongodb-source
        |    id: mongo-source
        |sink:
        |  - type: kafka-sink
        |    id: kafka-sink
        |    dependency: kafka-source
        |  - type: elasticsearch-sink
        |    id: es-sink
        |    dependency: es-source
        |  - type: mongodb-sink
        |    id: mongo-sink
        |    dependency: mongo-source
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain allOf (
      "kafka-source", "kafka-sink", "kafka-runtime",
      "elasticsearch-source", "elasticsearch-sink", "elasticsearch-runtime",
      "mongodb-source", "mongodb-sink", "mongodb-runtime")
  }

  test("Kafka Avro and Protobuf values select the optional schema registry runtime") {
    Seq("avro", "protobuf").foreach { format =>
      val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
        "yaml",
        s"""
           |job:
           |  mode: batch
           |  appName: schema registry runtime selection
           |source:
           |  - type: kafka-source
           |    id: kafka-source
           |    config:
           |      subscribe: orders
           |      valueFormat: $format
           |      valueSchema:
           |        id: long
           |      schemaRegistry:
           |        url: http://registry:8081
           |sink:
           |  - type: console-sink
           |    id: sink
           |    dependency: kafka-source
           |""".stripMargin))

      val types = ScalaInterop.connectorTypes(pipeline).asScala
      types should contain allOf ("kafka-runtime", "kafka-schema-registry-runtime")
    }

    val jsonPipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: json kafka runtime selection
        |source:
        |  - type: kafka-source
        |    id: kafka-source
        |    config:
        |      subscribe: orders
        |      valueFormat: json
        |      valueSchema:
        |        id: long
        |sink:
        |  - type: console-sink
        |    id: sink
        |    dependency: kafka-source
        |""".stripMargin))
    val jsonTypes = ScalaInterop.connectorTypes(jsonPipeline).asScala
    jsonTypes should contain ("kafka-runtime")
    jsonTypes should not contain ("kafka-schema-registry-runtime")
  }

  test("native CDC sources select the shared cdc-runtime jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: streaming
        |  appName: cdc runtime selection
        |source:
        |  - type: mysql-cdc-source
        |    id: source-1
        |    config:
        |      schema: "id int"
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain allOf ("mysql-cdc-source", "cdc-runtime")
  }

  test("a Hudi source and sink select one shared runtime jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: hudi shared runtime selection
        |source:
        |  - type: hudi-source
        |    id: source-1
        |    config:
        |      table: source_orders
        |      tablePath: /tmp/source-orders
        |sink:
        |  - type: hudi-sink
        |    id: sink-1
        |    config:
        |      table: target_orders
        |      tablePath: /tmp/target-orders
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain allOf ("hudi-source", "hudi-sink", "hudi-runtime")
  }

  test("an Iceberg catalog selects the shared Iceberg runtime jar") {
    val pipeline = ScalaInterop.parse(JobConfiguration.fromText(
      "yaml",
      """
        |job:
        |  mode: batch
        |  appName: iceberg catalog runtime selection
        |source:
        |  - type: file-common-source
        |    id: source-1
        |catalog:
        |  - name: lake
        |    type: iceberg
        |    properties:
        |      type: hadoop
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin))

    val types = ScalaInterop.connectorTypes(pipeline).asScala
    types should contain ("iceberg-runtime")
    types should not contain ("iceberg-source")
  }
}
