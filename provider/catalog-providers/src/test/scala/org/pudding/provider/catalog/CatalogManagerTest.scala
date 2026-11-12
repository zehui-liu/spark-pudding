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

package org.pudding.provider.catalog

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import org.apache.spark.SparkConf
import org.pudding.core.PuddingException
import org.pudding.core.catalog.CatalogManager
import org.pudding.core.configure.{CatalogConf, ConnectionConf}
import org.pudding.core.connection.ConnectionRegistry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for native catalog resolution and the Gravitino REST resolver.
 */
class CatalogManagerTest extends AnyFunSuite with Matchers {

  private def hiveConnection(name: String = "hms"): ConnectionConf = {
    ConnectionConf(
      name = name,
      `type` = "hive-metastore",
      endpoint = Some("thrift://hms:9083"),
      properties = Map("metastore.sasl.enabled" -> "true"))
  }

  private def jdbcConnection(name: String = "pg"): ConnectionConf = {
    ConnectionConf(
      name = name,
      `type` = "jdbc",
      endpoint = Some("jdbc:postgresql://db:5432/app"),
      auth = Map("username" -> "pudding", "password" -> "secret"),
      properties = Map("driver" -> "org.postgresql.Driver"))
  }

  test("resolve a Hive catalog from a metastore connection") {
    val registry = ConnectionRegistry(Seq(hiveConnection()))
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "hive_prod",
        `type` = "hive",
        connection = Some("hms"),
        properties = Map("warehouse" -> "hdfs://nn/warehouse"))),
      registry)

    catalogs.size shouldBe 1
    val hive = catalogs.head
    hive.`type` shouldBe "hive"
    hive.sessionCatalog shouldBe true
    hive.sparkConf("spark.sql.catalogImplementation") shouldBe "hive"
    hive.sparkConf("hive.metastore.uris") shouldBe "thrift://hms:9083"
    hive.sparkConf("spark.hadoop.hive.metastore.uris") shouldBe "thrift://hms:9083"
    hive.sparkConf("hive.metastore.sasl.enabled") shouldBe "true"
    hive.sparkConf("spark.sql.warehouse.dir") shouldBe "hdfs://nn/warehouse"
  }

  test("resolve an Iceberg catalog and register the extension") {
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "lake",
        `type` = "iceberg",
        properties = Map("type" -> "hadoop", "warehouse" -> "s3://bucket/warehouse"))),
      ConnectionRegistry.Empty)

    val iceberg = catalogs.head
    iceberg.`type` shouldBe "iceberg"
    iceberg.sparkCatalogName shouldBe "lake"
    iceberg.sessionCatalog shouldBe false
    iceberg.sparkConf("spark.sql.catalog.lake") shouldBe "org.apache.iceberg.spark.SparkCatalog"
    iceberg.sparkConf("spark.sql.catalog.lake.type") shouldBe "hadoop"
    iceberg.sparkConf("spark.sql.catalog.lake.warehouse") shouldBe "s3://bucket/warehouse"
    iceberg.sparkConf("spark.sql.extensions") shouldBe
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }

  test("resolve an Iceberg catalog backed by a Hive metastore") {
    val registry = ConnectionRegistry(Seq(hiveConnection()))
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "lake_hms",
        `type` = "iceberg",
        connection = Some("hms"))),
      registry)

    val iceberg = catalogs.head
    iceberg.sparkConf("spark.sql.catalog.lake_hms.type") shouldBe "hive"
    iceberg.sparkConf("spark.sql.catalog.lake_hms.uri") shouldBe "thrift://hms:9083"
  }

  test("resolve a JDBC catalog from a JDBC connection") {
    val registry = ConnectionRegistry(Seq(jdbcConnection()))
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "pg_catalog",
        `type` = "jdbc",
        connection = Some("pg"))),
      registry)

    val jdbc = catalogs.head
    jdbc.`type` shouldBe "jdbc"
    jdbc.sparkConf("spark.sql.catalog.pg_catalog") shouldBe
      "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
    jdbc.sparkConf("spark.sql.catalog.pg_catalog.url") shouldBe
      "jdbc:postgresql://db:5432/app"
    jdbc.sparkConf("spark.sql.catalog.pg_catalog.user") shouldBe "pudding"
    jdbc.sparkConf("spark.sql.catalog.pg_catalog.password") shouldBe "secret"
    jdbc.sparkConf("spark.sql.catalog.pg_catalog.driver") shouldBe "org.postgresql.Driver"
  }

  test("reject more than one Hive session catalog") {
    val registry = ConnectionRegistry(Seq(hiveConnection("hms-1"), hiveConnection("hms-2")))
    val error = intercept[PuddingException] {
      CatalogManager.resolve(
        Seq(
          CatalogConf("hive-1", "hive", connection = Some("hms-1")),
          CatalogConf("hive-2", "hive", connection = Some("hms-2"))),
        registry)
    }
    error.getMessage should include ("only one session catalog")
  }

  test("resolve a Hudi catalog") {
    val registry = ConnectionRegistry(Seq(hiveConnection()))
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "hudi_prod",
        `type` = "hudi",
        connection = Some("hms"),
        properties = Map("warehouse" -> "hdfs://nn/warehouse"))),
      registry)

    val hudi = catalogs.head
    hudi.`type` shouldBe "hudi"
    hudi.sessionCatalog shouldBe true
    hudi.sparkCatalogName shouldBe "spark_catalog"
    hudi.sparkConf("spark.sql.catalog.spark_catalog") shouldBe
      "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    hudi.sparkConf("spark.sql.extensions") shouldBe
      "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    hudi.sparkConf("spark.serializer") shouldBe
      "org.apache.spark.serializer.KryoSerializer"
    hudi.sparkConf("spark.kryo.registrator") shouldBe
      "org.apache.spark.HoodieSparkKryoRegistrar"
    hudi.sparkConf("spark.sql.catalogImplementation") shouldBe "hive"
    hudi.sparkConf("hive.metastore.uris") shouldBe "thrift://hms:9083"
    hudi.sparkConf("spark.sql.warehouse.dir") shouldBe "hdfs://nn/warehouse"
  }

  test("Hudi catalog settings can be disabled") {
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "hudi_prod",
        `type` = "hudi",
        properties = Map(
          "enableHiveSupport" -> "false",
          "setSerializer" -> "false",
          "setKryoRegistrator" -> "false"))),
      ConnectionRegistry.Empty)

    val hudi = catalogs.head
    hudi.sparkConf should not contain key ("spark.sql.catalogImplementation")
    hudi.sparkConf should not contain key ("spark.serializer")
    hudi.sparkConf should not contain key ("spark.kryo.registrator")
  }

  test("resolve a named Paimon catalog") {
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "lake",
        `type` = "paimon",
        properties = Map(
          "warehouse" -> "file:/tmp/paimon-warehouse",
          "metastore" -> "filesystem",
          "table-default.bucket" -> "2"))),
      ConnectionRegistry.Empty)

    val paimon = catalogs.head
    paimon.`type` shouldBe "paimon"
    paimon.sessionCatalog shouldBe false
    paimon.sparkCatalogName shouldBe "lake"
    paimon.sparkConf("spark.sql.catalog.lake") shouldBe
      "org.apache.paimon.spark.SparkCatalog"
    paimon.sparkConf("spark.sql.catalog.lake.warehouse") shouldBe
      "file:/tmp/paimon-warehouse"
    paimon.sparkConf("spark.sql.catalog.lake.metastore") shouldBe "filesystem"
    paimon.sparkConf("spark.sql.catalog.lake.table-default.bucket") shouldBe "2"
    paimon.sparkConf("spark.sql.extensions") shouldBe
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
  }

  test("resolve a Paimon catalog backed by a Hive metastore") {
    val registry = ConnectionRegistry(Seq(hiveConnection()))
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "lake",
        `type` = "paimon",
        connection = Some("hms"),
        properties = Map("warehouse" -> "hdfs://nn/paimon"))),
      registry)

    val paimon = catalogs.head
    paimon.sparkConf("spark.sql.catalog.lake.metastore") shouldBe "hive"
    paimon.sparkConf("spark.sql.catalog.lake.uri") shouldBe "thrift://hms:9083"
    paimon.sparkConf("spark.sql.catalog.lake.warehouse") shouldBe "hdfs://nn/paimon"
  }

  test("resolve Paimon as a generic session catalog") {
    val catalogs = CatalogManager.resolve(
      Seq(CatalogConf(
        name = "lake",
        `type` = "paimon",
        properties = Map(
          "warehouse" -> "file:/tmp/paimon-warehouse",
          "sessionCatalog" -> "true"))),
      ConnectionRegistry.Empty)

    val paimon = catalogs.head
    paimon.sessionCatalog shouldBe true
    paimon.sparkCatalogName shouldBe "spark_catalog"
    paimon.sparkConf("spark.sql.catalog.spark_catalog") shouldBe
      "org.apache.paimon.spark.SparkGenericCatalog"
  }

  test("Paimon catalog requires a warehouse") {
    val error = intercept[PuddingException] {
      CatalogManager.resolve(
        Seq(CatalogConf("lake", "paimon", properties = Map("metastore" -> "filesystem"))),
        ConnectionRegistry.Empty)
    }
    error.getMessage should include ("requires property warehouse")
  }

  test("reject two session catalogs") {
    val error = intercept[PuddingException] {
      CatalogManager.resolve(
        Seq(
          CatalogConf("hudi_prod", "hudi"),
          CatalogConf(
            "lake",
            "paimon",
            properties = Map(
              "warehouse" -> "file:/tmp/paimon-warehouse",
              "sessionCatalog" -> "true"))),
        ConnectionRegistry.Empty)
    }
    error.getMessage should include ("only one session catalog")
  }

  test("reject a named catalog that collides with the session catalog name") {
    val error = intercept[PuddingException] {
      CatalogManager.resolve(
        Seq(
          CatalogConf("hudi_prod", "hudi"),
          CatalogConf(
            "jdbc_lake",
            "jdbc",
            properties = Map(
              "url" -> "jdbc:postgresql://db:5432/app",
              "sparkCatalogName" -> "spark_catalog"))),
        ConnectionRegistry.Empty)
    }
    error.getMessage should include ("duplicated Spark catalog name")
  }

  test("reject an invalid Gravitino timeout before calling the server") {
    val registry = ConnectionRegistry(Seq(ConnectionConf(
      name = "gravitino-prod",
      `type` = "gravitino",
      endpoint = Some("http://localhost:1"))))
    Seq("soon", "0").foreach { timeout =>
      val error = intercept[PuddingException] {
        CatalogManager.resolve(
          Seq(CatalogConf(
            name = "lake",
            `type` = "gravitino",
            connection = Some("gravitino-prod"),
            properties = Map(
              "metalake" -> "prod",
              "catalog" -> "lake",
              "timeoutMs" -> timeout))),
          registry)
      }
      error.getMessage should include ("timeoutMs")
    }
  }

  test("install merges extensions instead of replacing them") {
    val iceberg = CatalogManager.resolve(
      Seq(CatalogConf("lake", "iceberg", properties = Map("type" -> "hadoop"))),
      ConnectionRegistry.Empty)
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.sql.extensions", "com.example.ExistingExtension")

    CatalogManager.install(iceberg, sparkConf)

    sparkConf.get("spark.sql.extensions") should include ("com.example.ExistingExtension")
    sparkConf.get("spark.sql.extensions") should include (
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

  test("resolve a Gravitino Iceberg catalog through REST") {
    val response =
      """
        |{
        |  "code": 0,
        |  "catalog": {
        |    "name": "lakehouse",
        |    "type": "RELATIONAL",
        |    "provider": "lakehouse-iceberg",
        |    "properties": {
        |      "catalog-backend": "hadoop",
        |      "warehouse": "s3://bucket/warehouse"
        |    }
        |  }
        |}
        |""".stripMargin

    withGravitinoServer(response) { (endpoint, requestPath, authorization) =>
      val registry = ConnectionRegistry(Seq(ConnectionConf(
        name = "gravitino-prod",
        `type` = "gravitino",
        endpoint = Some(endpoint),
        auth = Map("token" -> "token-value"))))
      val catalogs = CatalogManager.resolve(
        Seq(CatalogConf(
          name = "lake",
          `type` = "gravitino",
          connection = Some("gravitino-prod"),
          properties = Map("metalake" -> "prod", "catalog" -> "lakehouse"))),
        registry)

      val catalog = catalogs.head
      catalog.`type` shouldBe "iceberg"
      catalog.sparkConf("spark.sql.catalog.lake") shouldBe
        "org.apache.iceberg.spark.SparkCatalog"
      catalog.sparkConf("spark.sql.catalog.lake.type") shouldBe "hadoop"
      catalog.sparkConf("spark.sql.catalog.lake.warehouse") shouldBe
        "s3://bucket/warehouse"
      requestPath.get() shouldBe "/api/metalakes/prod/catalogs/lakehouse"
      authorization.get() shouldBe "Bearer token-value"
    }
  }

  test("resolve a Gravitino Hive catalog through REST") {
    val response =
      """
        |{
        |  "code": 0,
        |  "catalog": {
        |    "name": "hive",
        |    "type": "RELATIONAL",
        |    "provider": "hive",
        |    "properties": {
        |      "metastore.uris": "thrift://hms:9083"
        |    }
        |  }
        |}
        |""".stripMargin

    withGravitinoServer(response) { (endpoint, _, _) =>
      val registry = ConnectionRegistry(Seq(ConnectionConf(
        name = "gravitino-prod",
        `type` = "gravitino",
        endpoint = Some(endpoint))))
      val catalogs = CatalogManager.resolve(
        Seq(CatalogConf(
          name = "hive_prod",
          `type` = "gravitino",
          connection = Some("gravitino-prod"),
          properties = Map("metalake" -> "prod", "catalog" -> "hive"))),
        registry)

      val hive = catalogs.head
      hive.`type` shouldBe "hive"
      hive.sessionCatalog shouldBe true
      hive.sparkConf("hive.metastore.uris") shouldBe "thrift://hms:9083"
    }
  }

  test("resolve a Gravitino JDBC catalog through REST") {
    val response =
      """
        |{
        |  "code": 0,
        |  "catalog": {
        |    "name": "pg",
        |    "type": "RELATIONAL",
        |    "provider": "jdbc-postgresql",
        |    "properties": {
        |      "jdbc-url": "jdbc:postgresql://db:5432/app",
        |      "jdbc-user": "pudding",
        |      "jdbc-password": "secret",
        |      "jdbc-driver": "org.postgresql.Driver"
        |    }
        |  }
        |}
        |""".stripMargin

    withGravitinoServer(response) { (endpoint, _, _) =>
      val registry = ConnectionRegistry(Seq(ConnectionConf(
        name = "gravitino-prod",
        `type` = "gravitino",
        endpoint = Some(endpoint))))
      val catalogs = CatalogManager.resolve(
        Seq(CatalogConf(
          name = "pg_catalog",
          `type` = "gravitino",
          connection = Some("gravitino-prod"),
          properties = Map("metalake" -> "prod", "catalog" -> "pg"))),
        registry)

      val jdbc = catalogs.head
      jdbc.`type` shouldBe "jdbc"
      jdbc.sparkConf("spark.sql.catalog.pg_catalog.url") shouldBe
        "jdbc:postgresql://db:5432/app"
      jdbc.sparkConf("spark.sql.catalog.pg_catalog.user") shouldBe "pudding"
      jdbc.sparkConf("spark.sql.catalog.pg_catalog.password") shouldBe "secret"
      jdbc.sparkConf("spark.sql.catalog.pg_catalog.driver") shouldBe "org.postgresql.Driver"
    }
  }

  test("reject an unsupported Gravitino provider") {
    val response =
      """
        |{
        |  "code": 0,
        |  "catalog": {
        |    "name": "events",
        |    "type": "MESSAGING",
        |    "provider": "kafka",
        |    "properties": {
        |      "bootstrap.servers": "broker:9092"
        |    }
        |  }
        |}
        |""".stripMargin

    withGravitinoServer(response) { (endpoint, _, _) =>
      val registry = ConnectionRegistry(Seq(ConnectionConf(
        name = "gravitino-prod",
        `type` = "gravitino",
        endpoint = Some(endpoint))))
      val error = intercept[PuddingException] {
        CatalogManager.resolve(
          Seq(CatalogConf(
            name = "events",
            `type` = "gravitino",
            connection = Some("gravitino-prod"),
            properties = Map("metalake" -> "prod", "catalog" -> "events"))),
          registry)
      }
      error.getMessage should include ("unsupported provider")
    }
  }

  private def withGravitinoServer(response: String)(
      test: (String, AtomicReference[String], AtomicReference[String]) => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val requestPath = new AtomicReference[String]()
    val authorization = new AtomicReference[String]()
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        requestPath.set(exchange.getRequestURI.toString)
        authorization.set(exchange.getRequestHeaders.getFirst("Authorization"))
        val bytes = response.getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, bytes.length)
        val output = exchange.getResponseBody
        try output.write(bytes) finally output.close()
      }
    })
    server.start()
    try {
      test(s"http://127.0.0.1:${server.getAddress.getPort}", requestPath, authorization)
    } finally {
      server.stop(0)
    }
  }
}
