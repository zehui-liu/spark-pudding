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

import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.{DataSource, SourceContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Kafka source option construction and decoding. No broker is required.
 */
class KafkaSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("test for kafka source")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
    }
  }

  private def raw(rows: (String, String)*): DataFrame = {
    val session = sparkSession
    import session.implicits._
    rows.zipWithIndex.map { case ((key, value), index) =>
      (key, value, "orders", 0, index.toLong, 1000L + index, 0)
    }.toDF("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
  }

  private def rawBinary(values: Array[Byte]*): DataFrame = {
    val session = sparkSession
    import session.implicits._
    values.zipWithIndex.map { case (value, index) =>
      ("k", value, "orders", 0, index.toLong, 1000L + index, 0)
    }.toDF("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
  }

  private def connection: ResolvedConnection = {
    ResolvedConnection(
      name = "kafka",
      `type` = "kafka",
      endpoint = Some("broker:9092"),
      auth = Map.empty,
      properties = Map.empty,
      options = Map(
        "kafka.bootstrap.servers" -> "broker:9092",
        "kafka.security.protocol" -> "SASL_SSL"))
  }

  test("register the kafka-source identifier") {
    new KafkaSource().identifier shouldBe "kafka-source"
    new KafkaSource().supportsConnection shouldBe true
    DataSource.identifiers should contain ("kafka-source")
  }

  test("declare batch and streaming execution modes") {
    new KafkaSource().supportedExecutionModes shouldBe
      Set(ExecutionMode.Batch, ExecutionMode.Streaming)
    intercept[PuddingException] {
      KafkaSource.validateOptions(Map("endingOffsets" -> "latest"), streaming = true)
    }.getMessage should include ("endingOffsets")
    KafkaSource.validateOptions(Map("startingOffsets" -> "earliest"), streaming = true)
  }

  test("keep key and value without a value schema") {
    val decoded = KafkaSource.decode(
      raw("k" -> """{"id":1,"name":"tom"}"""),
      Map.empty)
    decoded.columns.toSeq shouldBe Seq("key", "value")
    decoded.select("key", "value").collect().head.getString(0) shouldBe "k"
  }

  test("parse a JSON value schema") {
    val decoded = KafkaSource.decode(
      raw("k" -> """{"id":1,"name":"tom"}""", "k2" -> """{"id":2,"name":"jerry"}"""),
      Map(
        "valueFormat" -> "json",
        "valueSchema" -> Map("id" -> "long", "name" -> "string")))
    decoded.columns.toSeq shouldBe Seq("id", "name")
    decoded.select("id", "name").collect()
      .map(row => (row.getLong(0), row.getString(1))).toSet shouldBe
      Set((1L, "tom"), (2L, "jerry"))
  }

  test("decode avro and protobuf through the optional schema registry runtime") {
    Seq("avro", "protobuf").foreach { format =>
      val decoded = KafkaSource.decode(
        rawBinary(
          Array[Byte](7.toByte) ++ "tom".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        Map(
          "valueFormat" -> format,
          "valueSchema" -> Map("id" -> "int", "name" -> "string"),
          "schemaRegistry" -> Map("url" -> "http://registry:8081")))
      decoded.select("id", "name").collect().head shouldBe
        org.apache.spark.sql.Row(7, "tom")
    }
  }

  test("add metadata columns when requested") {
    val decoded = KafkaSource.decode(
      raw("k" -> """{"id":1}"""),
      Map(
        "valueFormat" -> "json",
        "valueSchema" -> Map("id" -> "long"),
        "includeMetadata" -> true))
    decoded.columns should contain allOf (
      "id", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp",
      "_kafka_key")
    decoded.select("_kafka_topic", "_kafka_partition").collect().head
      .getString(0) shouldBe "orders"
  }

  test("normalize a generic CDC operation column") {
    val decoded = KafkaSource.decode(
      raw("k" -> """{"id":1,"name":"tom","op":"u"}"""),
      Map(
        "valueFormat" -> "json",
        "valueSchema" -> Map("id" -> "long", "name" -> "string", "op" -> "string"),
        "cdcFormat" -> "op-column",
        "opField" -> "op",
        "opColumn" -> "__op"))
    decoded.columns should contain ("__op")
    decoded.columns should not contain "op"
    decoded.select("__op", "_kafka_offset").collect().head.getString(0) shouldBe "U"
  }

  test("decode a Debezium envelope") {
    val decoded = KafkaSource.decode(
      raw(
        "k" -> """{"before":null,"after":{"id":1,"name":"tom"},"op":"c","ts_ms":1000}""",
        "k2" -> """{"before":{"id":2,"name":"jerry"},"after":null,"op":"d","ts_ms":2000}"""),
      Map(
        "valueFormat" -> "json",
        "valueSchema" -> Map("id" -> "long", "name" -> "string"),
        "cdcFormat" -> "debezium"))
    decoded.select("id", "name", "__op", "_cdc_timestamp").collect()
      .map(row => (row.getLong(0), row.getString(1), row.getString(2))).toSet shouldBe
      Set((1L, "tom", "I"), (2L, "jerry", "D"))
  }

  test("fail or drop malformed JSON according to parseMode") {
    val invalid = raw("k" -> "{not-json")
    intercept[Exception] {
      KafkaSource.decode(
        invalid,
        Map(
          "valueFormat" -> "json",
          "valueSchema" -> Map("id" -> "long"),
          "parseMode" -> "failfast")).collect()
    }
    val dropped = KafkaSource.decode(
      invalid,
      Map(
        "valueFormat" -> "json",
        "valueSchema" -> Map("id" -> "long"),
        "parseMode" -> "dropMalformed"))
    dropped.count() shouldBe 0
  }

  test("quarantine dropped malformed records in batch mode") {
    val directory = Files.createTempDirectory("pudding-kafka-bad-records")
    val badPath = directory.resolve("bad").toString
    try {
      val decoded = KafkaSource.decode(
        raw(
          "bad" -> "{not-json",
          "good" -> """{"id":2}"""),
        Map(
          "valueFormat" -> "json",
          "valueSchema" -> Map("id" -> "long"),
          "parseMode" -> "dropMalformed",
          "badRecordsPath" -> badPath))

      decoded.count() shouldBe 1L
      val bad = sparkSession.read.json(badPath)
      bad.count() shouldBe 1L
      bad.select("_error").collect().head.getString(0) should include ("not-json")
    } finally {
      val stream = Files.walk(directory)
      try {
        stream.iterator().asScala.toVector.reverse.foreach(path => Files.deleteIfExists(path))
      } finally {
        stream.close()
      }
    }
  }

  test("retain a corrupt-record column in permissive streaming mode") {
    val decoded = KafkaSource.decode(
      raw(
        "bad" -> "{not-json",
        "good" -> """{"id":2}"""),
      Map(
        "valueFormat" -> "json",
        "valueSchema" -> Map("id" -> "long"),
        "parseMode" -> "permissive",
        "includeCorruptRecord" -> true),
      streaming = true)

    decoded.columns should contain (KafkaSource.CorruptRecordColumn)
    decoded.count() shouldBe 2L
    decoded.filter(decoded(KafkaSource.CorruptRecordColumn).isNotNull).count() shouldBe 1L
  }

  test("reject badRecordsPath for streaming and incompatible parse modes") {
    val invalid = raw("bad" -> "{not-json")
    val config = Map(
      "valueFormat" -> "json",
      "valueSchema" -> Map("id" -> "long"),
      "parseMode" -> "dropMalformed",
      "badRecordsPath" -> "/tmp/pudding-bad")

    intercept[PuddingException] {
      KafkaSource.decode(invalid, config, streaming = true)
    }.getMessage should include ("batch-only")

    intercept[PuddingException] {
      KafkaSource.decode(
        invalid,
        Map(
          "valueFormat" -> "json",
          "valueSchema" -> Map("id" -> "long"),
          "parseMode" -> "failfast",
          "badRecordsPath" -> "/tmp/pudding-bad"))
    }.getMessage should include ("requires parseMode=dropMalformed")
  }

  test("create a streaming DataFrame without connecting to a broker") {
    val frame = new KafkaSource().initData(
      sparkSession,
      SourceContext(
        config = Some(Map(
          "subscribe" -> "orders",
          "kafkaBootstrapServers" -> "localhost:9092")),
        executionMode = Some(ExecutionMode.Streaming)))
    frame.isStreaming shouldBe true
    Try(frame.schema.fieldNames.toSeq).get should contain ("value")
  }

  test("build reader options from a connection") {
    val options = KafkaSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "subscribe" -> "orders",
        "kafkaBootstrapServers" -> "ignored:9092",
        "options" -> Map("startingOffsets" -> "earliest", "maxOffsetsPerTrigger" -> 100))),
      connection = Some(connection)))

    options("subscribe") shouldBe "orders"
    options("kafka.bootstrap.servers") shouldBe "broker:9092"
    options("kafka.security.protocol") shouldBe "SASL_SSL"
    options("startingOffsets") shouldBe "earliest"
    options("maxOffsetsPerTrigger") shouldBe "100"
    options should not contain key ("kafkaBootstrapServers")
  }

  test("support assign and subscribePattern instead of subscribe") {
    val assign = KafkaSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "assign" -> """{"orders":[0,1]}""",
        "kafkaBootstrapServers" -> "broker:9092"))))
    assign("assign") shouldBe """{"orders":[0,1]}"""
    assign should not contain key ("subscribe")

    val pattern = KafkaSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "subscribePattern" -> "orders-.*",
        "kafkaBootstrapServers" -> "broker:9092"))))
    pattern("subscribePattern") shouldBe "orders-.*"

    intercept[PuddingException] {
      KafkaSource.readerOptions(SourceContext(
        config = Some(Map[String, Any](
          "subscribe" -> "orders",
          "assign" -> """{"orders":[0]}""",
          "kafkaBootstrapServers" -> "broker:9092"))))
    }.getMessage should include ("exactly one")
  }

  test("accept legacy bootstrap servers when no connection is configured") {
    val options = KafkaSource.readerOptions(SourceContext(
      config = Some(Map[String, Any](
        "subscribe" -> "orders",
        "kafkaBootstrapServers" -> "legacy:9092"))))

    options("kafka.bootstrap.servers") shouldBe "legacy:9092"
  }

  test("reject a wrong connection type and missing required options") {
    val error = intercept[PuddingException] {
      KafkaSource.readerOptions(SourceContext(
        config = Some(Map("subscribe" -> "orders")),
        connection = Some(connection.copy(`type` = "jdbc"))))
    }
    error.getMessage should include ("requires a kafka connection")

    intercept[PuddingException] {
      KafkaSource.readerOptions(SourceContext(
        config = Some(Map("kafkaBootstrapServers" -> "broker:9092"))))
    }
    intercept[PuddingException] {
      KafkaSource.readerOptions(SourceContext(
        config = Some(Map("subscribe" -> "orders"))))
    }
  }
}
