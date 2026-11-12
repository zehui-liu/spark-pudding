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

import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import org.pudding.core.PuddingException
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.{DataSink, SinkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Kafka sink option construction and DataFrame validation. No Kafka broker is required.
 */
class KafkaSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

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

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("test for kafka sink")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.close()
    }
  }

  private def dataFrame(rows: Seq[Seq[Any]], fields: (String, DataType)*): DataFrame = {
    val schema = StructType(fields.map { case (name, dataType) => StructField(name, dataType) })
    val rowRdd = spark.sparkContext.parallelize(rows.map(values => Row.fromSeq(values)))
    spark.createDataFrame(rowRdd, schema)
  }

  test("register the kafka-sink identifier") {
    new KafkaSink().identifier shouldBe "kafka-sink"
    new KafkaSink().supportsConnection shouldBe true
    DataSink.identifiers should contain ("kafka-sink")
  }

  test("build writer options from a connection") {
    val options = KafkaSink.writerOptions(SinkContext(
      config = Some(Map[String, Any](
        "topic" -> "orders",
        "options" -> Map("acks" -> "all"))),
      connection = Some(connection)))

    options("kafka.bootstrap.servers") shouldBe "broker:9092"
    options("kafka.security.protocol") shouldBe "SASL_SSL"
    options("topic") shouldBe "orders"
    options("acks") shouldBe "all"
  }

  test("accept legacy bootstrap servers and require a broker") {
    val options = KafkaSink.writerOptions(SinkContext(
      config = Some(Map[String, Any](
        "kafkaBootstrapServers" -> "legacy:9092",
        "topic" -> "orders"))))
    options("kafka.bootstrap.servers") shouldBe "legacy:9092"

    intercept[PuddingException] {
      KafkaSink.writerOptions(SinkContext(
        config = Some(Map[String, Any]("topic" -> "orders"))))
    }
    val error = intercept[PuddingException] {
      KafkaSink.writerOptions(SinkContext(
        config = Some(Map[String, Any]("topic" -> "orders")),
        connection = Some(connection.copy(`type` = "jdbc"))))
    }
    error.getMessage should include ("requires a kafka connection")
  }

  test("reject invalid option maps") {
    val nullValue = intercept[PuddingException] {
      KafkaSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "topic" -> "orders",
          "kafkaBootstrapServers" -> "broker:9092",
          "options" -> Map("acks" -> null)))))
    }
    nullValue.getMessage should include ("null key or value")

    val notMap = intercept[PuddingException] {
      KafkaSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "topic" -> "orders",
          "kafkaBootstrapServers" -> "broker:9092",
          "options" -> "not-a-map"))))
    }
    notMap.getMessage should include ("must be a map")

    val topicOption = intercept[PuddingException] {
      KafkaSink.writerOptions(SinkContext(
        config = Some(Map[String, Any](
          "topic" -> "orders",
          "kafkaBootstrapServers" -> "broker:9092",
          "options" -> Map("topic" -> "other")))))
    }
    topicOption.getMessage should include ("config.topic")
  }

  test("prepare key, value and topic columns") {
    val result = KafkaSink.prepare(
      dataFrame(
        Seq(Seq("k1", "v1", "topic-1")),
        "key" -> StringType, "value" -> StringType, "topic" -> StringType),
      Map.empty)
    result.columns.toSeq shouldBe Seq("key", "value", "topic")
  }

  test("select configured key and value columns") {
    val result = KafkaSink.prepare(
      dataFrame(
        Seq(Seq("k1", "v1", "topic-1", "ignored")),
        "k" -> StringType, "v" -> StringType, "topic" -> StringType,
        "extra" -> StringType),
      Map("keyColumn" -> "k", "valueColumn" -> "v"))
    result.columns.toSeq shouldBe Seq("key", "value", "topic")
  }

  test("use a fixed topic without a topic column") {
    val result = KafkaSink.prepare(
      dataFrame(Seq(Seq("v1")), "value" -> StringType),
      Map("topic" -> "fixed-topic"))
    result.columns.toSeq shouldBe Seq("value")
  }

  test("preserve optional partition columns") {
    val result = KafkaSink.prepare(
      dataFrame(
        Seq(Seq("v1", "topic-1", 1)),
        "value" -> StringType, "topic" -> StringType, "partition" -> IntegerType),
      Map.empty)
    result.columns.toSeq shouldBe Seq("value", "topic", "partition")
  }

  test("validate value, topic and partition columns") {
    intercept[PuddingException] {
      KafkaSink.prepare(
        dataFrame(Seq(Seq("topic-1")), "topic" -> StringType),
        Map.empty)
    }
    intercept[PuddingException] {
      KafkaSink.prepare(
        dataFrame(Seq(Seq("v1")), "value" -> StringType),
        Map.empty)
    }
    intercept[PuddingException] {
      KafkaSink.prepare(
        dataFrame(
          Seq(Seq(1, "topic-1")),
          "value" -> IntegerType, "topic" -> StringType),
        Map("topic" -> "fixed"))
    }
    intercept[PuddingException] {
      KafkaSink.prepare(
        dataFrame(
          Seq(Seq("v1", "topic-1", "not-int")),
          "value" -> StringType, "topic" -> StringType,
          "partition" -> StringType),
        Map.empty)
    }
  }

  test("validate save modes and streaming settings") {
    KafkaSink.saveMode(Map.empty) shouldBe SaveMode.Append
    KafkaSink.saveMode(Map("mode" -> "errorifexists")) shouldBe SaveMode.ErrorIfExists
    intercept[PuddingException] {
      KafkaSink.saveMode(Map("mode" -> "overwrite"))
    }
    KafkaSink.streamingEnabled(Map("isStreaming" -> true)) shouldBe true
    KafkaSink.outputMode(Map.empty) shouldBe "append"
    intercept[PuddingException] {
      KafkaSink.outputMode(Map("outputMode" -> "update"))
    }
    intercept[PuddingException] {
      KafkaSink.outputMode(Map("outputMode" -> "complete"))
    }
  }

  test("streaming write requires a checkpoint") {
    val error = intercept[PuddingException] {
      new KafkaSink().writer(
        dataFrame(Seq(Seq("v1")), "value" -> StringType),
        SinkContext(
          config = Some(Map[String, Any](
            "topic" -> "orders",
            "isStreaming" -> true)),
          connection = Some(connection)))
    }
    error.getMessage should include ("checkpointLocation")
  }
}
