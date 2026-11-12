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

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.pudding.core.definition.SourceContext
import org.pudding.core.metric.StreamingMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Optional Kafka container test for the streaming reader and JSON/CDC decoding.
 *
 * Enabled by `PUDDING_TESTCONTAINERS=true`. A missing Docker daemon skips the test unless
 * `PUDDING_TESTCONTAINERS_REQUIRED=true` is set.
 */
class KafkaSourceStreamingIntegrationTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val enabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image =
    sys.env.getOrElse("PUDDING_TEST_KAFKA_IMAGE", "apache/kafka-native:3.8.0")

  private val topic = "pudding_stream_cdc"

  private var container: KafkaContainer = _

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    if (this.enabled) {
      try {
        this.container = new KafkaContainer(DockerImageName.parse(this.image))
        this.container.start()
      } catch {
        case NonFatal(error) =>
          this.container = null
          if (this.required) {
            throw new IllegalStateException(
              "Kafka container is required but failed to start: " +
                error.getClass.getSimpleName, error)
          }
      }
    }
    if (this.container != null) {
      this.sparkSession = SparkSession.builder()
        .appName("test for kafka streaming source")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    if (this.sparkSession != null) {
      this.sparkSession.close()
    }
    if (this.container != null) {
      this.container.stop()
    }
  }

  test("read streaming JSON CDC rows from Kafka") {
    assume(this.container != null, "Kafka container is not available")
    val bootstrap = this.container.getBootstrapServers
    this.createTopic(bootstrap)
    this.produce(bootstrap, Seq(
      """{"id":1,"name":"tom","op":"i"}""",
      """{"id":1,"name":"tom_v2","op":"u"}""",
      """{"id":2,"name":"jerry","op":"d"}"""))

    val checkpoint = Files.createTempDirectory("kafka-stream-checkpoint")
    StreamingMetrics.reset()
    try {
      val frame = new KafkaSource().initData(
        this.sparkSession,
        SourceContext(
          config = Some(Map(
            "subscribe" -> this.topic,
            "kafkaBootstrapServers" -> bootstrap,
            "valueFormat" -> "json",
            "valueSchema" -> Map("id" -> "int", "name" -> "string", "op" -> "string"),
            "cdcFormat" -> "op-column",
            "opField" -> "op",
            "options" -> Map("startingOffsets" -> "earliest"))),
          executionMode = Some(org.pudding.core.configure.ExecutionMode.Streaming)))
      val query = frame.writeStream
        .format("memory")
        .queryName("kafka_cdc_stream")
        .option("checkpointLocation", checkpoint.toString)
        .trigger(Trigger.AvailableNow)
        .start()
      query.awaitTermination()

      this.sparkSession.table("kafka_cdc_stream")
        .select("id", "name", "__op")
        .collect()
        .map(row => (row.getInt(0), row.getString(1), row.getString(2))).toSet shouldBe
        Set((1, "tom", "I"), (1, "tom_v2", "U"), (2, "jerry", "D"))

      this.awaitMetric(StreamingMetrics.SourceLagUpdatedMs)
      StreamingMetrics.value(StreamingMetrics.SourceHealthy) shouldBe 1L
      StreamingMetrics.value(StreamingMetrics.SourcePartitions) shouldBe 1L
      StreamingMetrics.value(StreamingMetrics.SourceLagRecords) shouldBe 0L
    } finally {
      this.deleteRecursively(checkpoint)
    }
  }

  private def awaitMetric(name: String): Unit = {
    val deadline = System.currentTimeMillis() + 30000L
    while (StreamingMetrics.value(name) <= 0L && System.currentTimeMillis() < deadline) {
      Thread.sleep(100L)
    }
  }

  private def createTopic(bootstrap: String): Unit = {
    val properties = new java.util.Properties()
    properties.put("bootstrap.servers", bootstrap)
    val admin = Admin.create(properties)
    try {
      try {
        admin.createTopics(Seq(new NewTopic(this.topic, 1, 1.toShort)).asJava).all().get()
      } catch {
        case _: org.apache.kafka.common.errors.TopicExistsException =>
      }
    } finally {
      admin.close()
    }
  }

  private def produce(bootstrap: String, rows: Seq[String]): Unit = {
    val properties = new java.util.Properties()
    properties.put("bootstrap.servers", bootstrap)
    properties.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](properties)
    try {
      rows.foreach(value =>
        producer.send(new ProducerRecord[String, String](this.topic, value)).get())
    } finally {
      producer.close()
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (path != null && Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(value => Try(Files.deleteIfExists(value)))
      } finally {
        stream.close()
      }
    }
  }
}
