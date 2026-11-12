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

import org.apache.kafka.common.TopicPartition
import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KafkaLagMetricsTest extends AnyFunSuite with Matchers {

  test("parse Kafka source offsets from query progress") {
    val offsets = KafkaLagMetrics.parseOffsets(
      """{"orders":{"0":10,"1":5},"payments":{"2":7}}""")

    offsets shouldBe Map(
      new TopicPartition("orders", 0) -> 10L,
      new TopicPartition("orders", 1) -> 5L,
      new TopicPartition("payments", 2) -> 7L)
    KafkaLagMetrics.parseOffsets("{}") shouldBe empty
  }

  test("reject malformed Kafka source offsets") {
    intercept[PuddingException] {
      KafkaLagMetrics.parseOffsets("not-json")
    }.getMessage should include ("invalid kafka source offset")
  }

  test("compute aggregate lag, maximum and partition count") {
    val consumed = Map(
      new TopicPartition("orders", 0) -> 10L,
      new TopicPartition("orders", 1) -> 5L,
      new TopicPartition("orders", 2) -> 20L)
    val latest = Map(
      new TopicPartition("orders", 0) -> 15L,
      new TopicPartition("orders", 1) -> 3L,
      new TopicPartition("orders", 2) -> 30L)

    KafkaLagMetrics.lagSnapshot(consumed, latest) shouldBe
      KafkaLagMetrics.LagSnapshot(records = 15L, maxLag = 10L, partitions = 3L)
    KafkaLagMetrics.lagSnapshot(Map.empty, Map.empty) shouldBe
      KafkaLagMetrics.LagSnapshot(0L, 0L, 0L)
  }

  test("build AdminClient properties from Kafka reader options") {
    val properties = KafkaLagMetrics.adminProperties(Map(
      "kafka.bootstrap.servers" -> "broker:9092",
      "kafka.security.protocol" -> "SASL_SSL",
      "subscribe" -> "orders"))

    properties.getProperty("bootstrap.servers") shouldBe "broker:9092"
    properties.getProperty("security.protocol") shouldBe "SASL_SSL"
    properties.containsKey("subscribe") shouldBe false

    intercept[PuddingException] {
      KafkaLagMetrics.adminProperties(Map("subscribe" -> "orders"))
    }.getMessage should include ("kafka.bootstrap.servers")
  }

  test("parse the lag metrics interval") {
    KafkaLagMetrics.interval(Map.empty) shouldBe KafkaLagMetrics.DefaultIntervalMs
    KafkaLagMetrics.interval(Map("lagMetricsIntervalMs" -> 1000)) shouldBe 1000L
    KafkaLagMetrics.interval(Map("lagMetricsIntervalMs" -> "2000")) shouldBe 2000L
    KafkaLagMetrics.interval(Map("lagMetricsIntervalMs" -> BigInt(3000))) shouldBe 3000L

    intercept[PuddingException] {
      KafkaLagMetrics.interval(Map("lagMetricsIntervalMs" -> 0))
    }.getMessage should include ("must be positive")
    intercept[PuddingException] {
      KafkaLagMetrics.interval(Map("lagMetricsIntervalMs" -> "bad"))
    }.getMessage should include ("must be a positive integer")
  }
}
