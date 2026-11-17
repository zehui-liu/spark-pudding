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

package org.pudding.core.util

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.pudding.core.PuddingException
import org.pudding.core.configure.{SourceConf, TransformConf}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the stateful-streaming checkpoint contract.
 */
class StreamingStateContractTest extends AnyFunSuite with Matchers {

  private def source(
      config: Map[String, Any] = Map("subscribe" -> "orders")): SourceConf = {
    SourceConf(
      `type` = "kafka-source",
      describe = None,
      id = "source1",
      config = Some(config))
  }

  private def aggregate(
      watermark: String = "1 minute"): TransformConf = {
    TransformConf(
      `type` = "aggregate-transform",
      describe = None,
      id = "aggregate1",
      config = Some(Map[String, Any](
        "eventTime" -> "event_time",
        "watermark" -> watermark,
        "window" -> "5 minutes",
        "groupBy" -> Seq("city"),
        "aggregations" -> Map("count" -> "count(*)"))),
      dependencies = Seq("source1"))
  }

  test("fingerprint is stable across map order and secret values") {
    val first = StreamingStateContract.fingerprint(
      source(Map("options" -> Map("b" -> 2, "a" -> 1), "password" -> "first")),
      Seq(aggregate()))
    val second = StreamingStateContract.fingerprint(
      source(Map("password" -> "second", "options" -> Map("a" -> 1, "b" -> 2))),
      Seq(aggregate()))

    first shouldBe second
    first should startWith ("sha256:")
    first should not include ("first")
    first should not include ("second")
  }

  test("fingerprint changes when the stateful configuration changes") {
    val first = StreamingStateContract.fingerprint(source(), Seq(aggregate()))
    val second = StreamingStateContract.fingerprint(
      source(), Seq(aggregate(watermark = "10 minutes")))

    first should not be second
  }

  test("reader tuning does not change the state contract") {
    val first = StreamingStateContract.fingerprint(
      source(Map(
        "subscribe" -> "orders",
        "options" -> Map("maxOffsetsPerTrigger" -> "1000"),
        "lagMetricsIntervalMs" -> 30000)),
      Seq(aggregate()))
    val second = StreamingStateContract.fingerprint(
      source(Map(
        "subscribe" -> "orders",
        "options" -> Map("maxOffsetsPerTrigger" -> "5000"),
        "lagMetricsIntervalMs" -> 60000)),
      Seq(aggregate()))

    first shouldBe second
  }

  test("write and verify a new contract") {
    val checkpoint = Files.createTempDirectory("pudding-state-contract-new")
    val contract = StreamingStateContract.fingerprint(source(), Seq(aggregate()))

    StreamingStateContract.verify(checkpoint.toString, contract)
    Files.exists(checkpoint.resolve(StreamingStateContract.FileName)) shouldBe true

    StreamingStateContract.verify(checkpoint.toString, contract)
  }

  test("reject a changed contract on an existing checkpoint") {
    val checkpoint = Files.createTempDirectory("pudding-state-contract-changed")
    val first = StreamingStateContract.fingerprint(source(), Seq(aggregate()))
    val second = StreamingStateContract.fingerprint(
      source(), Seq(aggregate(watermark = "10 minutes")))

    StreamingStateContract.verify(checkpoint.toString, first)

    val error = intercept[PuddingException] {
      StreamingStateContract.verify(checkpoint.toString, second)
    }
    error.getMessage should include ("different streaming state contract")
    error.getMessage should include (checkpoint.toString)
    error.getMessage should not include ("10 minutes")
  }

  test("adopt a pre-contract checkpoint") {
    val checkpoint = Files.createTempDirectory("pudding-state-contract-legacy")
    Files.createDirectories(checkpoint.resolve("commits/0"))
    val contract = StreamingStateContract.fingerprint(source(), Seq(aggregate()))

    StreamingStateContract.verify(checkpoint.toString, contract)

    Files.exists(checkpoint.resolve(StreamingStateContract.FileName)) shouldBe true
  }

  test("reject a malformed contract file") {
    val checkpoint = Files.createTempDirectory("pudding-state-contract-bad")
    Files.write(
      checkpoint.resolve(StreamingStateContract.FileName),
      "not-json".getBytes(StandardCharsets.UTF_8))
    val contract = StreamingStateContract.fingerprint(source(), Seq(aggregate()))

    intercept[PuddingException] {
      StreamingStateContract.verify(checkpoint.toString, contract)
    }.getMessage should include ("not valid JSON")
  }

  test("state contract requires a checkpoint location") {
    intercept[PuddingException] {
      StreamingCheckpoint.validate(
        Map.empty, None, None, Some("sha256:abc"))
    }.getMessage should include ("requires checkpointLocation")
  }
}
