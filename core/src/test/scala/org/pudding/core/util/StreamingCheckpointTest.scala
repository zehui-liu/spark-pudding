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

package org.pudding.core.util

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the streaming checkpoint retention policy and directory deletion.
 */
class StreamingCheckpointTest extends AnyFunSuite with Matchers {

  test("default to keeping the checkpoint") {
    StreamingCheckpoint.retention(Map.empty) shouldBe StreamingCheckpoint.KeepPolicy
    StreamingCheckpoint.retention(
      Map("checkpointRetention" -> "keep")) shouldBe StreamingCheckpoint.KeepPolicy
    StreamingCheckpoint.retention(
      Map("checkpointRetention" -> " delete-on-success ")) shouldBe
      StreamingCheckpoint.DeleteOnSuccessPolicy
  }

  test("reject an unsupported checkpoint retention value") {
    val error = intercept[PuddingException] {
      StreamingCheckpoint.retention(Map("checkpointRetention" -> "delete"))
    }
    error.getMessage should include ("checkpointRetention must be")

    intercept[PuddingException] {
      StreamingCheckpoint.retention(Map("checkpointRetention" -> true))
    }.getMessage should include ("checkpointRetention must be a string")
  }

  test("delete-on-success requires a finite trigger and a checkpoint") {
    val cfg = Map[String, Any]("checkpointRetention" -> "delete-on-success")
    StreamingCheckpoint.validate(
      cfg, Some("available-now"), Some("/tmp/checkpoint")) shouldBe
      StreamingCheckpoint.DeleteOnSuccessPolicy
    StreamingCheckpoint.validate(
      cfg, Some("once"), Some("/tmp/checkpoint")) shouldBe
      StreamingCheckpoint.DeleteOnSuccessPolicy

    intercept[PuddingException] {
      StreamingCheckpoint.validate(cfg, Some("processing-time: 10 seconds"), Some("/tmp/cp"))
    }.getMessage should include ("requires a finite trigger")

    intercept[PuddingException] {
      StreamingCheckpoint.validate(cfg, None, Some("/tmp/checkpoint"))
    }.getMessage should include ("requires a finite trigger")

    intercept[PuddingException] {
      StreamingCheckpoint.validate(cfg, Some("available-now"), None)
    }.getMessage should include ("requires checkpointLocation")

    intercept[PuddingException] {
      StreamingCheckpoint.validate(cfg, Some("available-now"), Some(" "))
    }.getMessage should include ("requires checkpointLocation")
  }

  test("keep does not require a finite trigger or a checkpoint") {
    StreamingCheckpoint.validate(
      Map.empty, Some("processing-time: 10 seconds"), None) shouldBe
      StreamingCheckpoint.KeepPolicy
  }

  test("delete a checkpoint directory recursively") {
    val checkpoint = Files.createTempDirectory("pudding-checkpoint-delete")
    val nested = Files.createDirectories(checkpoint.resolve("commits/1"))
    Files.write(nested.resolve("file"), "data".getBytes(StandardCharsets.UTF_8))

    StreamingCheckpoint.deleteCheckpoint(checkpoint.toString)

    Files.exists(checkpoint) shouldBe false
  }

  test("deleting a missing checkpoint is a no-op") {
    val checkpoint = Files.createTempDirectory("pudding-checkpoint-missing")
      .resolve("not-created")

    StreamingCheckpoint.deleteCheckpoint(checkpoint.toString)

    Files.exists(checkpoint) shouldBe false
  }

  test("refuse to delete a filesystem root") {
    intercept[PuddingException] {
      StreamingCheckpoint.deleteCheckpoint("/")
    }.getMessage should include ("refusing to delete a filesystem root")

    intercept[PuddingException] {
      StreamingCheckpoint.deleteCheckpoint(" ")
    }.getMessage should include ("checkpointLocation can not be empty")
  }
}
