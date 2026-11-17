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

package org.pudding.client.submit

import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests immutable submission records and their state transitions.
 */
class SubmissionRecordTest extends AnyFunSuite with Matchers {

  private val createdAt = Instant.parse("2026-10-23T10:00:00Z")

  test("builds a pending record and applies state transitions") {
    val pending = SubmissionRecord
      .builder("submission-1", "local", ApplicationState.PENDING, createdAt)
      .appName("test app")
      .master("local[1]")
      .deployMode("client")
      .build()

    pending.getUpdatedAt shouldBe createdAt
    pending.getFinishedAt.isPresent shouldBe false
    pending.isFinal shouldBe false
    pending.getApplicationId.isPresent shouldBe false

    val running = pending.withStatus(
      ApplicationState.RUNNING, "app-1", createdAt.plusSeconds(5))

    running.getState shouldBe ApplicationState.RUNNING
    running.getApplicationId.get shouldBe "app-1"
    running.getUpdatedAt shouldBe createdAt.plusSeconds(5)
    running.getFinishedAt.isPresent shouldBe false
    pending.getState shouldBe ApplicationState.PENDING

    val finished = running.withStatus(
      ApplicationState.FINISHED, null, createdAt.plusSeconds(10))

    finished.getState shouldBe ApplicationState.FINISHED
    finished.getApplicationId.get shouldBe "app-1"
    finished.getFinishedAt.get shouldBe createdAt.plusSeconds(10)
    finished.isSuccessful shouldBe true
    finished.getError.isPresent shouldBe false
  }

  test("a final record never changes") {
    val finished = SubmissionRecord
      .builder("submission-2", "yarn", ApplicationState.FINISHED, createdAt)
      .appName("test app")
      .build()

    finished.withStatus(ApplicationState.KILLED, "app-2", createdAt.plusSeconds(5)) should be theSameInstanceAs finished
    finished.withFailure(
      ApplicationState.FAILED, "late failure", createdAt.plusSeconds(5)) should be theSameInstanceAs finished
  }

  test("records a bounded single-line failure") {
    val pending = record("submission-3", "kubernetes")
    val message = "first line\nsecond line\r\n" + ("x" * (SubmissionRecord.MAX_ERROR_LENGTH + 100))

    val failed = pending.withFailure(
      ApplicationState.FAILED, message, createdAt.plusSeconds(2))

    failed.getState shouldBe ApplicationState.FAILED
    failed.isSuccessful shouldBe false
    failed.getFinishedAt.get shouldBe createdAt.plusSeconds(2)
    failed.getError.get should not include ("\n")
    failed.getError.get should not include ("\r")
    failed.getError.get.length shouldBe SubmissionRecord.MAX_ERROR_LENGTH
    failed.getError.get should endWith ("...")
  }

  test("timestamps never move backwards") {
    val updated = record("submission-4", "local")
      .withStatus(ApplicationState.RUNNING, null, createdAt.plusSeconds(10))

    val stale = updated.withStatus(
      ApplicationState.SUBMITTED, null, createdAt.plusSeconds(5))

    stale.getState shouldBe ApplicationState.SUBMITTED
    stale.getUpdatedAt shouldBe createdAt.plusSeconds(10)
  }

  test("rejects invalid transitions and records") {
    val pending = record("submission-5", "local")

    an[SubmissionException] should be thrownBy pending.withStatus(
      ApplicationState.PENDING, null, createdAt)
    an[SubmissionException] should be thrownBy pending.withFailure(
      ApplicationState.RUNNING, "not a failure", createdAt)
    an[SubmissionException] should be thrownBy SubmissionRecord.builder(
      "../escape", "local", ApplicationState.PENDING, createdAt)
    an[SubmissionException] should be thrownBy SubmissionRecord.builder(
      "submission-6", "local", ApplicationState.PENDING, createdAt).build()
    an[SubmissionException] should be thrownBy SubmissionRecord
      .builder("submission-7", "local", ApplicationState.RUNNING, createdAt)
      .appName("test app")
      .finishedAt(createdAt)
      .build()
    an[SubmissionException] should be thrownBy SubmissionRecord
      .builder("submission-8", "local", ApplicationState.PENDING, createdAt)
      .appName("test app")
      .updatedAt(createdAt.minusSeconds(1))
      .build()
  }

  test("keeps Kubernetes job Secret metadata across transitions") {
    val pending = SubmissionRecord
      .builder("submission-10", "kubernetes", ApplicationState.PENDING, createdAt)
      .appName("test app")
      .namespace("data-platform")
      .kubernetesJobSecret("pudding-job-submission-10")
      .kubernetesJobSecretKey("pipeline.yaml")
      .kubernetesJobSecretOwned(true)
      .build()

    pending.getKubernetesJobSecret.get shouldBe "pudding-job-submission-10"
    pending.getKubernetesJobSecretKey.get shouldBe "pipeline.yaml"
    pending.isKubernetesJobSecretOwned shouldBe true

    val finished = pending
      .withStatus(ApplicationState.RUNNING, "app-10", createdAt.plusSeconds(5))
      .withStatus(ApplicationState.FINISHED, null, createdAt.plusSeconds(10))

    finished.getNamespace.get shouldBe "data-platform"
    finished.getKubernetesJobSecret.get shouldBe "pudding-job-submission-10"
    finished.getKubernetesJobSecretKey.get shouldBe "pipeline.yaml"
    finished.isKubernetesJobSecretOwned shouldBe true
    finished.equals(pending.withStatus(
      ApplicationState.RUNNING, "app-10", createdAt.plusSeconds(5))
      .withStatus(ApplicationState.FINISHED, null, createdAt.plusSeconds(10))) shouldBe true
    finished.hashCode shouldBe pending
      .withStatus(ApplicationState.RUNNING, "app-10", createdAt.plusSeconds(5))
      .withStatus(ApplicationState.FINISHED, null, createdAt.plusSeconds(10))
      .hashCode

    val other = SubmissionRecord
      .builder("submission-10", "kubernetes", ApplicationState.FINISHED, createdAt)
      .appName("test app")
      .namespace("data-platform")
      .kubernetesJobSecret("pudding-job-submission-10")
      .kubernetesJobSecretKey("job.conf")
      .kubernetesJobSecretOwned(true)
      .updatedAt(createdAt.plusSeconds(10))
      .build()
    finished.equals(other) shouldBe false
  }

  test("rejects incomplete Kubernetes job Secret metadata") {
    val keyOnly = intercept[SubmissionException] {
      SubmissionRecord
        .builder("submission-11", "kubernetes", ApplicationState.PENDING, createdAt)
        .appName("test app")
        .kubernetesJobSecretKey("pipeline.yaml")
        .build()
    }
    keyOnly.getMessage should include ("requires a Secret name")

    val ownedOnly = intercept[SubmissionException] {
      SubmissionRecord
        .builder("submission-12", "kubernetes", ApplicationState.PENDING, createdAt)
        .appName("test app")
        .kubernetesJobSecretOwned(true)
        .build()
    }
    ownedOnly.getMessage should include ("requires a Secret name")
  }

  test("toString does not expose error details or kubeconfig paths") {
    val failed = record("submission-9", "kubernetes")
      .withFailure(ApplicationState.FAILED, "secret failure detail", createdAt.plusSeconds(1))

    failed.toString should include ("submission-9")
    failed.toString should not include ("secret failure detail")
    failed.toString should not include ("/private/kubeconfig")
  }

  test("toString does not expose the Kubernetes job Secret name") {
    val pending = SubmissionRecord
      .builder("submission-13", "kubernetes", ApplicationState.PENDING, createdAt)
      .appName("test app")
      .kubernetesJobSecret("sensitive-secret-name")
      .kubernetesJobSecretKey("pipeline.yaml")
      .kubernetesJobSecretOwned(true)
      .build()

    pending.toString should include ("submission-13")
    pending.toString should not include ("sensitive-secret-name")
    pending.toString should not include ("pipeline.yaml")
  }

  private def record(id: String, target: String): SubmissionRecord =
    SubmissionRecord
      .builder(id, target, ApplicationState.PENDING, createdAt)
      .appName("test app")
      .kubeconfig("/private/kubeconfig")
      .build()
}
