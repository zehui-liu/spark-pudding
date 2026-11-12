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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests immutable submission requests, including Kubernetes Secret delivery options.
 */
class SubmissionRequestTest extends AnyFunSuite with Matchers {

  test("defaults to target-level Kubernetes Secret delivery") {
    val built = request().build()

    built.getSubmissionId.isPresent shouldBe false
    built.getJobSecret.isPresent shouldBe false
    built.getJobSecretKey.isPresent shouldBe false
    built.isManagedJobSecret shouldBe false
    built.isVerbose shouldBe false
  }

  test("accepts a caller supplied submission id") {
    val built = request().submissionId("  web-submission-1  ").build()

    built.getSubmissionId.get shouldBe "web-submission-1"
  }

  test("rejects an invalid caller supplied submission id") {
    intercept[SubmissionException] {
      request().submissionId("contains a space")
    }.getMessage should include ("invalid submission id")

    intercept[SubmissionException] {
      request().submissionId(" ")
    }.getMessage should include ("can not be empty")
  }

  test("accepts an explicit Secret or a managed Secret") {
    val explicit = request()
      .jobSecret("  external-secret  ")
      .jobSecretKey("  pipeline.yaml  ")
      .build()
    explicit.getJobSecret.get shouldBe "external-secret"
    explicit.getJobSecretKey.get shouldBe "pipeline.yaml"
    explicit.isManagedJobSecret shouldBe false

    val managed = request()
      .managedJobSecret()
      .jobSecretKey("pipeline.yaml")
      .build()
    managed.getJobSecret.isPresent shouldBe false
    managed.getJobSecretKey.get shouldBe "pipeline.yaml"
    managed.isManagedJobSecret shouldBe true

    val disabled = request().managedJobSecret(true).managedJobSecret(false).build()
    disabled.isManagedJobSecret shouldBe false
  }

  test("rejects ambiguous and incomplete Secret options") {
    val error = intercept[SubmissionException] {
      request()
        .jobSecret("external-secret")
        .managedJobSecret()
        .build()
    }
    error.getMessage should include ("managed job Secret")

    val missingSecret = intercept[SubmissionException] {
      request().jobSecretKey("pipeline.yaml").build()
    }
    missingSecret.getMessage should include ("requires a job Secret")

    val blankSecret = intercept[SubmissionException] {
      request().jobSecret("   ").jobSecretKey("pipeline.yaml").build()
    }
    blankSecret.getMessage should include ("requires a job Secret")
  }

  private def request(): SubmissionRequest.Builder =
    SubmissionRequest.builder(
      JobConfiguration.fromText("yaml", "job:\n  mode: batch\n  appName: request test\n"))
}
