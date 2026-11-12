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
 * Tests the target-neutral submission entry point.
 */
class PuddingSubmitterTest extends AnyFunSuite with Matchers {

  test("prepares and submits through the selected target") {
    val plan = SubmissionPlan.builder("fake")
      .appName("fake job")
      .sparkHome("/opt/spark")
      .master("local[1]")
      .deployMode("client")
      .appResource("/opt/pudding/libs/pudding-core.jar")
      .mainClass("org.pudding.core.PuddingSpoon")
      .build()
    val expectedResult = new SubmissionResult(
      "fake",
      ApplicationState.FINISHED,
      "application_1",
      null)

    var prepared = false
    var submittedPlan: SubmissionPlan = null
    val target = new ExecutionTarget {
      override def getName: String = "fake"

      override def prepare(request: SubmissionRequest): SubmissionPlan = {
        prepared = true
        plan
      }

      override def submit(candidate: SubmissionPlan): RunningApplication = {
        submittedPlan = candidate
        new FakeRunningApplication(expectedResult)
      }
    }
    val job = JobConfiguration.fromText("yaml", "placeholder")
    val request = SubmissionRequest.builder(job).build()

    val application = new PuddingSubmitter().submit(target, request)
    val result = application.await()

    prepared shouldBe true
    (submittedPlan eq plan) shouldBe true
    result should be theSameInstanceAs expectedResult
  }

  private class FakeRunningApplication(result: SubmissionResult) extends RunningApplication {

    override def getTargetName: String = result.getTargetName

    override def getApplicationId: java.util.Optional[String] = result.getApplicationId

    override def getState: ApplicationState = result.getState

    override def getError: java.util.Optional[Throwable] = result.getError

    override def await(): SubmissionResult = result

    override def cancel(): Unit = {
    }
  }
}
