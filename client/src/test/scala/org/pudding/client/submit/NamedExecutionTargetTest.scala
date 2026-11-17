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

import java.util.Optional

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests aliasing built-in targets without changing their canonical plan type.
 */
class NamedExecutionTargetTest extends AnyFunSuite with Matchers {

  test("delegates plans and submissions under a named alias") {
    val delegate = new StubTarget("yarn", "yarn")
    val target = ExecutionTarget.named("yarn-prod", delegate)

    target.getName shouldBe "yarn-prod"
    target.getTargetType shouldBe TargetNames.YARN

    val plan = target.prepare(request())
    plan.getTargetName shouldBe TargetNames.YARN
    delegate.submitted shouldBe false

    target.submit(plan)
    delegate.submitted shouldBe true
  }

  test("rejects plans whose canonical type does not match the delegate") {
    val target = ExecutionTarget.named("yarn-prod", new StubTarget("yarn", "yarn"))

    val error = intercept[SubmissionException] {
      target.submit(SubmissionPlan.builder("kubernetes")
        .submissionId("submission-1")
        .appName("named target test")
        .sparkHome("/opt/spark")
        .master("k8s://https://cluster:443")
        .deployMode("cluster")
        .appResource("core.jar")
        .mainClass("Main")
        .build())
    }
    error.getMessage should include ("named target 'yarn-prod'")
  }

  test("requires a valid alias and delegate") {
    intercept[SubmissionException] {
      ExecutionTarget.named("Yarn Prod", new StubTarget("yarn", "yarn"))
    }
    an[NullPointerException] should be thrownBy
      ExecutionTarget.named("yarn-prod", null)
  }

  private def request(): SubmissionRequest =
    SubmissionRequest
      .builder(JobConfiguration.fromText("yaml", "job:\n  mode: batch\n  appName: named target test\n"))
      .build()

  private final class StubTarget(
      name: String,
      targetType: String) extends ExecutionTarget {

    @volatile var submitted = false

    override def getName: String = name

    override def getTargetType: String = targetType

    override def prepare(request: SubmissionRequest): SubmissionPlan =
      SubmissionPlan.builder(name)
        .submissionId("submission-1")
        .appName("named target test")
        .sparkHome("/opt/spark")
        .master("yarn")
        .deployMode("cluster")
        .appResource("core.jar")
        .mainClass("Main")
        .build()

    override def submit(plan: SubmissionPlan): RunningApplication = {
      submitted = true
      new RunningApplication {
        override def getTargetName: String = plan.getTargetName

        override def getApplicationId: Optional[String] = Optional.empty()

        override def getState: ApplicationState = ApplicationState.UNKNOWN

        override def getError: Optional[Throwable] = Optional.empty()

        override def getRecentLogs(maxLines: Int): ApplicationLog =
          ApplicationLog.empty()

        override def await(): SubmissionResult =
          new SubmissionResult(plan.getTargetName, ApplicationState.UNKNOWN, null, null)

        override def cancel(): Unit = ()
      }
    }
  }
}
