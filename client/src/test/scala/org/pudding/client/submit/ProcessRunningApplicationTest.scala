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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests Kubernetes submission result tracking without starting a real Spark process.
 */
class ProcessRunningApplicationTest extends AnyFunSuite with Matchers {

  test("reports success from the final succeeded pod phase") {
    val output =
      """26/10/23 00:36:15 INFO LoggingPodStatusWatcherImpl: Application status for spark-1 (phase: Running)
        |26/10/23 00:36:15 INFO LoggingPodStatusWatcherImpl: Application status for spark-1 (phase: Succeeded)
        |""".stripMargin
    val process = new FakeProcess(0, output)
    val application = ProcessRunningApplication.monitor(process, "kubernetes")

    application.getState shouldBe ApplicationState.RUNNING

    process.finish()
    val result = application.await()

    result.getState shouldBe ApplicationState.FINISHED
    result.isSuccessful shouldBe true
    result.getApplicationId.get shouldBe "spark-1"
    result.getError.isPresent shouldBe false
  }

  test("reports failure from the final failed pod phase even when spark-submit exits zero") {
    val output =
      """26/10/23 00:37:07 INFO LoggingPodStatusWatcherImpl: Application status for spark-2 (phase: Failed)
        |""".stripMargin
    val process = new FakeProcess(0, output)
    val application = ProcessRunningApplication.monitor(process, "kubernetes")

    process.finish()
    val result = application.await()

    result.getState shouldBe ApplicationState.FAILED
    result.isSuccessful shouldBe false
    result.getApplicationId.get shouldBe "spark-2"
    result.getError.isPresent shouldBe true
    result.getError.get.getMessage should include("Failed")
  }

  test("reports LOST when spark-submit exits zero without a final pod phase") {
    val process = new FakeProcess(0, "")
    val application = ProcessRunningApplication.monitor(process, "kubernetes")

    process.finish()
    val result = application.await()

    result.getState shouldBe ApplicationState.LOST
    result.isSuccessful shouldBe false
    result.getError.isPresent shouldBe true
  }

  test("reports a successful YARN client submission from the process exit code") {
    val output =
      """26/11/11 10:00:00 INFO Client: Submitting application application_1762800000000_0001 to ResourceManager
        |26/11/11 10:00:01 INFO Client: Application report for application_1762800000000_0001 (state: FINISHED)
        |""".stripMargin
    val process = new FakeProcess(0, output)
    val application = ProcessRunningApplication.monitor(process, "yarn")

    process.finish()
    val result = application.await()

    result.getState shouldBe ApplicationState.FINISHED
    result.isSuccessful shouldBe true
    result.getApplicationId.get shouldBe "application_1762800000000_0001"
    result.getError.isPresent shouldBe false
  }

  test("reports a failed YARN client submission from the process exit code") {
    val process = new FakeProcess(1, "")
    val application = ProcessRunningApplication.monitor(process, "yarn")

    process.finish()
    val result = application.await()

    result.getState shouldBe ApplicationState.FAILED
    result.isSuccessful shouldBe false
    result.getError.get.getMessage should include("exited with code 1")
  }

  test("reports cancellation when the process is destroyed") {
    val process = new FakeProcess(143, "")
    val application = ProcessRunningApplication.monitor(process, "kubernetes")

    application.cancel()
    val result = application.await()

    result.getState shouldBe ApplicationState.KILLED
    result.isSuccessful shouldBe false
    result.getError.isPresent shouldBe false
  }

  test("keeps recent spark-submit output") {
    val output = "first line\nsecond line\nthird line\n"
    val process = new FakeProcess(0, output)
    val application = ProcessRunningApplication.monitor(process, "kubernetes")

    process.finish()
    application.await()

    val log = application.getRecentLogs(2)
    log.getText shouldBe "second line\nthird line"
    log.isTruncated shouldBe true
    an[SubmissionException] should be thrownBy application.getRecentLogs(0)
  }

  private class FakeProcess(exitCode: Int, output: String) extends Process {

    private val done = new CountDownLatch(1)

    private val stdout = new ByteArrayInputStream(output.getBytes(StandardCharsets.UTF_8))

    @volatile
    private var running = true

    override def getOutputStream: OutputStream = new ByteArrayOutputStream()

    override def getInputStream: InputStream = stdout

    override def getErrorStream: InputStream = new ByteArrayInputStream(new Array[Byte](0))

    override def waitFor(): Int = {
      done.await()
      exitCode
    }

    override def exitValue(): Int = {
      if (running) {
        throw new IllegalThreadStateException("process has not exited")
      }
      exitCode
    }

    override def destroy(): Unit = finish()

    def finish(): Unit = {
      running = false
      done.countDown()
    }
  }
}
