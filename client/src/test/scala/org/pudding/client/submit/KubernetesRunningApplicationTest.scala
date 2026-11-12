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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.pudding.client.submit.k8s.{DriverPodStatus, KubernetesPodMonitor}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Tests Kubernetes API lifecycle tracking with a fake pod monitor and process.
 */
class KubernetesRunningApplicationTest extends AnyFunSuite with Matchers {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext.global

  private val pollMillis = 10L

  private val graceMillis = 80L

  private val cancelTimeoutMillis = 300L

  test("reports success and the application id from the driver pod") {
    val process = new FakeProcess("Application status for spark-1 (phase: Running)\n")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-1")))
    val application = start(process, monitor)

    monitor.setPod(Some(new DriverPodStatus("driver-pod", "Succeeded", "spark-1")))
    process.exitNow(0)

    val result = await(application)
    result.getState shouldBe ApplicationState.FINISHED
    result.getApplicationId.get shouldBe "spark-1"
  }

  test("reports a failed driver pod even when spark-submit exits zero") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-2")))
    val application = start(process, monitor)

    monitor.setPod(Some(new DriverPodStatus("driver-pod", "Failed", "spark-2")))
    process.exitNow(0)

    val result = await(application)
    result.getState shouldBe ApplicationState.FAILED
    result.getError.get.getMessage should include ("phase Failed")
  }

  test("reports failure when the driver pod succeeded but spark-submit failed") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Succeeded", "spark-3")))
    val application = start(process, monitor)

    process.exitNow(2)

    val result = await(application)
    result.getState shouldBe ApplicationState.FAILED
    result.getError.get.getMessage should include ("exited with code 2")
  }

  test("finishes from the driver pod without waiting forever for spark-submit") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Succeeded", "spark-4")))
    val application = start(process, monitor)

    val result = await(application)
    result.getState shouldBe ApplicationState.FINISHED
    process.wasDestroyed shouldBe true
  }

  test("falls back to spark-submit output when the Kubernetes API fails") {
    val process = new FakeProcess(
      "Application status for spark-5 (phase: Succeeded)\n")
    val monitor = new FakeMonitor(Some(new DriverPodStatus("driver-pod", "Running", "spark-5")))
    monitor.failLookup = true
    val application = start(process, monitor)

    process.exitNow(0)

    val result = await(application)
    result.getState shouldBe ApplicationState.FINISHED
    result.getApplicationId.get shouldBe "spark-5"
  }

  test("reports LOST when neither the API nor the output has a final phase") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(None)
    val application = start(process, monitor)

    process.exitNow(0)

    val result = await(application)
    result.getState shouldBe ApplicationState.LOST
  }

  test("cancel deletes the driver pod and reports KILLED") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-7")))
    val application = start(process, monitor)

    application.cancel()

    val result = await(application)
    result.getState shouldBe ApplicationState.KILLED
    process.wasDestroyed shouldBe true
    monitor.deletions.get should be > 0
  }

  test("cancel reports failure when driver pod deletion cannot be confirmed") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-cancel-failed")))
    monitor.failDelete = true
    val application = start(process, monitor)

    application.cancel()

    val result = await(application)
    result.getState shouldBe ApplicationState.FAILED
    result.getError.get.getMessage should include ("could not be confirmed")
  }

  test("returns driver pod logs while the application is running") {
    val process = new FakeProcess("")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-8")))
    monitor.setDriverLogs(Some(ApplicationLog.of(
      java.util.Arrays.asList("driver line 1", "driver line 2"), false)))
    val application = start(process, monitor)

    val log = application.getRecentLogs(5)

    log.getLines.asScala shouldBe List("driver line 1", "driver line 2")
    monitor.logRequests.get should be > 0

    application.cancel()
    await(application)
  }

  test("falls back to spark-submit output when pod logs are unavailable") {
    val process = new FakeProcess("submission line 1\nsubmission line 2\n")
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-9")))
    monitor.failLogs = true
    val application = start(process, monitor)

    process.exitNow(0)
    monitor.setPod(Some(new DriverPodStatus("driver-pod", "Succeeded", "spark-9")))
    await(application)

    val log = application.getRecentLogs(2)
    log.getText shouldBe "submission line 1\nsubmission line 2"
  }

  private def start(
      process: FakeProcess,
      monitor: FakeMonitor): RunningApplication = {
    KubernetesRunningApplication.start(
      process,
      "kubernetes",
      "submission-test",
      monitor,
      pollMillis,
      graceMillis,
      cancelTimeoutMillis)
  }

  private def await(application: RunningApplication): SubmissionResult = {
    Await.result(Future(application.await()), 5.seconds)
  }

  private final class FakeMonitor(initial: Option[DriverPodStatus])
      extends KubernetesPodMonitor {

    private val pod = new AtomicReference[Optional[DriverPodStatus]](
      initial.map(Optional.of(_)).getOrElse(Optional.empty[DriverPodStatus]()))

    private val deleted = new AtomicInteger(0)

    private val driverLogs = new AtomicReference[Optional[ApplicationLog]](
      Optional.empty[ApplicationLog]())

    val logRequests = new AtomicInteger(0)

    @volatile var failLookup = false

    @volatile var failLogs = false

    @volatile var failDelete = false

    def setPod(status: Option[DriverPodStatus]): Unit = {
      pod.set(status.map(Optional.of(_)).getOrElse(Optional.empty[DriverPodStatus]()))
    }

    def setDriverLogs(log: Option[ApplicationLog]): Unit = {
      driverLogs.set(log.map(Optional.of(_)).getOrElse(Optional.empty[ApplicationLog]()))
    }

    def deletions: AtomicInteger = deleted

    override def findDriverPod(submissionId: String): Optional[DriverPodStatus] = {
      if (failLookup) {
        throw new IllegalStateException("api unavailable")
      }
      pod.get()
    }

    override def deleteDriverPod(submissionId: String): Boolean = {
      deleted.incrementAndGet()
      if (failDelete) {
        false
      } else {
        pod.set(Optional.empty())
        true
      }
    }

    override def getDriverLogs(
        submissionId: String, maxLines: Int): Optional[ApplicationLog] = {
      logRequests.incrementAndGet()
      if (failLogs) {
        throw new IllegalStateException("pod logs unavailable")
      }
      driverLogs.get()
    }

    override def close(): Unit = {
    }
  }

  private final class FakeProcess(output: String) extends Process {

    private val exited = new AtomicBoolean(false)

    private val destroyed = new AtomicBoolean(false)

    private val eventually = new CountDownLatch(1)

    private val code = new AtomicInteger(Int.MinValue)

    def exitNow(exitCode: Int): Unit = {
      if (exited.compareAndSet(false, true)) {
        code.set(exitCode)
        eventually.countDown()
      }
    }

    def wasDestroyed: Boolean = destroyed.get()

    override def getOutputStream: OutputStream = new ByteArrayOutputStream()

    override def getInputStream: InputStream =
      new ByteArrayInputStream(output.getBytes(StandardCharsets.UTF_8))

    override def getErrorStream: InputStream =
      new ByteArrayInputStream(Array.emptyByteArray)

    override def waitFor(): Int = {
      eventually.await()
      code.get()
    }

    override def waitFor(timeout: Long, unit: TimeUnit): Boolean = {
      eventually.await(timeout, unit)
    }

    override def exitValue(): Int = {
      if (!exited.get()) {
        throw new IllegalThreadStateException("process is still running")
      }
      code.get()
    }

    override def destroy(): Unit = {
      destroyed.set(true)
      exitNow(143)
    }

    override def destroyForcibly(): Process = {
      destroy()
      this
    }

    override def isAlive: Boolean = !exited.get()
  }
}
