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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Path}
import java.time.{Duration, Instant}
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.{Collections, UUID}

import org.pudding.client.submit.k8s.{DriverPodStatus, KubernetesJobSecretManager, KubernetesPodMonitor}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Tests durable submission management, Kubernetes reattachment and cancellation.
 */
class SubmissionManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val createdAt = Instant.parse("2026-10-23T10:00:00Z")

  private val managers = scala.collection.mutable.ArrayBuffer.empty[SubmissionManager]

  private val directories = scala.collection.mutable.ArrayBuffer.empty[Path]

  private var store: FileSubmissionStore = _

  override def beforeEach(): Unit = {
    val base = Files.createTempDirectory("pudding-manager-test-")
    directories += base
    store = new FileSubmissionStore(base.resolve("store"))
  }

  override def afterEach(): Unit = {
    managers.reverse.foreach(_.close())
    managers.clear()
    directories.reverse.foreach(deleteRecursively)
    directories.clear()
  }

  test("persists metadata before launch and follows the attached application") {
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application, store)
    target.planCustomizer = { builder =>
      builder.sparkConf("spark.hadoop.fs.s3a.secret.key", "DO-NOT-PERSIST")
      builder.environment("AWS_SECRET_ACCESS_KEY", "DO-NOT-PERSIST")
    }
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .autoStartWatcher(false))

    val managed = manager.submit(request())

    val beforeLaunch = target.persistedBeforeLaunch.get
    beforeLaunch.getState shouldBe ApplicationState.PENDING
    beforeLaunch.getSubmissionId shouldBe managed.getSubmissionId
    beforeLaunch.getTargetName shouldBe "local"
    beforeLaunch.getAppName shouldBe "manager test"
    beforeLaunch.getMaster.get shouldBe "local[1]"
    beforeLaunch.getDeployMode.get shouldBe "client"
    beforeLaunch.getNamespace.isPresent shouldBe false
    beforeLaunch.getKubeconfig.isPresent shouldBe false

    managed.getRecord.getState shouldBe ApplicationState.UNKNOWN
    store.find(managed.getSubmissionId).get shouldBe managed.getRecord

    application.state = ApplicationState.RUNNING
    application.applicationId = "app-1"
    application.recent = ApplicationLog.of(
      Collections.singletonList("attached log line"), false)

    val running = managed.refresh()
    running.getState shouldBe ApplicationState.RUNNING
    running.getApplicationId.get shouldBe "app-1"
    running.getError.isPresent shouldBe false
    store.find(managed.getSubmissionId).get shouldBe running
    managed.getRecentLogs(2).getText shouldBe "attached log line"

    application.state = ApplicationState.FINISHED
    val finished = managed.refresh()
    finished.getState shouldBe ApplicationState.FINISHED
    finished.isSuccessful shouldBe true
    finished.getFinishedAt.isPresent shouldBe true
    store.find(managed.getSubmissionId).get shouldBe finished

    val persisted = storeContents()
    persisted should not include ("DO-NOT-PERSIST")
    persisted should not include ("spark.hadoop.fs.s3a.secret.key")
    persisted should not include ("AWS_SECRET_ACCESS_KEY")

    manager.close()
    application.cancels.get shouldBe 0
  }

  test("creates a managed Secret after the pending record and deletes it when final") {
    val application = new FakeRunningApplication("kubernetes")
    val target = new FakeTarget("kubernetes", application, store)
    target.planCustomizer = { builder =>
      builder.jobSecret("pudding-job-managed", "pipeline.yaml", true)
    }
    val secrets = new FakeSecretManager()
    var recordAtCreate: Option[SubmissionRecord] = None
    secrets.onCreate = () => {
      recordAtCreate = store.list().asScala.headOption
    }
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .jobSecretManager(secrets)
      .autoStartWatcher(false))

    val managed = manager.submit("kubernetes", managedRequest())

    val pending = recordAtCreate.get
    pending.getSubmissionId shouldBe managed.getSubmissionId
    pending.getState shouldBe ApplicationState.PENDING
    pending.getKubernetesJobSecret.get shouldBe "pudding-job-managed"
    pending.getKubernetesJobSecretKey.get shouldBe "pipeline.yaml"
    pending.isKubernetesJobSecretOwned shouldBe true
    pending.getNamespace.get shouldBe "pudding-test"
    pending.getKubeconfig.get shouldBe "/private/kubeconfig"

    secrets.creates.asScala.toList shouldBe List(SecretCreate(
      "k8s://https://cluster:443",
      "pudding-test",
      "/private/kubeconfig",
      "pudding-job-managed",
      "pipeline.yaml",
      managedRequestContent))
    managed.getRecord.getState shouldBe ApplicationState.UNKNOWN

    application.state = ApplicationState.FINISHED
    managed.refresh().getState shouldBe ApplicationState.FINISHED

    val finished = managed.getRecord
    finished.getKubernetesJobSecret.get shouldBe "pudding-job-managed"
    finished.getKubernetesJobSecretKey.get shouldBe "pipeline.yaml"
    finished.isKubernetesJobSecretOwned shouldBe true
    awaitCondition(!secrets.deletes.isEmpty)
    secrets.deletes.get(0) shouldBe SecretDelete(
      "k8s://https://cluster:443",
      "pudding-test",
      "/private/kubeconfig",
      "pudding-job-managed")

    manager.reconcile()
    Thread.sleep(100)
    secrets.deletes.size shouldBe 1
    storeContents() should not include ("MANAGED-SECRET-CONTENT")
  }

  test("persists a failed record when a managed Secret can not be created") {
    val application = new FakeRunningApplication("kubernetes")
    val target = new FakeTarget("kubernetes", application, store)
    target.planCustomizer = { builder =>
      builder.jobSecret("pudding-job-failing", "job.conf", true)
    }
    val secrets = new FakeSecretManager()
    secrets.createFailure = new SubmissionException("not allowed to create Secrets")
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .jobSecretManager(secrets)
      .autoStartWatcher(false))

    val error = intercept[SubmissionFailedException] {
      manager.submit("kubernetes", managedRequest())
    }
    error.getMessage should include ("not allowed to create Secrets")
    error.getRecord.get.getState shouldBe ApplicationState.FAILED
    target.submittedPlan shouldBe null

    val record = manager.list().asScala.head
    record.getState shouldBe ApplicationState.FAILED
    record.getError.get should include ("not allowed to create Secrets")
    record.getKubernetesJobSecret.get shouldBe "pudding-job-failing"
    record.isKubernetesJobSecretOwned shouldBe true
    store.find(record.getSubmissionId).get shouldBe record

    // Cleanup still runs because the API may have created the Secret before failing.
    awaitCondition(!secrets.deletes.isEmpty)
    secrets.deletes.get(0).name shouldBe "pudding-job-failing"
  }

  test("never deletes a Kubernetes job Secret it does not own") {
    val application = new FakeRunningApplication("kubernetes")
    val target = new FakeTarget("kubernetes", application, store)
    target.planCustomizer = { builder =>
      builder.jobSecret("external-secret", "job.conf", false)
    }
    val secrets = new FakeSecretManager()
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .jobSecretManager(secrets)
      .autoStartWatcher(false))

    val managed = manager.submit("kubernetes", request())

    secrets.creates.isEmpty shouldBe true
    managed.getRecord.getKubernetesJobSecret.get shouldBe "external-secret"
    managed.getRecord.isKubernetesJobSecretOwned shouldBe false

    application.state = ApplicationState.FINISHED
    managed.refresh().getState shouldBe ApplicationState.FINISHED
    manager.reconcile()
    Thread.sleep(150)
    secrets.deletes.isEmpty shouldBe true
  }

  test("cleans up owned Secrets for final records after a restart") {
    val finished = managedRecord("submission-restored", "pudding-job-restored")
      .withStatus(ApplicationState.RUNNING, "app-restored", createdAt.plusSeconds(1))
      .withStatus(ApplicationState.FINISHED, null, createdAt.plusSeconds(2))
    store.save(finished)
    val secrets = new FakeSecretManager()

    newManager(SubmissionManager.builder()
      .store(store)
      .jobSecretManager(secrets)
      .autoStartWatcher(false))

    awaitCondition(!secrets.deletes.isEmpty)
    secrets.deletes.get(0) shouldBe SecretDelete(
      "k8s://https://cluster:443",
      "pudding-test",
      "/private/kubeconfig",
      "pudding-job-restored")
    store.find("submission-restored").get.isKubernetesJobSecretOwned shouldBe true
  }

  test("does not delete external Secrets for final records after a restart") {
    val external = SubmissionRecord.builder(
        "submission-external", "kubernetes", ApplicationState.FINISHED, createdAt)
      .appName("restored app")
      .master("k8s://https://cluster:443")
      .deployMode("cluster")
      .namespace("pudding-test")
      .kubeconfig("/private/kubeconfig")
      .kubernetesJobSecret("external-secret")
      .kubernetesJobSecretKey("job.conf")
      .kubernetesJobSecretOwned(false)
      .finishedAt(createdAt.plusSeconds(2))
      .build()
    store.save(external)
    val secrets = new FakeSecretManager()

    newManager(SubmissionManager.builder()
      .store(store)
      .jobSecretManager(secrets)
      .autoStartWatcher(false))

    Thread.sleep(150)
    secrets.deletes.isEmpty shouldBe true
    store.find("submission-external").get shouldBe external
  }

  test("persists a failed launch and rethrows") {
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application, store)
    target.launchFailure = new IllegalStateException("boom")
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .autoStartWatcher(false))

    val error = intercept[SubmissionFailedException] {
      manager.submit(request())
    }
    error.getMessage should include ("boom")
    error.getSubmissionId shouldBe error.getRecord.get.getSubmissionId

    val record = manager.list().asScala.head
    record.getState shouldBe ApplicationState.FAILED
    record.getError.get should include ("boom")
    record.getFinishedAt.isPresent shouldBe true
    store.find(record.getSubmissionId).get shouldBe record
    target.persistedBeforeLaunch.get.getState shouldBe ApplicationState.PENDING
    target.persistedBeforeLaunch.get.getSubmissionId shouldBe record.getSubmissionId
  }

  test("uses a caller supplied submission id") {
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application, store)
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .autoStartWatcher(false))
    val submission = SubmissionRequest
      .builder(JobConfiguration.fromText("yaml", "job:\n  mode: batch\n  appName: manager test\n"))
      .submissionId("caller-submission-1")
      .build()

    val managed = manager.submit(submission)

    managed.getSubmissionId shouldBe "caller-submission-1"
    store.find("caller-submission-1").isPresent shouldBe true
  }

  test("explains a failure when the launcher reports no error") {
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application, store)
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .autoStartWatcher(false))
    val managed = manager.submit(request())

    application.applicationId = "app-without-details"
    application.state = ApplicationState.FAILED
    val failed = managed.refresh()

    failed.getError.get should include (failed.getSubmissionId)
    failed.getError.get should include ("target 'local'")
    failed.getError.get should include ("app-without-details")
    failed.getError.get should include ("persisted launcher logs")
  }

  test("prunes only final records that ended before the cutoff") {
    val now = Instant.now()
    val oldFinal = SubmissionRecord
      .builder("old-final", "local", ApplicationState.FINISHED, now.minusSeconds(3600))
      .appName("old final")
      .master("local[1]")
      .deployMode("client")
      .finishedAt(now.minusSeconds(3500))
      .build()
    val recentFinal = SubmissionRecord
      .builder("recent-final", "local", ApplicationState.FINISHED, now.minusSeconds(600))
      .appName("recent final")
      .master("local[1]")
      .deployMode("client")
      .updatedAt(now.minusSeconds(30))
      .finishedAt(now.minusSeconds(30))
      .build()
    val oldActive = record(
      "old-active", "local", ApplicationState.RUNNING, Some("app-active"))
    store.save(oldFinal)
    store.save(recentFinal)
    store.save(oldActive)
    val manager = newManager(SubmissionManager.builder()
      .store(store)
      .autoStartWatcher(false))

    manager.pruneFinalBefore(now.minusSeconds(60)) shouldBe 1

    manager.find("old-final").isPresent shouldBe false
    manager.find("recent-final").isPresent shouldBe true
    manager.find("old-active").get.getState shouldBe ApplicationState.LOST
    store.find("old-final").isPresent shouldBe true
  }

  test("restores a non-Kubernetes submission as lost") {
    val running = record(
      "submission-local", "local", ApplicationState.RUNNING,
      Some("app-local"))
    store.save(running)

    val manager = newManager(SubmissionManager.builder()
      .store(store)
      .autoStartWatcher(false))

    val restored = manager.find("submission-local").get.getRecord
    restored.getState shouldBe ApplicationState.LOST
    restored.getError.get should include ("client restarted")
    restored.getFinishedAt.isPresent shouldBe true
    store.find("submission-local").get shouldBe restored
  }

  test("reattaches a Kubernetes submission through the driver pod label") {
    val running = record(
      "submission-k8s", "kubernetes", ApplicationState.RUNNING,
      Some("spark-0"))
    store.save(running)
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-0")),
      Some(ApplicationLog.of(Collections.singletonList("driver log line"), false)))
    val factory = new FakeMonitorFactory(() => monitor)
    val manager = newManager(SubmissionManager.builder()
      .store(store)
      .monitorFactory(factory)
      .autoStartWatcher(false))

    manager.reconcile()
    factory.calls shouldBe List(
      ("k8s://https://cluster:443", "pudding-test", "/private/kubeconfig"))

    monitor.setPod(Some(new DriverPodStatus("driver-pod", "Succeeded", "spark-1")))
    manager.reconcile()

    val finished = manager.find("submission-k8s").get.getRecord
    finished.getState shouldBe ApplicationState.FINISHED
    finished.getApplicationId.get shouldBe "spark-1"
    finished.getFinishedAt.isPresent shouldBe true
    store.find("submission-k8s").get shouldBe finished
    monitor.closes.get should be >= 1

    manager.find("submission-k8s").get.getRecentLogs(3).getText shouldBe
      "driver log line"
    factory.calls.size shouldBe 2
  }

  test("marks a reattached Kubernetes submission lost when the pod never appears") {
    val running = record(
      "submission-missing", "kubernetes", ApplicationState.RUNNING,
      Some("spark-missing"))
    store.save(running)
    val monitor = new FakeMonitor(None)
    val manager = newManager(SubmissionManager.builder()
      .store(store)
      .monitorFactory(new FakeMonitorFactory(() => monitor))
      .reattachGrace(Duration.ofMillis(40))
      .autoStartWatcher(false))

    manager.reconcile()
    manager.find("submission-missing").get.getState shouldBe ApplicationState.RUNNING

    Thread.sleep(80)
    manager.reconcile()

    val lost = manager.find("submission-missing").get.getRecord
    lost.getState shouldBe ApplicationState.LOST
    lost.getError.get should include ("no Kubernetes driver pod")
    lost.getFinishedAt.isPresent shouldBe true
    store.find("submission-missing").get shouldBe lost
  }

  test("cancel of a reattached Kubernetes submission deletes the driver pod") {
    val running = record(
      "submission-cancel", "kubernetes", ApplicationState.RUNNING,
      Some("spark-cancel"))
    store.save(running)
    val monitor = new FakeMonitor(
      Some(new DriverPodStatus("driver-pod", "Running", "spark-cancel")))
    val manager = newManager(SubmissionManager.builder()
      .store(store)
      .monitorFactory(new FakeMonitorFactory(() => monitor))
      .autoStartWatcher(false))

    val managed = manager.find("submission-cancel").get
    managed.cancel()

    monitor.deletions.get shouldBe 1
    monitor.closes.get should be >= 1
    val killed = managed.getRecord
    killed.getState shouldBe ApplicationState.KILLED
    killed.isSuccessful shouldBe false
    killed.getFinishedAt.isPresent shouldBe true
    store.find("submission-cancel").get shouldBe killed

    managed.cancel()
    monitor.deletions.get shouldBe 1
  }

  test("cancel of an attached application delegates to the application") {
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application)
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .autoStartWatcher(false))

    val managed = manager.submit(request())
    managed.cancel()

    application.cancels.get shouldBe 1
    managed.getState shouldBe ApplicationState.UNKNOWN

    application.state = ApplicationState.KILLED
    managed.refresh().getState shouldBe ApplicationState.KILLED
    store.find(managed.getSubmissionId).get.getState shouldBe ApplicationState.KILLED
  }

  test("the watcher refreshes submissions in the background") {
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application)
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .pollInterval(Duration.ofMillis(10)))

    val managed = manager.submit(request())
    application.state = ApplicationState.RUNNING
    application.applicationId = "app-background"

    awaitCondition {
      val found = store.find(managed.getSubmissionId)
      found.isPresent &&
        found.get.getState == ApplicationState.RUNNING &&
        found.get.getApplicationId.isPresent &&
        found.get.getApplicationId.get == "app-background"
    }
    store.find(managed.getSubmissionId).get.getApplicationId.get shouldBe "app-background"
  }

  test("close waits for an in-flight submission and rejects later ones") {
    val launchEntered = new CountDownLatch(1)
    val allowLaunch = new CountDownLatch(1)
    val application = new FakeRunningApplication("local")
    val target = new FakeTarget("local", application)
    target.beforeLaunch = () => {
      launchEntered.countDown()
      allowLaunch.await(5, TimeUnit.SECONDS)
      ()
    }
    val manager = newManager(SubmissionManager.builder()
      .target(target)
      .store(store)
      .autoStartWatcher(false))
    val executor = Executors.newFixedThreadPool(2)
    try {
      val submitting = executor.submit(new Callable[ManagedSubmission] {
        override def call(): ManagedSubmission = manager.submit(request())
      })
      launchEntered.await(5, TimeUnit.SECONDS) shouldBe true

      val closing = executor.submit(new Runnable {
        override def run(): Unit = manager.close()
      })
      Thread.sleep(200)
      closing.isDone shouldBe false

      allowLaunch.countDown()
      val managed = submitting.get(5, TimeUnit.SECONDS)
      closing.get(5, TimeUnit.SECONDS)

      manager.find(managed.getSubmissionId).isPresent shouldBe true
      store.find(managed.getSubmissionId).get.getState shouldBe
        ApplicationState.UNKNOWN
      an[SubmissionException] should be thrownBy manager.submit(request())
    } finally {
      allowLaunch.countDown()
      executor.shutdownNow()
      manager.close()
    }
  }

  test("selects among configured targets and rejects ambiguous submissions") {
    val localApplication = new FakeRunningApplication("local")
    val kubernetesApplication = new FakeRunningApplication("kubernetes")
    val localTarget = new FakeTarget("local", localApplication)
    val kubernetesTarget = new FakeTarget("kubernetes", kubernetesApplication)
    val manager = newManager(SubmissionManager.builder()
      .target(localTarget)
      .target(kubernetesTarget)
      .store(store)
      .autoStartWatcher(false))

    an[SubmissionException] should be thrownBy manager.submit(request())

    val managed = manager.submit("k8s", request())
    managed.getTargetName shouldBe "kubernetes"
    store.find(managed.getSubmissionId).get.getTargetName shouldBe "kubernetes"

    an[SubmissionException] should be thrownBy manager.submit("unknown", request())
    an[SubmissionException] should be thrownBy SubmissionManager.builder()
      .target(localTarget)
      .target(localTarget)
      .store(store)
  }

  test("submits to multiple clusters of the same kind through named targets") {
    val yarnEast = new FakeRunningApplication("yarn")
    val yarnWest = new FakeRunningApplication("yarn")
    val eastTarget = new FakeTarget("yarn", yarnEast)
    val westTarget = new FakeTarget("yarn", yarnWest)
    val manager = newManager(SubmissionManager.builder()
      .target(ExecutionTarget.named("yarn-east", eastTarget))
      .target(ExecutionTarget.named("yarn-west", westTarget))
      .store(store)
      .autoStartWatcher(false))

    val managed = manager.submit("yarn-east", request())

    managed.getTargetName shouldBe "yarn"
    managed.getRecord.getTargetName shouldBe "yarn"
    store.find(managed.getSubmissionId).get.getTargetName shouldBe "yarn"
    eastTarget.submittedPlan should not be null
    westTarget.submittedPlan shouldBe null

    val ambiguous = intercept[SubmissionException] {
      manager.submit("yarn", request())
    }
    ambiguous.getMessage should include ("ambiguous")
    ambiguous.getMessage should include ("yarn-east")
    ambiguous.getMessage should include ("yarn-west")
    an[SubmissionException] should be thrownBy manager.submit("yarn-client", request())
    an[SubmissionException] should be thrownBy manager.submit("missing", request())
  }

  test("keeps Kubernetes records canonical when using named clusters") {
    val application = new FakeRunningApplication("kubernetes")
    val target = new FakeTarget("kubernetes", application)
    val manager = newManager(SubmissionManager.builder()
      .target(ExecutionTarget.named("k8s-prod", target))
      .store(store)
      .autoStartWatcher(false))

    val managed = manager.submit("k8s-prod", request())

    managed.getTargetName shouldBe "kubernetes"
    managed.getRecord.getTargetName shouldBe "kubernetes"
    managed.getRecord.getNamespace.get shouldBe "pudding-test"
    managed.getRecord.getKubeconfig.get shouldBe "/private/kubeconfig"
    target.submittedPlan.getTargetName shouldBe "kubernetes"

    application.state = ApplicationState.FINISHED
    managed.refresh().getState shouldBe ApplicationState.FINISHED
    store.find(managed.getSubmissionId).get.getTargetName shouldBe "kubernetes"
  }

  test("falls back to a canonical type when only one target of that kind exists") {
    val application = new FakeRunningApplication("yarn")
    val target = new FakeTarget("yarn", application)
    val manager = newManager(SubmissionManager.builder()
      .target(ExecutionTarget.named("yarn-prod", target))
      .store(store)
      .autoStartWatcher(false))

    val managed = manager.submit("yarn", request())
    managed.getRecord.getTargetName shouldBe "yarn"
    manager.submit("yarn-cluster", request()).getRecord.getTargetName shouldBe "yarn"
  }

  test("updates targets without disturbing active submissions") {
    val localApplication = new FakeRunningApplication("local")
    val yarnApplication = new FakeRunningApplication("yarn")
    val localTarget = new FakeTarget("local", localApplication)
    val yarnTarget = new FakeTarget("yarn", yarnApplication)
    val namedYarn = ExecutionTarget.named("yarn-prod", yarnTarget)
    val manager = newManager(SubmissionManager.builder()
      .target(localTarget)
      .target(namedYarn)
      .store(store)
      .autoStartWatcher(false))

    val managed = manager.submit("yarn-prod", request())
    localApplication.state = ApplicationState.RUNNING

    manager.updateTargets(List[ExecutionTarget](localTarget).asJava)

    an[SubmissionException] should be thrownBy manager.submit("yarn-prod", request())
    an[SubmissionException] should be thrownBy manager.submit("yarn", request())
    manager.submit(request()).getRecord.getTargetName shouldBe "local"

    yarnApplication.state = ApplicationState.FINISHED
    managed.refresh().getState shouldBe ApplicationState.FINISHED
    manager.find(managed.getSubmissionId).isPresent shouldBe true

    manager.updateTargets(Collections.emptyList[ExecutionTarget]())
    an[SubmissionException] should be thrownBy manager.submit(request())
    an[SubmissionException] should be thrownBy manager.updateTargets(List[ExecutionTarget](
      ExecutionTarget.named("yarn-a", yarnTarget),
      ExecutionTarget.named("yarn-a", yarnTarget)).asJava)

    manager.close()
    an[SubmissionException] should be thrownBy
      manager.updateTargets(List[ExecutionTarget](localTarget).asJava)
  }

  test("validates manager builder options") {
    an[SubmissionException] should be thrownBy SubmissionManager.builder()
      .autoStartWatcher(false)
      .build()
    an[SubmissionException] should be thrownBy SubmissionManager.builder()
      .store(store)
      .pollInterval(Duration.ZERO)
    an[SubmissionException] should be thrownBy SubmissionManager.builder()
      .store(store)
      .reattachGrace(Duration.ofMillis(-1))
  }

  private def newManager(builder: SubmissionManager.Builder): SubmissionManager = {
    val manager = builder.build()
    managers += manager
    manager
  }

  private val managedRequestContent =
    "job:\n  mode: batch\n  appName: manager test\n  describe: MANAGED-SECRET-CONTENT\n"

  private def request(): SubmissionRequest =
    SubmissionRequest
      .builder(JobConfiguration.fromText("yaml", "job:\n  mode: batch\n  appName: manager test\n"))
      .build()

  private def managedRequest(): SubmissionRequest =
    SubmissionRequest
      .builder(JobConfiguration.fromText("yaml", managedRequestContent))
      .managedJobSecret()
      .build()

  private def managedRecord(id: String, secretName: String): SubmissionRecord =
    SubmissionRecord
      .builder(id, "kubernetes", ApplicationState.PENDING, createdAt)
      .appName("restored app")
      .master("k8s://https://cluster:443")
      .deployMode("cluster")
      .namespace("pudding-test")
      .kubeconfig("/private/kubeconfig")
      .kubernetesJobSecret(secretName)
      .kubernetesJobSecretKey("job.conf")
      .kubernetesJobSecretOwned(true)
      .build()

  private def record(
      id: String,
      targetName: String,
      state: ApplicationState,
      applicationId: Option[String]): SubmissionRecord = {
    val builder = SubmissionRecord
      .builder(id, targetName, state, createdAt)
      .appName("restored app")
      .applicationId(applicationId.orNull)
    if (targetName == "kubernetes") {
      builder
        .master("k8s://https://cluster:443")
        .deployMode("cluster")
        .namespace("pudding-test")
        .kubeconfig("/private/kubeconfig")
    } else {
      builder.master("local[1]").deployMode("client")
    }
    builder.build()
  }

  private def storeContents(): String = {
    val stream = Files.list(store.getDirectory)
    try {
      stream.iterator().asScala.toList
        .map(path => new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
        .mkString("\n")
    } finally {
      stream.close()
    }
  }

  private def awaitCondition(condition: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + 5000
    while (!condition && System.currentTimeMillis() < deadline) {
      Thread.sleep(10)
    }
    condition shouldBe true
  }

  private def deleteRecursively(path: Path): Unit = {
    if (path != null && Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toList
          .sortBy(_.getNameCount)
          .reverse
          .foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }

  private final class FakeTarget(
      name: String,
      application: RunningApplication,
      storeForInspection: FileSubmissionStore = null) extends ExecutionTarget {

    @volatile var launchFailure: RuntimeException = _

    @volatile var beforeLaunch: () => Unit = () => ()

    @volatile var planCustomizer: SubmissionPlan.Builder => Unit =
      (_: SubmissionPlan.Builder) => ()

    @volatile var persistedBeforeLaunch: Option[SubmissionRecord] = None

    @volatile var submittedPlan: SubmissionPlan = _

    override def getName: String = name

    override def prepare(request: SubmissionRequest): SubmissionPlan = {
      val submissionId = if (request.getSubmissionId.isPresent) {
        request.getSubmissionId.get
      } else {
        UUID.randomUUID().toString
      }
      val builder = SubmissionPlan
        .builder(name)
        .submissionId(submissionId)
        .appName("manager test")
        .sparkHome("/opt/spark")
        .master(if (name == "kubernetes") "k8s://https://cluster:443" else "local[1]")
        .deployMode(if (name == "kubernetes") "cluster" else "client")
        .appResource("core.jar")
        .mainClass("Main")
      if (name == "kubernetes") {
        builder.sparkConf("spark.kubernetes.namespace", "pudding-test")
        builder.environment("KUBECONFIG", "/private/kubeconfig")
      }
      planCustomizer(builder)
      builder.build()
    }

    override def submit(plan: SubmissionPlan): RunningApplication = {
      submittedPlan = plan
      if (storeForInspection != null) {
        val found = storeForInspection.find(plan.getSubmissionId)
        persistedBeforeLaunch = if (found.isPresent) Some(found.get) else None
      }
      beforeLaunch()
      if (launchFailure != null) {
        throw launchFailure
      }
      application
    }
  }

  private final class FakeRunningApplication(targetName: String)
      extends RunningApplication {

    @volatile var state: ApplicationState = ApplicationState.UNKNOWN

    @volatile var applicationId: String = _

    @volatile var failure: Throwable = _

    @volatile var recent: ApplicationLog = ApplicationLog.empty()

    val cancels = new AtomicInteger()

    override def getTargetName: String = targetName

    override def getApplicationId: Optional[String] =
      Optional.ofNullable(applicationId)

    override def getState: ApplicationState = state

    override def getError: Optional[Throwable] = Optional.ofNullable(failure)

    override def getRecentLogs(maxLines: Int): ApplicationLog = {
      ApplicationLog.requirePositive(maxLines)
      recent
    }

    override def await(): SubmissionResult =
      new SubmissionResult(targetName, state, applicationId, failure)

    override def cancel(): Unit = {
      cancels.incrementAndGet()
    }
  }

  private final class FakeMonitor(
      initial: Option[DriverPodStatus],
      logs: Option[ApplicationLog] = None) extends KubernetesPodMonitor {

    private val pod = new java.util.concurrent.atomic.AtomicReference[
      Optional[DriverPodStatus]](
      initial.map(Optional.of(_)).getOrElse(Optional.empty[DriverPodStatus]()))

    val deletions = new AtomicInteger()

    val closes = new AtomicInteger()

    def setPod(status: Option[DriverPodStatus]): Unit = {
      pod.set(status.map(Optional.of(_)).getOrElse(Optional.empty[DriverPodStatus]()))
    }

    override def findDriverPod(submissionId: String): Optional[DriverPodStatus] =
      pod.get()

    override def deleteDriverPod(submissionId: String): Boolean = {
      deletions.incrementAndGet()
      pod.set(Optional.empty[DriverPodStatus]())
      true
    }

    override def getDriverLogs(
        submissionId: String, maxLines: Int): Optional[ApplicationLog] = {
      ApplicationLog.requirePositive(maxLines)
      logs.map(Optional.of(_)).getOrElse(Optional.empty[ApplicationLog]())
    }

    override def close(): Unit = {
      closes.incrementAndGet()
    }
  }

  private final class FakeMonitorFactory(supplier: () => KubernetesPodMonitor)
      extends KubernetesMonitorFactory {

    val calls = scala.collection.mutable.ArrayBuffer.empty[(String, String, String)]

    override def create(
        master: String, namespace: String, kubeconfig: String): KubernetesPodMonitor = {
      synchronized {
        calls += ((master, namespace, kubeconfig))
        supplier()
      }
    }
  }

  private final class FakeSecretManager extends KubernetesJobSecretManager {

    val creates = new java.util.concurrent.CopyOnWriteArrayList[SecretCreate]()

    val deletes = new java.util.concurrent.CopyOnWriteArrayList[SecretDelete]()

    @volatile var createFailure: RuntimeException = _

    @volatile var deleteFailure: RuntimeException = _

    @volatile var onCreate: () => Unit = () => ()

    override def create(
        master: String,
        namespace: String,
        kubeconfig: String,
        secretName: String,
        secretKey: String,
        content: String): Unit = {
      onCreate()
      val failure = createFailure
      if (failure != null) {
        throw failure
      }
      creates.add(SecretCreate(master, namespace, kubeconfig, secretName, secretKey, content))
    }

    override def delete(
        master: String,
        namespace: String,
        kubeconfig: String,
        secretName: String): Unit = {
      val failure = deleteFailure
      if (failure != null) {
        throw failure
      }
      deletes.add(SecretDelete(master, namespace, kubeconfig, secretName))
    }
  }
}

private final case class SecretCreate(
    master: String,
    namespace: String,
    kubeconfig: String,
    name: String,
    key: String,
    content: String)

private final case class SecretDelete(
    master: String,
    namespace: String,
    kubeconfig: String,
    name: String)
