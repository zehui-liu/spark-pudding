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

package org.pudding.client.target

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileSystems, Files, Path, Paths}
import java.util.Arrays

import org.pudding.client.submit.{JobConfiguration, SubmissionException, SubmissionPlan, SubmissionRequest}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Tests target-neutral preparation and the built-in execution targets.
 */
class ExecutionTargetTest extends AnyFunSuite with Matchers
  with BeforeAndAfterAll with BeforeAndAfterEach {

  private val coreJar = "pudding-core-1.0.0.jar"

  private val sourceJar = "mock-source-1.0.jar"

  private val sinkJar = "mock-sink-1.0.jar"

  private val fileSinkJar = "file-common-sink-1.0.jar"

  private var distribution: Path = _

  private val preparedPlans = ArrayBuffer.empty[SubmissionPlan]

  override def afterEach(): Unit = {
    preparedPlans.foreach(plan =>
      plan.getTemporaryPaths.asScala.foreach(deleteRecursively))
    preparedPlans.clear()
  }

  override def beforeAll(): Unit = {
    distribution = Files.createTempDirectory("pudding-target-test")
    val libs = Files.createDirectories(distribution.resolve("libs"))
    List(
      coreJar,
      sourceJar,
      sinkJar,
      fileSinkJar,
      "database-cdc-source-1.0.jar",
      "cdc-runtime-1.0.jar",
      "unrelated-1.0.jar",
      "hadoop-oss-runtime-1.0.jar",
      "hadoop-azure-runtime-1.0.jar",
      "gcs-runtime-1.0.jar",
      "hadoop-cos-runtime-1.0.jar",
      coreJar.replace(".jar", "-sources.jar")
    ).foreach(name => Files.createFile(libs.resolve(name)))
  }

  override def afterAll(): Unit = {
    deleteRecursively(distribution)
  }

  test("local target builds a plan with only used connectors and overrides deployment") {
    val jobConf = job(
      "local[2]",
      "client",
      Map("spark.executor.cores" -> "2"))
    val target = LocalTarget.builder()
      .master("local[4]")
      .sparkConf("spark.executor.memory", "512m")
      .sparkConf("spark.master", "local[8]")
      .sparkConf("spark.app.name", "ignored")
      .build()

    val plan = prepare(target, request(jobConf))

    plan.getTargetName shouldBe "local"
    plan.getMaster shouldBe "local[4]"
    plan.getDeployMode shouldBe "client"
    plan.getAppName shouldBe "target test"
    plan.getSparkHome shouldBe "/opt/spark"
    plan.getMainClass shouldBe "org.pudding.core.PuddingSpoon"
    plan.getAppResource.endsWith("/libs/" + coreJar) shouldBe true
    fileNames(plan.getJars) shouldBe List(sinkJar, sourceJar)
    plan.getSparkConf.get("spark.executor.cores") shouldBe "2"
    plan.getSparkConf.get("spark.executor.memory") shouldBe "512m"
    plan.getSparkConf.containsKey("spark.master") shouldBe false
    plan.getSparkConf.containsKey("spark.app.name") shouldBe false
    plan.getSparkConf.containsKey("spark.submit.deployMode") shouldBe false
    plan.getEnvironment.get("FOO") shouldBe "bar"
    plan.getAppArgs.asScala should contain ("--jobFile")
    plan.getAppArgs.asScala should not contain ("--jobText")
    plan.getAppArgs.asScala should contain ("yaml")
    val configFile = Paths.get(argument(plan, "--jobFile"))
    configFile.isAbsolute shouldBe true
    configFile.getFileName.toString shouldBe "job.conf"
    new String(Files.readAllBytes(configFile), StandardCharsets.UTF_8) shouldBe jobConf.getContent
    if (FileSystems.getDefault.supportedFileAttributeViews.contains("posix")) {
      Files.getPosixFilePermissions(configFile) shouldBe
        PosixFilePermissions.fromString("rw-------")
    }
    plan.getTemporaryPaths.size shouldBe 1
    plan.getTemporaryPaths.get(0) shouldBe configFile.getParent
    plan.getAppArgs.asScala should not contain jobConf.getContent
    plan.toString should not include ("source-1")
  }

  test("file-sink selects the shared file-common-sink jar") {
    val jobConf = JobConfiguration.fromText(
      "yaml",
      job("local[2]", "client", Map.empty).getContent
        .replace("type: 'mock-sink'", "type: 'file-sink'"))

    val plan = prepare(LocalTarget.builder().build(), request(jobConf))

    fileNames(plan.getJars) shouldBe List(fileSinkJar, sourceJar)
  }


  test("native CDC source selects the database-cdc-source and cdc-runtime jars") {
    val jobConf = JobConfiguration.fromText(
      "yaml",
      job("local[2]", "client", Map.empty).getContent
        .replace("type: 'mock-source'", "type: 'mysql-cdc-source'"))

    val plan = prepare(LocalTarget.builder().build(), request(jobConf))

    fileNames(plan.getJars) should contain allOf (
      "database-cdc-source-1.0.jar", "cdc-runtime-1.0.jar", sinkJar)
    fileNames(plan.getJars) should not contain (sourceJar)
  }

  test("select object-store runtimes from URI schemes in the job configuration") {
    val base = job("local[2]", "client", Map.empty).getContent
      .replace(
        "    id: 'source-1'\n",
        "    id: 'source-1'\n    config:\n      path: 'oss://bucket/orders'\n")
      .replace(
        "    dependency: 'source-1'\n",
        "    dependency: 'source-1'\n    config:\n      path: 'abfs://container/out'\n")
    val plan = prepare(
      LocalTarget.builder().build(),
      request(JobConfiguration.fromText("yaml", base)))

    fileNames(plan.getJars) should contain ("hadoop-oss-runtime-1.0.jar")
    fileNames(plan.getJars) should contain ("hadoop-azure-runtime-1.0.jar")
  }

  test("include state backends and the S3 runtime without a connector type") {
    val custom = Files.createTempDirectory("pudding-base-jars")
    try {
      val libs = Files.createDirectories(custom.resolve("libs"))
      List(
        coreJar,
        sourceJar,
        sinkJar,
        "redis-state-provider-1.0.jar",
        "etcd-state-provider-1.0.jar",
        "hadoop-aws-runtime-1.0.jar").foreach(name => Files.createFile(libs.resolve(name)))
      val jobConf = job(custom, "local[2]", "client", Map.empty)
      val plan = prepare(LocalTarget.builder().build(), request(jobConf))
      fileNames(plan.getJars) shouldBe List(
        "etcd-state-provider-1.0.jar",
        "hadoop-aws-runtime-1.0.jar",
        sinkJar,
        sourceJar,
        "redis-state-provider-1.0.jar")
    } finally {
      deleteRecursively(custom)
    }
  }

  test("targets honour a caller supplied submission id") {
    val jobConf = job("local[2]", "client", Map.empty)
    val submission = SubmissionRequest.builder(jobConf)
      .submissionId("web-submission-1")
      .build()

    val plan = prepare(LocalTarget.builder().build(), submission)

    plan.getSubmissionId shouldBe "web-submission-1"
  }

  test("job configuration exposes an immutable Spark configuration view") {
    val jobConf = job(
      "local[2]",
      "client",
      Map("spark.executor.cores" -> "2", "spark.sql.shuffle.partitions" -> "4"))

    val sparkConf = jobConf.getSparkConf

    sparkConf.get("spark.executor.cores") shouldBe "2"
    sparkConf.get("spark.sql.shuffle.partitions") shouldBe "4"
    an[UnsupportedOperationException] should be thrownBy {
      sparkConf.put("spark.executor.memory", "1g")
    }
  }

  test("local target falls back to local[*] for a non-local job master") {
    val jobConf = job("yarn", "client", Map.empty)

    val plan = prepare(LocalTarget.builder().build(), request(jobConf))

    plan.getMaster shouldBe "local[*]"
  }

  test("local target supplies job settings omitted by a platform submission") {
    val yaml =
      """
        |job:
        |  mode: batch
        |  appName: 'platform job'
        |source:
        |  - type: 'mock-source'
        |    id: 'source-1'
        |sink:
        |  - type: 'mock-sink'
        |    id: 'sink-1'
        |    dependency: 'source-1'
        |""".stripMargin
    val target = LocalTarget.builder()
      .sparkHome("/opt/spark")
      .sparkPuddingHome(distribution.toString)
      .build()

    val plan = prepare(target, request(JobConfiguration.fromText("yaml", yaml)))

    plan.getSparkHome shouldBe "/opt/spark"
    plan.getMaster shouldBe "local[*]"
    plan.getDeployMode shouldBe "client"
    plan.getAppName shouldBe "platform job"
  }

  test("yarn target uses cluster mode, queue and a remote resource base") {
    val jobConf = job(
      "local[2]",
      "client",
      Map(
        "spark.master" -> "local[2]",
        "spark.executor.cores" -> "1",
        "spark.yarn.queue" -> "from-job"))
    val resourceBase = "hdfs://namenode/apps/spark-pudding/1.0.0"
    val target = YarnTarget.builder()
      .deployMode("cluster")
      .queue("root.analytics")
      .resourceBase(resourceBase)
      .sparkConf("spark.executor.instances", "3")
      .build()

    val plan = prepare(target, request(jobConf))

    plan.getTargetName shouldBe "yarn"
    plan.getMaster shouldBe "yarn"
    plan.getDeployMode shouldBe "cluster"
    plan.getAppResource shouldBe resourceBase + "/libs/" + coreJar
    plan.getJars.size shouldBe 2
    plan.getJars.get(0).startsWith(resourceBase + "/libs/") shouldBe true
    plan.getSparkConf.get("spark.yarn.queue") shouldBe "root.analytics"
    plan.getSparkConf.get("spark.executor.cores") shouldBe "1"
    plan.getSparkConf.get("spark.executor.instances") shouldBe "3"
    plan.getSparkConf.containsKey("spark.master") shouldBe false
    plan.getSparkConf.get("spark.yarn.dist.files") should endWith ("#pudding-job.conf")
    val stagedFile = plan.getSparkConf.get("spark.yarn.dist.files")
      .stripSuffix("#pudding-job.conf")
    Files.isRegularFile(Paths.get(stagedFile)) shouldBe true
    argument(plan, "--jobFile") shouldBe "pudding-job.conf"
    argument(plan, "--confType") shouldBe "yaml"
    plan.getAppArgs.asScala should not contain ("--jobText")
    plan.getTemporaryPaths.size shouldBe 1
  }

  test("yarn client mode passes the local configuration file") {
    val jobConf = job("yarn", "client", Map.empty)
    val plan = prepare(
      YarnTarget.builder().deployMode("client").build(),
      request(jobConf))

    plan.getDeployMode shouldBe "client"
    plan.getSparkConf.containsKey("spark.yarn.dist.files") shouldBe false
    Paths.get(argument(plan, "--jobFile")).isAbsolute shouldBe true
    plan.getAppArgs.asScala should not contain ("--jobText")
  }

  test("yarn client mode tracks the spark-submit process instead of the launcher handle") {
    // YARN client never connects to the Spark launcher protocol, so the launcher handle would
    // report a successful driver exit as LOST. The process exit status is authoritative.
    YarnTarget.usesProcessMonitoring("client") shouldBe true
    YarnTarget.usesProcessMonitoring("CLIENT") shouldBe true
    YarnTarget.usesProcessMonitoring("cluster") shouldBe false
    YarnTarget.usesProcessMonitoring(null) shouldBe false
  }

  test("yarn target appends the staged file to configured distributed files") {
    val jobConf = job("yarn", "cluster", Map("spark.yarn.dist.files" -> "/shared/extra.txt"))
    val plan = prepare(YarnTarget.builder().build(), request(jobConf))

    val distributed = plan.getSparkConf.get("spark.yarn.dist.files")
    distributed should startWith ("/shared/extra.txt,")
    distributed should endWith ("#pudding-job.conf")
    distributed.count(_ == ',') shouldBe 1
  }

  test("yarn target uses yarn even when the job master differs") {
    val jobConf = job("yarn", "cluster", Map.empty)
    val plan = prepare(YarnTarget.builder().build(), request(jobConf))

    plan.getMaster shouldBe "yarn"

    val localTarget = LocalTarget.builder().master("yarn").build()
    val error = intercept[SubmissionException] {
      localTarget.prepare(request(jobConf))
    }
    error.getMessage should include ("local target requires")
  }

  test("kubernetes target builds image and credential plan") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "client",
      Map("spark.executor.instances" -> "2"))
    val target = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding")
      .image("registry.example/spark-pudding:1.0.0")
      .namespace("data-platform")
      .serviceAccount("pudding-spark")
      .imagePullSecrets(Arrays.asList("regcred", "backup-regcred"))
      .jobSecret("pudding-job-config")
      .jobSecretKey("pipeline.yaml")
      .kubeconfig("/etc/pudding/kubeconfig")
      .sparkConf("spark.kubernetes.driver.label.team", "data")
      .build()

    val plan = target.prepare(request(jobConf))

    plan.getTargetName shouldBe "kubernetes"
    plan.getMaster shouldBe "k8s://https://cluster.example:443"
    plan.getDeployMode shouldBe "cluster"
    plan.getAppResource shouldBe "local:///opt/spark-pudding/libs/" + coreJar
    plan.getJars.size shouldBe 2
    plan.getSparkConf.get("spark.kubernetes.container.image") shouldBe
      "registry.example/spark-pudding:1.0.0"
    plan.getSparkConf.get("spark.kubernetes.namespace") shouldBe "data-platform"
    plan.getSparkConf.get(
      "spark.kubernetes.authenticate.driver.serviceAccountName") shouldBe "pudding-spark"
    plan.getSparkConf.get(
      "spark.kubernetes.container.image.pullSecrets") shouldBe "regcred,backup-regcred"
    plan.getSparkConf.get("spark.executor.instances") shouldBe "2"
    plan.getSparkConf.get(
      "spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG") shouldBe
      "pudding-job-config:pipeline.yaml"
    plan.getJobSecret.get shouldBe "pudding-job-config"
    plan.getJobSecretKey.get shouldBe "pipeline.yaml"
    plan.isJobSecretOwned shouldBe false
    plan.getAppArgs.asScala should contain ("--jobEnv")
    plan.getAppArgs.asScala should contain ("PUDDING_JOB_CONFIG")
    plan.getAppArgs.asScala should not contain ("--jobText")
    plan.getAppArgs.asScala should not contain ("--jobFile")
    plan.getTemporaryPaths.isEmpty shouldBe true
    plan.toString should not include ("mock-source")
    plan.getEnvironment.get("KUBECONFIG") shouldBe "/etc/pudding/kubeconfig"
    plan.getSubmissionId should not be null
    plan.getSparkConf.get(KubernetesTarget.SUBMISSION_LABEL_CONF) shouldBe
      plan.getSubmissionId
  }

  test("joins resource bases with trailing slashes") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val plan = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding/")
      .jobSecret("pudding-job-config")
      .build()
      .prepare(request(jobConf))

    plan.getAppResource shouldBe "local:///opt/spark-pudding/libs/" + coreJar
    plan.getSparkConf.get(
      "spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG") shouldBe
      "pudding-job-config:" + KubernetesTarget.DEFAULT_JOB_SECRET_KEY
  }

  test("kubernetes target generates an SDK-managed secret per submission") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val target = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding")
      .sparkConf(
        "spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG",
        "must-not-win:job.conf")
      .build()
    val submission = SubmissionRequest.builder(jobConf)
      .managedJobSecret()
      .jobSecretKey("pipeline.yaml")
      .build()

    val plan = prepare(target, submission)

    val secret = plan.getJobSecret.get
    secret should startWith ("pudding-job-")
    secret should endWith (plan.getSubmissionId)
    plan.getJobSecretKey.get shouldBe "pipeline.yaml"
    plan.isJobSecretOwned shouldBe true
    plan.getSparkConf.get(
      "spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG") shouldBe
      secret + ":pipeline.yaml"
    plan.toString should not include (secret)
  }

  test("a request-level secret overrides the target-level secret") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val target = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding")
      .jobSecret("target-secret")
      .jobSecretKey("target.conf")
      .build()
    val submission = SubmissionRequest.builder(jobConf)
      .jobSecret("request-secret")
      .jobSecretKey("request.yaml")
      .build()

    val plan = prepare(target, submission)

    plan.getJobSecret.get shouldBe "request-secret"
    plan.getJobSecretKey.get shouldBe "request.yaml"
    plan.isJobSecretOwned shouldBe false
    plan.getSparkConf.get(
      "spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG") shouldBe
      "request-secret:request.yaml"
  }

  test("kubernetes target validates request-level secret metadata") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val target = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding")
      .jobSecret("target-secret")
      .build()

    val invalidName = intercept[SubmissionException] {
      prepare(target, SubmissionRequest.builder(jobConf)
        .jobSecret("Invalid_Secret")
        .build())
    }
    invalidName.getMessage should include ("invalid Kubernetes Secret name")

    val invalidKey = intercept[SubmissionException] {
      prepare(target, SubmissionRequest.builder(jobConf)
        .jobSecret("request-secret")
        .jobSecretKey("bad/key")
        .build())
    }
    invalidKey.getMessage should include ("invalid Kubernetes Secret key")
  }

  test("kubernetes target rejects managed content larger than a Secret limit") {
    val large = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val padded = new StringBuilder(large.getContent)
      .append("#")
      .append("x" * 716800)
      .append("\n")
      .toString
    val largeJob = JobConfiguration.fromText("yaml", padded)
    val submission = SubmissionRequest.builder(largeJob)
      .managedJobSecret()
      .build()

    val error = intercept[SubmissionException] {
      KubernetesTarget.builder()
        .resourceBase("local:///opt/spark-pudding")
        .build()
        .prepare(submission)
    }
    error.getMessage should include ("larger than")
  }

  test("kubernetes target uses an image from the job configuration") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val target = KubernetesTarget.builder()
      .resourceBase("hdfs:///apps/spark-pudding/1.0.0")
      .jobSecret("pudding-job-config")
      .build()

    val plan = target.prepare(request(jobConf))

    plan.getSparkConf.get("spark.kubernetes.container.image") shouldBe "job/image:1"
    plan.getDeployMode shouldBe "cluster"
  }

  test("kubernetes target rejects a local resource base") {
    val jobConf = job("k8s://https://cluster.example:443", "client", Map.empty)
    val target = KubernetesTarget.builder()
      .image("registry.example/spark-pudding:1.0.0")
      .resourceBase(distribution.toString)
      .build()

    val error = intercept[SubmissionException] {
      target.prepare(request(jobConf))
    }
    error.getMessage should include ("cluster-visible resource base")
  }

  test("kubernetes target requires a container image") {
    val jobConf = job("k8s://https://cluster.example:443", "client", Map.empty)
    val target = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding")
      .build()

    val error = intercept[SubmissionException] {
      target.prepare(request(jobConf))
    }
    error.getMessage should include ("container image")
  }

  test("kubernetes target requires a job configuration secret") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))
    val target = KubernetesTarget.builder()
      .resourceBase("local:///opt/spark-pudding")
      .build()

    val error = intercept[SubmissionException] {
      target.prepare(request(jobConf))
    }
    error.getMessage should include ("job configuration Secret")
  }

  test("kubernetes target rejects invalid secret names and keys") {
    val jobConf = job(
      "k8s://https://cluster.example:443",
      "cluster",
      Map("spark.kubernetes.container.image" -> "job/image:1"))

    val invalidName = intercept[SubmissionException] {
      KubernetesTarget.builder()
        .resourceBase("local:///opt/spark-pudding")
        .jobSecret("Invalid_Secret")
        .build()
        .prepare(request(jobConf))
    }
    invalidName.getMessage should include ("invalid Kubernetes Secret name")

    val invalidKey = intercept[SubmissionException] {
      KubernetesTarget.builder()
        .resourceBase("local:///opt/spark-pudding")
        .jobSecret("pudding-job-config")
        .jobSecretKey("bad/key")
        .build()
        .prepare(request(jobConf))
    }
    invalidKey.getMessage should include ("invalid Kubernetes Secret key")
  }

  test("target requires the pudding-core jar") {
    val brokenDistribution = Files.createTempDirectory("pudding-broken-test")
    try {
      val libs = Files.createDirectories(brokenDistribution.resolve("libs"))
      Files.createFile(libs.resolve(sourceJar))
      val jobConf = job(brokenDistribution, "local[2]", "client", Map.empty)

      val error = intercept[SubmissionException] {
        LocalTarget.builder().build().prepare(request(jobConf))
      }
      error.getMessage should include ("pudding-core jar not found")
    } finally {
      deleteRecursively(brokenDistribution)
    }
  }

  private def job(
      master: String,
      deployMode: String,
      sparkConf: Map[String, String]): JobConfiguration = {
    job(distribution, master, deployMode, sparkConf)
  }

  private def job(
      distributionPath: Path,
      master: String,
      deployMode: String,
      sparkConf: Map[String, String]): JobConfiguration = {
    val yaml = new StringBuilder
    yaml.append("job:\n")
      .append("  mode: batch\n")
      .append("  appName: 'target test'\n")
      .append("  describe: 'target test'\n")
      .append("  sparkHome: '/opt/spark'\n")
      .append("  sparkPuddingHome: '").append(quote(distributionPath.toString)).append("'\n")
      .append("  master: '").append(quote(master)).append("'\n")
      .append("  deployMode: '").append(quote(deployMode)).append("'\n")

    if (sparkConf.nonEmpty) {
      yaml.append("  config:\n")
      sparkConf.foreach { case (key, value) =>
        yaml.append("    ").append(key).append(": '")
          .append(quote(value)).append("'\n")
      }
    }
    yaml.append("source:\n")
      .append("  - type: 'mock-source'\n")
      .append("    describe: 'source for target tests'\n")
      .append("    id: 'source-1'\n")
      .append("sink:\n")
      .append("  - type: 'mock-sink'\n")
      .append("    describe: 'sink for target tests'\n")
      .append("    id: 'sink-1'\n")
      .append("    dependency: 'source-1'\n")
    JobConfiguration.fromText("yaml", yaml.toString())
  }

  private def prepare(
      target: org.pudding.client.submit.ExecutionTarget,
      submissionRequest: SubmissionRequest): SubmissionPlan = {
    val plan = target.prepare(submissionRequest)
    preparedPlans += plan
    plan
  }

  private def request(jobConfiguration: JobConfiguration): SubmissionRequest = {
    SubmissionRequest.builder(jobConfiguration)
      .putEnvironment("FOO", "bar")
      .verbose(true)
      .build()
  }

  private def argument(plan: SubmissionPlan, name: String): String = {
    val args = plan.getAppArgs
    val index = args.indexOf(name)
    index should be >= 0
    args.get(index + 1)
  }

  private def fileNames(paths: java.util.List[String]): List[String] = {
    paths.asScala.map(path => path.substring(path.lastIndexOf('/') + 1)).toList
  }

  private def quote(value: String): String = {
    value.replace("'", "''")
  }

  private def deleteRecursively(path: Path): Unit = {
    if (path != null && Files.exists(path)) {
      val paths = Files.walk(path)
      try {
        paths.iterator().asScala.toList
          .sortBy(_.getNameCount)
          .reverse
          .foreach(candidate => Files.deleteIfExists(candidate))
      } finally {
        paths.close()
      }
    }
  }
}
