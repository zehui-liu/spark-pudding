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

package org.pudding.core.spark

import java.nio.file.{Files, Path, Paths}

import org.apache.hadoop.conf.Configuration
import org.pudding.core.PuddingException
import org.pudding.core.definition.IncrementalConfig
import org.pudding.core.configure.{
  ConnectionConf,
  JobConf,
  JobMode,
  JobPipelineConf,
  SinkConf,
  SourceConf,
  SyncStrategy,
  TransformConf
}
import org.pudding.core.incremental.{
  IncrementalMetrics,
  IncrementalPlanDescriptor,
  IncrementalStateCodec,
  IncrementalStateStore
}
import org.pudding.core.parser.YamlJobConfigureParser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable._
import scala.io.Source

/**
 * test buildJob method
 */
class BuildJobTest extends AnyFunSuite with Matchers with BeforeAndAfterAll{

  test("test for method buildSparkJob") {
    val yamlStr = Source.fromResource("spark-pudding-test-mock.yaml").mkString
    val jobPipelineCfg = new YamlJobConfigureParser().parseFromString(yamlStr)
    BuildJob.buildSparkJob(jobPipelineCfg)
  }

  test("test for a complex spark job") {
    val yamlStr = Source.fromResource("spark-pudding-test-mock-complex.yaml").mkString
    val jobPipelineCfg = new YamlJobConfigureParser().parseFromString(yamlStr)
    BuildJob.buildSparkJob(jobPipelineCfg)
  }

  test("execute transforms in dependency order when declared out of order") {
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "out of order transforms",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Batch
    )
    val sourceConf = SourceConf("mock_source", None, "source-1", None)
    val transform1 = TransformConf("mock_filter", None, "transform-1", None, Seq("source-1"))
    val transform2 = TransformConf("mock_filter", None, "transform-2", None, Seq("transform-1"))
    val sinkConf = SinkConf("show-sink", None, "sink-1", None, "transform-2")
    val pipelineConf = JobPipelineConf(
      sources = Seq(sourceConf),
      transforms = Some(Seq(transform2, transform1)),
      sinks = Seq(sinkConf),
      job = jobConf)

    BuildJob.buildSparkJob(pipelineConf)
  }

  test("resolve a connection and pass it to a capable source and sink") {
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "connection wiring",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Batch
    )
    val connection = ConnectionConf(
      name = "jdbc-prod",
      `type` = "jdbc",
      endpoint = Some("jdbc:mysql://db:3306/app"),
      auth = Map("username" -> "pudding", "password" -> "secret"))
    val sourceConf = SourceConf(
      "mock_shared_source", None, "source-1", None, connection = Some("jdbc-prod"))
    val sinkConf = SinkConf(
      "mock_shared_sink", None, "sink-1", None, "source-1", connection = Some("jdbc-prod"))
    val pipelineConf = JobPipelineConf(
      sources = Seq(sourceConf),
      transforms = None,
      sinks = Seq(sinkConf),
      job = jobConf,
      connections = Seq(connection))

    noException should be thrownBy BuildJob.buildSparkJob(pipelineConf)
  }

  test("commit an incremental cursor after every sink succeeds") {
    val beforeCommit = IncrementalMetrics.value(IncrementalMetrics.TransactionCommit)
    val beforeLock = IncrementalMetrics.value(IncrementalMetrics.LockAcquired)
    val beforeRun = IncrementalMetrics.value(IncrementalMetrics.RunCompleted)
    val statePath = Files.createTempDirectory("pudding-incremental-state")
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "incremental cursor",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Sync,
      strategy = Some(SyncStrategy.Incremental)
    )
    val sourceConf = SourceConf(
      "mock_incremental_source",
      None,
      "source-1",
      Some(Map("incremental" -> Map(
        "semantics" -> "upsert",
        "keyFields" -> Seq("id"),
        "initial" -> "latest",
        "statePath" -> statePath.toString))))
    val sinkConf = SinkConf(
      "mock_incremental_sink", None, "sink-1", None, "source-1")
    val pipelineConf = JobPipelineConf(
      sources = Seq(sourceConf),
      transforms = None,
      sinks = Seq(sinkConf),
      job = jobConf)

    BuildJob.buildSparkJob(pipelineConf)

    val state = new IncrementalStateStore(statePath.toString, new Configuration())
      .load("source-1")
    state.committedCursor shouldBe Some("10")
    state.pending shouldBe None
    state.lastTransactionId should not be empty
    state.lastCommittedAt should not be empty
    state.attempts shouldBe 0
    Files.exists(this.stateFile(statePath, "source-1")) shouldBe true
    Files.exists(this.lockFile(statePath, "source-1")) shouldBe false
    IncrementalMetrics.value(IncrementalMetrics.TransactionCommit) should be > beforeCommit
    IncrementalMetrics.value(IncrementalMetrics.LockAcquired) should be > beforeLock
    IncrementalMetrics.value(IncrementalMetrics.RunCompleted) should be > beforeRun
  }

  test("run a sync full pipeline and inject overwrite into the sink") {
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "sync full mode",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Sync,
      strategy = Some(SyncStrategy.Full))
    val pipelineConf = JobPipelineConf(
      sources = Seq(SourceConf("mock_source", None, "source-1", None)),
      transforms = None,
      sinks = Seq(SinkConf("mock_shared_sink", None, "sink-1", None, "source-1")),
      job = jobConf)

    noException should be thrownBy BuildJob.buildSparkJob(pipelineConf)
  }

  test("propagate incremental metadata into transforms") {
    val statePath = Files.createTempDirectory("pudding-incremental-transform-state")
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "incremental transform context",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Sync,
      strategy = Some(SyncStrategy.Incremental))
    val sourceConf = SourceConf(
      "mock_incremental_source",
      None,
      "source-1",
      Some(Map("incremental" -> Map(
        "semantics" -> "upsert",
        "keyFields" -> Seq("id"),
        "initial" -> "latest",
        "statePath" -> statePath.toString))))
    val transformConf = TransformConf(
      "mock_incremental_aware", None, "transform-1", None, Seq("source-1"))
    val sinkConf = SinkConf(
      "mock_incremental_sink", None, "sink-1", None, "transform-1")
    val pipelineConf = JobPipelineConf(
      sources = Seq(sourceConf),
      transforms = Some(Seq(transformConf)),
      sinks = Seq(sinkConf),
      job = jobConf)

    BuildJob.buildSparkJob(pipelineConf)

    Files.exists(this.stateFile(statePath, "source-1")) shouldBe true
  }

  test("wrap sink failures with the sink id and close the spark session") {
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "failing sink",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Batch
    )
    val sourceConf = SourceConf("mock_source", None, "source-1", None)
    val sinkConf = SinkConf(
      "show-sink",
      None,
      "sink-1",
      Some(Map[String, Any]("numRows" -> "not-a-number")),
      "source-1"
    )
    val pipelineConf = JobPipelineConf(Seq(sourceConf), None, Seq(sinkConf), jobConf)

    val error = intercept[PuddingException] {
      BuildJob.buildSparkJob(pipelineConf)
    }

    error.getMessage should include ("sink 'sink-1' (type 'show-sink') failed")
  }

  test("reject an uncommitted append transaction without an operator decision") {
    val statePath = Files.createTempDirectory("pudding-pending-append")
    val pipeline = this.incrementalPipeline(statePath, "append")
    this.withPendingTransaction(pipeline, "10")

    val error = intercept[PuddingException] {
      BuildJob.buildSparkJob(pipeline)
    }

    error.getMessage should include ("uncommitted incremental transaction")
    error.getMessage should include ("resolve")
    val state = this.incrementalState(statePath).load("source-1")
    state.committedCursor shouldBe None
    state.pending.map(_.transactionId) shouldBe Some("pending-txn")
    Files.exists(this.lockFile(statePath, "source-1")) shouldBe false
  }

  test("replay an uncommitted upsert transaction for an idempotent sink") {
    val statePath = Files.createTempDirectory("pudding-pending-upsert")
    val pipeline = this.incrementalPipeline(statePath, "upsert")
    this.withPendingTransaction(pipeline, "10")

    BuildJob.buildSparkJob(pipeline)

    val state = this.incrementalState(statePath).load("source-1")
    state.committedCursor shouldBe Some("10")
    state.pending shouldBe None
    state.lastTransactionId shouldBe Some("pending-txn")
    state.attempts shouldBe 0
  }

  test("allow an explicit retry for a non-idempotent sink") {
    val statePath = Files.createTempDirectory("pudding-pending-retry")
    val pipeline = this.incrementalPipeline(statePath, "append", Some("retry"))
    this.withPendingTransaction(pipeline, "10")

    BuildJob.buildSparkJob(pipeline)

    val state = this.incrementalState(statePath).load("source-1")
    state.committedCursor shouldBe Some("10")
    state.pending shouldBe None
  }

  test("reject a pending transaction created for a different plan") {
    val statePath = Files.createTempDirectory("pudding-pending-plan-change")
    val original = this.incrementalPipeline(statePath, "upsert")
    this.withPendingTransaction(original, "10")
    val changed = this.incrementalPipeline(
      statePath,
      "upsert",
      sinkConfig = Some(Map[String, Any]("table" -> "different-target")))

    val error = intercept[PuddingException] {
      BuildJob.buildSparkJob(changed)
    }

    error.getMessage should include ("different source, sink or configuration")
    val state = this.incrementalState(statePath).load("source-1")
    state.pending.map(_.transactionId) shouldBe Some("pending-txn")
  }

  test("require a bootstrap cursor when the state directory is empty") {
    val statePath = Files.createTempDirectory("pudding-empty-incremental-state")

    val error = intercept[PuddingException] {
      BuildJob.buildSparkJob(
        this.incrementalPipeline(statePath, "append", initial = None))
    }

    error.getMessage should include ("requires start or initial=latest/full")
  }

  test("leave a pending transaction when the sink fails") {
    val statePath = Files.createTempDirectory("pudding-pending-sink-failure")
    val pipeline = this.incrementalPipeline(
      statePath,
      "upsert",
      sinkConfig = Some(Map[String, Any]("fail" -> true)))

    val error = intercept[PuddingException] {
      BuildJob.buildSparkJob(pipeline)
    }

    error.getMessage should include ("mock incremental sink failure")
    val state = this.incrementalState(statePath).load("source-1")
    state.committedCursor shouldBe None
    state.pending.map(_.toCursor) shouldBe Some("10")
    state.attempts shouldBe 1
    state.lastFailureAt should not be empty
    state.lastFailureReason.get should include ("mock incremental sink failure")
    Files.exists(this.lockFile(statePath, "source-1")) shouldBe false
  }

  private def incrementalPipeline(
      statePath: Path,
      semantics: String,
      recovery: Option[String] = None,
      sinkConfig: Option[Map[String, Any]] = None,
      initial: Option[String] = Some("latest")): JobPipelineConf = {
    val jobConf = JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = s"incremental $semantics",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false")),
      mode = JobMode.Sync,
      strategy = Some(SyncStrategy.Incremental))
    var incremental = Map[String, Any](
      "semantics" -> semantics,
      "statePath" -> statePath.toString)
    initial.foreach(value => incremental += "initial" -> value)
    if (semantics != "append") {
      incremental += "keyFields" -> Seq("id")
    }
    recovery.foreach(value => incremental += "recovery" -> value)
    val sourceConf = SourceConf(
      "mock_incremental_source",
      None,
      "source-1",
      Some(Map("incremental" -> incremental)))
    val sinkConf = SinkConf(
      "mock_incremental_sink", None, "sink-1", sinkConfig, "source-1")
    JobPipelineConf(
      sources = Seq(sourceConf),
      transforms = None,
      sinks = Seq(sinkConf),
      job = jobConf)
  }

  private def withPendingTransaction(
      pipeline: JobPipelineConf,
      toCursor: String): Unit = {
    val sourceConf = pipeline.sources.head
    val sinkConf = pipeline.sinks.head
    val spec = IncrementalConfig.parse(sourceConf.config, sourceConf.id).get
    val plan = IncrementalPlanDescriptor.create(
      sourceType = sourceConf.`type`,
      sourceId = sourceConf.id,
      sinkType = sinkConf.`type`,
      sinkId = sinkConf.id,
      semantics = spec.semantics,
      keyFields = spec.keyFields,
      opColumn = spec.opColumn,
        sourceConfig = sourceConf.config.map(_.filterNot { case (key, _) => key == "incremental" }),
      sinkConfig = sinkConf.config,
      stateBackend = "filesystem")
    val store = this.incrementalState(Paths.get(spec.statePath.get))
    store.acquire(sourceConf.id)
    try {
      store.begin(sourceConf.id, None, toCursor, "pending-txn", plan)
    } finally {
      store.release(sourceConf.id)
    }
  }

  private def incrementalState(statePath: Path): IncrementalStateStore = {
    new IncrementalStateStore(statePath.toString, new Configuration())
  }

  private def fileBase(sourceId: String): String = {
    sourceId.replaceAll("[^A-Za-z0-9._-]", "_") + "-" +
      IncrementalStateCodec.sha256(sourceId).take(12)
  }

  private def stateFile(statePath: Path, sourceId: String): Path =
    statePath.resolve(this.fileBase(sourceId) + ".state")

  private def lockFile(statePath: Path, sourceId: String): Path =
    statePath.resolve(this.fileBase(sourceId) + ".lock")
}
