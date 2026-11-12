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

package org.pudding.core.spark

import org.pudding.core.PuddingException
import org.pudding.core.configure.{
  CatalogConf,
  ConnectionConf,
  ExecutionMode,
  JobConf,
  JobMode,
  JobPipelineConf,
  SinkConf,
  SourceConf,
  SyncStrategy,
  TransformConf
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable._

/**
 * Tests pipeline validation before Spark resources are allocated.
 */
class PipelineValidatorTest extends AnyFunSuite with Matchers {

  private def source(id: String = "source-1", sourceType: String = "mock_source"): SourceConf = {
    SourceConf(
      `type` = sourceType,
      describe = None,
      id = id,
      config = None
    )
  }

  private def transform(
      id: String,
      dependencies: Seq[String],
      transformType: String = "mock_filter"): TransformConf = {
    TransformConf(
      `type` = transformType,
      describe = None,
      id = id,
      config = None,
      dependencies = dependencies
    )
  }

  private def sink(
      id: String = "sink-1",
      dependency: String = "source-1",
      sinkType: String = "show-sink"): SinkConf = {
    SinkConf(
      `type` = sinkType,
      describe = None,
      id = id,
      config = None,
      dependency = dependency
    )
  }

  private def jobConf(
      appName: String = "pipeline validator test",
      deployMode: String = "client",
      mode: JobMode = JobMode.Batch,
      strategy: Option[SyncStrategy] = None,
      trigger: Option[String] = None): JobConf = {
    JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = appName,
      master = "local[2]",
      deployMode = deployMode,
      describe = None,
      config = None,
      mode = mode,
      strategy = strategy,
      trigger = trigger
    )
  }

  private def incrementalJob: JobConf =
    this.jobConf(
      mode = JobMode.Sync,
      strategy = Some(SyncStrategy.Incremental))

  private def fullJob: JobConf =
    this.jobConf(
      mode = JobMode.Sync,
      strategy = Some(SyncStrategy.Full))

  private def streamingJob(trigger: Option[String] = None): JobConf =
    this.jobConf(mode = JobMode.Streaming, trigger = trigger)

  private def pipeline(
      sources: Seq[SourceConf] = Seq(source()),
      transforms: Option[Seq[TransformConf]] = None,
      sinks: Seq[SinkConf] = Seq(sink()),
      job: JobConf = jobConf()): JobPipelineConf = {
    JobPipelineConf(sources, transforms, sinks, job)
  }

  test("accept a pipeline without transforms") {
    val plan = PipelineValidator.validate(pipeline())

    plan.orderedTransforms shouldBe empty
  }

  test("reject multiple sinks in batch mode") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sinks = Seq(sink("sink-1"), sink("sink-2"))))
    }
    error.getMessage should include ("job mode 'batch' requires exactly one sink")
  }

  test("order transforms by dependencies instead of configuration order") {
    val transform1 = transform("transform-1", Seq("source-1"))
    val transform2 = transform("transform-2", Seq("transform-1"))
    val conf = pipeline(
      transforms = Some(Seq(transform2, transform1)),
      sinks = Seq(sink(dependency = "transform-2"))
    )

    val plan = PipelineValidator.validate(conf)

    plan.orderedTransforms.map(_.id) shouldBe Seq("transform-1", "transform-2")
  }

  test("reject duplicated component ids") {
    val conf = pipeline(sources = Seq(source("duplicated"), source("duplicated")))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("duplicated component id: duplicated")
  }

  test("reject an id shared by a source and a sink") {
    val conf = pipeline(
      sources = Seq(source("shared-id")),
      sinks = Seq(sink("shared-id", dependency = "shared-id"))
    )

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("duplicated component id: shared-id")
  }

  test("reject duplicated sink ids") {
    val conf = pipeline(
      sinks = Seq(sink("sink-1"), sink("sink-1"))
    )

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("duplicated component id: sink-1")
  }

  test("reject unknown transform dependencies") {
    val transform1 = transform("transform-1", Seq("missing-source"))
    val conf = pipeline(
      transforms = Some(Seq(transform1)),
      sinks = Seq(sink(dependency = "transform-1"))
    )

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("unknown dependencies: missing-source")
  }

  test("reject transform dependency cycles") {
    val transform1 = transform("transform-1", Seq("transform-2"))
    val transform2 = transform("transform-2", Seq("transform-1"))
    val conf = pipeline(
      transforms = Some(Seq(transform1, transform2)),
      sinks = Seq(sink(dependency = "transform-2"))
    )

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("cycle")
  }

  test("reject unknown sink dependencies") {
    val conf = pipeline(sinks = Seq(sink(dependency = "missing-component")))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("sink 'sink-1' depends on unknown component: missing-component")
  }

  test("reject a transform without dependencies") {
    val transform1 = transform("transform-1", Seq.empty)
    val conf = pipeline(
      transforms = Some(Seq(transform1)),
      sinks = Seq(sink(dependency = "transform-1"))
    )

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("must have at least one dependency")
  }

  test("reject duplicated transform dependencies") {
    val transform1 = transform("transform-1", Seq("source-1", "source-1"))
    val conf = pipeline(
      transforms = Some(Seq(transform1)),
      sinks = Seq(sink(dependency = "transform-1"))
    )

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("duplicated dependencies: source-1")
  }

  test("reject unknown connector types") {
    val conf = pipeline(sources = Seq(source(sourceType = "missing-source")))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("unknown source type: missing-source")
    error.getMessage should include ("available:")
  }

  test("reject blank component ids") {
    val conf = pipeline(sources = Seq(source(" ")))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(conf)
    }
    error.getMessage should include ("source[0].id can not be empty")
  }

  test("reject an invalid job configuration") {
    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(job = jobConf(appName = " ")))
    }

    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(job = jobConf(deployMode = "standalone")))
    }
  }

  private def jdbcConnection(name: String = "jdbc-prod"): ConnectionConf = {
    ConnectionConf(
      name = name,
      `type` = "jdbc",
      endpoint = Some("jdbc:mysql://db:3306/app"),
      auth = Map("username" -> "pudding", "password" -> "secret"))
  }

  private def sharedPipeline(
      sourceConf: SourceConf,
      sinkConf: SinkConf = sink(),
      connections: Seq[ConnectionConf] = Seq(jdbcConnection()),
      catalogs: Seq[CatalogConf] = Seq.empty): JobPipelineConf = {
    JobPipelineConf(
      sources = Seq(sourceConf),
      transforms = None,
      sinks = Seq(sinkConf),
      job = jobConf(),
      connections = connections,
      catalogs = catalogs)
  }

  test("accept a connection reference on a capable source") {
    val sourceConf = SourceConf(
      "mock_shared_source", None, "source-1", None, connection = Some("jdbc-prod"))

    noException should be thrownBy PipelineValidator.validate(sharedPipeline(sourceConf))
  }

  test("accept a catalog reference on a capable sink") {
    val sinkConf = SinkConf(
      "mock_shared_sink", None, "sink-1", None, "source-1", catalog = Some("lake"))
    val catalog = CatalogConf("lake", "iceberg", properties = Map("type" -> "hadoop"))

    noException should be thrownBy PipelineValidator.validate(
      sharedPipeline(source(), sinkConf, catalogs = Seq(catalog)))
  }

  test("defer a Gravitino catalog's native type check to resolution") {
    val sourceConf = SourceConf(
      "mock_shared_source", None, "source-1", None, catalog = Some("lake"))
    val catalog = CatalogConf(
      "lake", "gravitino", properties = Map("metalake" -> "prod", "catalog" -> "lakehouse"))

    noException should be thrownBy PipelineValidator.validate(
      sharedPipeline(sourceConf, catalogs = Seq(catalog)))
  }

  test("reject an unknown connection reference") {
    val sourceConf = SourceConf(
      "mock_shared_source", None, "source-1", None, connection = Some("missing"))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(sourceConf))
    }
    error.getMessage should include ("source 'source-1' references unknown connection: missing")
  }

  test("reject a connection reference on a source that does not support it") {
    val sourceConf = SourceConf(
      "mock_source", None, "source-1", None, connection = Some("jdbc-prod"))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(sourceConf))
    }
    error.getMessage should include ("does not support connection references")
  }

  test("reject a connection type that the source does not accept") {
    val sourceConf = SourceConf(
      "mock_shared_source", None, "source-1", None, connection = Some("kafka-prod"))
    val kafka = ConnectionConf("kafka-prod", "kafka", endpoint = Some("broker:9092"))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(sourceConf, connections = Seq(kafka)))
    }
    error.getMessage should include ("requires connection types jdbc")
    error.getMessage should include ("but 'kafka-prod' is 'kafka'")
  }

  test("reject a catalog type that the sink does not accept") {
    val sinkConf = SinkConf(
      "mock_shared_sink", None, "sink-1", None, "source-1", catalog = Some("hive_prod"))
    val hive = CatalogConf("hive_prod", "hive")

    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(source(), sinkConf, catalogs = Seq(hive)))
    }
    error.getMessage should include ("requires catalog types iceberg")
    error.getMessage should include ("but 'hive_prod' is 'hive'")
  }

  test("reject a catalog reference on a sink that does not support it") {
    val sinkConf = SinkConf(
      "show-sink", None, "sink-1", None, "source-1", catalog = Some("lake"))
    val catalog = CatalogConf("lake", "iceberg", properties = Map("type" -> "hadoop"))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(source(), sinkConf, catalogs = Seq(catalog)))
    }
    error.getMessage should include ("does not support catalog references")
  }

  test("reject both connection and catalog on one component") {
    val sourceConf = SourceConf(
      "mock_shared_source",
      None,
      "source-1",
      None,
      connection = Some("jdbc-prod"),
      catalog = Some("lake"))
    val catalog = CatalogConf("lake", "iceberg", properties = Map("type" -> "hadoop"))

    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(sourceConf, catalogs = Seq(catalog)))
    }
    error.getMessage should include ("can not set both connection and catalog")
  }

  test("reject duplicated connection and catalog names") {
    val duplicatedConnection = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(
        source(),
        connections = Seq(jdbcConnection("same"), jdbcConnection("same"))))
    }
    duplicatedConnection.getMessage should include ("duplicated connection name: same")

    val duplicateCatalog = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(
        source(),
        catalogs = Seq(
          CatalogConf("lake", "iceberg", properties = Map("type" -> "hadoop")),
          CatalogConf("lake", "iceberg", properties = Map("type" -> "hadoop")))))
    }
    duplicateCatalog.getMessage should include ("duplicated catalog name: lake")
  }

  test("reject unknown connection and catalog provider types") {
    val unknownConnection = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(
        source(), connections = Seq(ConnectionConf("broken", "unknown"))))
    }
    unknownConnection.getMessage should include ("unknown connection type: unknown")

    val unknownCatalog = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(
        source(), catalogs = Seq(CatalogConf("broken", "unknown"))))
    }
    unknownCatalog.getMessage should include ("unknown catalog type: unknown")
  }

  test("reject a catalog that references an unknown connection") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(sharedPipeline(
        source(),
        catalogs = Seq(CatalogConf("lake", "iceberg", connection = Some("missing")))))
    }
    error.getMessage should include ("catalog 'lake' references unknown connection: missing")
  }

  private def incrementalSource(
      values: Map[String, Any],
      id: String = "source-1",
      withStatePath: Boolean = true): SourceConf = {
    val withState = if (!withStatePath || values.contains("statePath")) {
      values
    } else {
      values + ("statePath" -> "file:/tmp/pudding-validator-state")
    }
    SourceConf(
      "mock_incremental_source",
      None,
      id,
      Some(Map("incremental" -> withState)))
  }

  private def incrementalSink(
      id: String = "sink-1",
      dependency: String = "source-1",
      sinkType: String = "mock_incremental_sink"): SinkConf = {
    SinkConf(sinkType, None, id, None, dependency)
  }

  test("plan a sync incremental pipeline and pass its semantics to the sink") {
    val sourceConf = this.incrementalSource(Map(
      "semantics" -> "upsert",
      "keyFields" -> Seq("id"),
      "initial" -> "latest"))
    val plan = PipelineValidator.validate(pipeline(
      sources = Seq(sourceConf),
      sinks = Seq(this.incrementalSink()),
      job = this.incrementalJob))

    plan.jobMode shouldBe JobMode.Sync
    plan.syncStrategy shouldBe Some(SyncStrategy.Incremental)
    val incremental = plan.incremental.get
    incremental.sourceId shouldBe "source-1"
    incremental.spec.semantics shouldBe org.pudding.core.definition.IncrementalSemantics.Upsert
  }

  test("accept sync incremental append semantics with a start cursor") {
    val sourceConf = this.incrementalSource(Map(
      "semantics" -> "append",
      "start" -> "10"))
    val plan = PipelineValidator.validate(pipeline(
      sources = Seq(sourceConf),
      sinks = Seq(this.incrementalSink()),
      job = this.incrementalJob))

    plan.incremental.get.spec.start shouldBe Some("10")
  }

  test("accept a sync incremental pluggable state backend without statePath") {
    val sourceConf = this.incrementalSource(
      Map(
        "semantics" -> "append",
        "initial" -> "latest",
        "state" -> Map(
          "backend" -> "filesystem",
          "statePath" -> "file:/tmp/pudding-validator-state")),
      withStatePath = false)
    val plan = PipelineValidator.validate(pipeline(
      sources = Seq(sourceConf),
      sinks = Seq(this.incrementalSink()),
      job = this.incrementalJob))

    plan.incremental.get.spec.statePath shouldBe None
    plan.incremental.get.spec.state.map(_.backend) shouldBe Some("filesystem")
  }

  test("accept a sync incremental source without a first-run cursor") {
    val sourceConf = this.incrementalSource(Map("semantics" -> "append"))
    val plan = PipelineValidator.validate(pipeline(
      sources = Seq(sourceConf),
      sinks = Seq(this.incrementalSink()),
      job = this.incrementalJob))

    plan.incremental.get.spec.start shouldBe None
    plan.incremental.get.spec.initial shouldBe "none"
  }

  test("reject a non-incremental source in sync incremental") {
    val sourceConf = SourceConf(
      "mock_source",
      None,
      "source-1",
      Some(Map("incremental" -> Map("semantics" -> "append", "initial" -> "full"))))
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(sourceConf),
        sinks = Seq(this.incrementalSink()),
        job = this.incrementalJob))
    }
    error.getMessage should include ("does not support incremental reads")
  }

  test("reject a sync incremental sink without the source semantics") {
    val sourceConf = this.incrementalSource(Map(
      "semantics" -> "cdc",
      "keyFields" -> Seq("id"),
      "initial" -> "latest"))
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(sourceConf),
        sinks = Seq(this.incrementalSink(sinkType = "show-sink")),
        job = this.incrementalJob))
    }
    error.getMessage should include ("does not support incremental semantics 'cdc'")
    error.getMessage should include ("supported: append")
  }

  test("reject a sync incremental transform that can change row identity") {
    val sourceConf = this.incrementalSource(Map(
      "semantics" -> "append",
      "initial" -> "full"))
    val transformConf = TransformConf(
      "mock-join", None, "transform-1", None, Seq("source-1"))
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(sourceConf),
        transforms = Some(Seq(transformConf)),
        sinks = Seq(this.incrementalSink(dependency = "transform-1")),
        job = this.incrementalJob))
    }
    error.getMessage should include (
      "sync incremental requires transforms that preserve key, operation and ordering metadata")
  }

  test("ignore a transform outside the incremental sink path") {
    val sourceConf = this.incrementalSource(Map(
      "semantics" -> "append",
      "initial" -> "full"))
    val orphan = TransformConf(
      "mock-join", None, "transform-orphan", None, Seq("source-1"))
    val plan = PipelineValidator.validate(pipeline(
      sources = Seq(sourceConf),
      transforms = Some(Seq(orphan)),
      sinks = Seq(this.incrementalSink(dependency = "source-1")),
      job = this.incrementalJob))

    plan.incremental shouldBe defined
  }

  test("reject multiple sources in sync incremental") {
    val source1 = this.incrementalSource(
      Map("semantics" -> "append", "initial" -> "full"), "source-1")
    val source2 = this.incrementalSource(
      Map("semantics" -> "append", "initial" -> "full"), "source-2")
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(source1, source2),
        sinks = Seq(this.incrementalSink()),
        job = this.incrementalJob))
    }
    error.getMessage should include ("requires exactly one source")
  }

  test("accept sync full with transforms and a full-refresh sink") {
    val plan = PipelineValidator.validate(pipeline(
      transforms = Some(Seq(transform("transform-1", Seq("source-1")))),
      sinks = Seq(sink(dependency = "transform-1")),
      job = this.fullJob))

    plan.jobMode shouldBe JobMode.Sync
    plan.syncStrategy shouldBe Some(SyncStrategy.Full)
    plan.executionMode shouldBe ExecutionMode.Batch
    plan.orderedTransforms.map(_.id) shouldBe Seq("transform-1")
  }

  test("reject multiple sources or sinks in sync full") {
    val multipleSinks = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sinks = Seq(sink("sink-1"), sink("sink-2")),
        job = this.fullJob))
    }
    multipleSinks.getMessage should include ("requires exactly one sink")

    val multipleSources = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(source("source-1"), source("source-2")),
        job = this.fullJob))
    }
    multipleSources.getMessage should include ("requires exactly one source")
  }

  test("reject a sync full sink without full-refresh support") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sinks = Seq(this.incrementalSink()),
        job = this.fullJob))
    }
    error.getMessage should include ("does not support full-refresh")
  }

  test("require an atomic sink or an explicit non-atomic opt-in for sync full") {
    val nonAtomic = sink(sinkType = "mock_non_atomic_sink")
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sinks = Seq(nonAtomic),
        job = this.fullJob))
    }
    error.getMessage should include ("cannot atomically replace")

    val optedIn = nonAtomic.copy(config = Some(Map("allowNonAtomicReplace" -> true)))
    val plan = PipelineValidator.validate(pipeline(
      sinks = Seq(optedIn),
      job = this.fullJob))
    plan.syncStrategy shouldBe Some(SyncStrategy.Full)
  }

  test("reject incremental configuration in batch and sync full") {
    val sourceConf = this.incrementalSource(Map(
      "semantics" -> "append",
      "initial" -> "full",
      "statePath" -> "file:/tmp/full-state"))

    val batch = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(sourceConf),
        sinks = Seq(sink())))
    }
    batch.getMessage should include ("job.mode=batch does not allow")

    val full = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(sourceConf),
        sinks = Seq(sink()),
        job = this.fullJob))
    }
    full.getMessage should include ("job.strategy=full does not allow")
  }

  test("require job.strategy for sync mode") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        job = jobConf(mode = JobMode.Sync)))
    }
    error.getMessage should include ("requires job.strategy")
  }

  test("reject strategy or trigger on incompatible modes") {
    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        job = jobConf(mode = JobMode.Batch, strategy = Some(SyncStrategy.Full))))
    }.getMessage should include ("does not support job.strategy")

    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        job = jobConf(mode = JobMode.Batch, trigger = Some("once"))))
    }.getMessage should include ("does not support job.trigger")

    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        job = jobConf(
          mode = JobMode.Sync,
          strategy = Some(SyncStrategy.Full),
          trigger = Some("once"))))
    }.getMessage should include ("does not support job.trigger")

    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        job = jobConf(
          mode = JobMode.Streaming,
          strategy = Some(SyncStrategy.Incremental))))
    }.getMessage should include ("does not support job.strategy")
  }

  test("require a sync incremental block and state") {
    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(job = this.incrementalJob))
    }.getMessage should include ("requires an enabled source.config.incremental block")

    val missingState = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(this.incrementalSource(
          Map(
            "semantics" -> "upsert",
            "keyFields" -> Seq("id"),
            "initial" -> "latest"),
          withStatePath = false)),
        sinks = Seq(this.incrementalSink()),
        job = this.incrementalJob))
    }
    missingState.getMessage should include ("requires incremental.statePath")
  }

  test("reject non-lake sinks in sync incremental") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(this.incrementalSource(Map(
          "semantics" -> "append",
          "initial" -> "latest",
          "statePath" -> "file:/tmp/incremental-state"))),
        sinks = Seq(this.incrementalSink(sinkType = "show-sink")),
        job = this.incrementalJob))
    }
    error.getMessage should include ("not a supported incremental sink")
  }

  test("accept a streaming pipeline with capable components") {
    val plan = PipelineValidator.validate(pipeline(
      transforms = Some(Seq(transform("transform-1", Seq("source-1")))),
      sinks = Seq(sink(dependency = "transform-1")),
      job = this.streamingJob(Some("available-now"))))

    plan.jobMode shouldBe JobMode.Streaming
    plan.executionMode shouldBe ExecutionMode.Streaming
    plan.streamingTrigger shouldBe Some("available-now")
  }

  test("reject multiple sources or sinks in streaming mode") {
    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sinks = Seq(sink("sink-1"), sink("sink-2")),
        job = this.streamingJob()))
    }.getMessage should include ("requires exactly one sink")

    intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(source("source-1"), source("source-2")),
        job = this.streamingJob()))
    }.getMessage should include ("requires exactly one source")
  }

  test("reject streaming components without streaming support") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        transforms = Some(Seq(transform("transform-1", Seq("source-1"), "mock-join"))),
        sinks = Seq(sink(dependency = "transform-1")),
        job = this.streamingJob()))
    }
    error.getMessage should include ("does not support streaming execution")
  }

  test("validate transform streaming configuration before Spark starts") {
    val transformConf = TransformConf(
      "mock_stateful", None, "transform-1", None, Seq("source-1"))
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        transforms = Some(Seq(transformConf)),
        sinks = Seq(sink(dependency = "transform-1")),
        job = this.streamingJob()))
    }
    error.getMessage should include ("requires watermark")

    val accepted = PipelineValidator.validate(pipeline(
      transforms = Some(Seq(transformConf.copy(
        config = Some(Map("watermark" -> "1 minute"))))),
      sinks = Seq(sink(dependency = "transform-1")),
      job = this.streamingJob()))
    accepted.executionMode shouldBe ExecutionMode.Streaming
  }

  test("reject an unsupported streaming output mode before Spark starts") {
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sinks = Seq(sink(
          sinkType = "show-sink",
          dependency = "source-1").copy(
          config = Some(Map("outputMode" -> "update")))),
        job = this.streamingJob()))
    }
    error.getMessage should include ("does not support streaming outputMode=update")
  }

  test("reject stateful streaming when the sink has no checkpoint support") {
    val transformConf = TransformConf(
      "mock_stateful",
      None,
      "transform-1",
      Some(Map("watermark" -> "1 minute")),
      Seq("source-1"))
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        transforms = Some(Seq(transformConf)),
        sinks = Seq(sink(
          sinkType = "mock-no-stateful",
          dependency = "transform-1")),
        job = this.streamingJob()))
    }
    error.getMessage should include ("does not support stateful streaming")
    error.getMessage should include ("transform-1")
  }

  test("reject incremental configuration in streaming mode") {
    val sourceConf = SourceConf(
      "mock_source",
      None,
      "source-1",
      Some(Map("incremental" -> Map(
        "semantics" -> "append",
        "initial" -> "latest",
        "statePath" -> "file:/tmp/stream-incremental-state"))))
    val error = intercept[PuddingException] {
      PipelineValidator.validate(pipeline(
        sources = Seq(sourceConf),
        sinks = Seq(sink()),
        job = this.streamingJob()))
    }
    error.getMessage should include ("job.mode=streaming does not allow")
  }
}
