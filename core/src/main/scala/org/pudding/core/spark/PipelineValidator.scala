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

import java.util.Locale

import scala.collection.mutable

import org.pudding.core.PuddingException
import org.pudding.core.catalog.CatalogProvider
import org.pudding.core.configure._
import org.pudding.core.connection.ConnectionProvider
import org.pudding.core.definition.{ComponentCapabilities, DataSink, DataSource, DataTransform, IncrementalConfig, IncrementalSemantics, IncrementalSource, IncrementalSpec}
import org.pudding.core.util.StreamingTrigger

/**
 * A validated incremental source and its downstream sinks.
 */
case class IncrementalPlan(
    sourceId: String,
    spec: IncrementalSpec)

/**
 * A validated pipeline execution plan.
 *
 * @param orderedTransforms transforms in dependency order
 * @param incremental       incremental execution metadata, when the source selects it
 * @param jobMode           user-facing mode: batch, sync or streaming
 * @param syncStrategy      full or incremental strategy for sync jobs
 * @param executionMode     internal batch or streaming execution engine
 * @param streamingTrigger  job-level streaming trigger, only for streaming mode
 */
case class PipelinePlan(
    orderedTransforms: Seq[TransformConf],
    incremental: Option[IncrementalPlan] = None,
    jobMode: JobMode,
    syncStrategy: Option[SyncStrategy] = None,
    executionMode: ExecutionMode,
    streamingTrigger: Option[String] = None)

/**
 * Validate a pipeline before Spark resources are allocated. Validation covers the graph
 * structure (ids, dependencies and cycles), shared connection/catalog references, the required
 * job mode, execution-mode capabilities and incremental semantics/metadata requirements.
 */
object PipelineValidator {

  private val VALID_DEPLOY_MODES = Set("client", "cluster")

  private case class ModeSelection(
      jobMode: JobMode,
      syncStrategy: Option[SyncStrategy],
      executionMode: ExecutionMode,
      trigger: Option[String])

  def validate(jobPipelineConf: JobPipelineConf): PipelinePlan = {
    if (jobPipelineConf == null) {
      throw new PuddingException("job pipeline configure can not be null!")
    }

    this.validateJob(jobPipelineConf.job)
    val mode = this.resolveMode(jobPipelineConf.job)

    val sourceCfs = Option(jobPipelineConf.sources).getOrElse(
      throw new PuddingException("source configure can not be null!"))
    val transformCfs = jobPipelineConf.transforms match {
      case Some(transforms) if transforms != null => transforms
      case Some(_) => throw new PuddingException("transform configure can not be null!")
      case None => Seq.empty[TransformConf]
    }
    val sinkCfs = Option(jobPipelineConf.sinks).getOrElse(
      throw new PuddingException("sink configure can not be null!"))
    val connectionCfs = Option(jobPipelineConf.connections).getOrElse(Seq.empty)
    val catalogCfs = Option(jobPipelineConf.catalogs).getOrElse(Seq.empty)

    if (sourceCfs.isEmpty) {
      throw new PuddingException("source configure can not be empty!")
    }
    if (sinkCfs.isEmpty) {
      throw new PuddingException("sink configure can not be empty!")
    }

    this.validateConnections(connectionCfs)
    this.validateCatalogs(catalogCfs, connectionCfs)
    this.validateSources(sourceCfs)
    this.validateTransforms(transformCfs)
    this.validateSinks(sinkCfs)
    this.validateUniqueIds(sourceCfs, transformCfs, sinkCfs)
    this.validateConnectorTypes(sourceCfs, transformCfs, sinkCfs)
    this.validateSharedReferences(sourceCfs, sinkCfs, connectionCfs, catalogCfs)

    val producerIds = sourceCfs.map(_.id).toSet ++ transformCfs.map(_.id)
    this.validateTransformDependencies(transformCfs, producerIds)
    this.validateSinkDependencies(sinkCfs, producerIds)

    val orderedTransforms = this.orderTransforms(transformCfs, sourceCfs.map(_.id).toSet)
    this.validateModePolicy(mode, sourceCfs, transformCfs, sinkCfs)
    val incremental = this.validateIncremental(
      sourceCfs, sinkCfs, transformCfs, mode)
    PipelinePlan(
      orderedTransforms = orderedTransforms,
      incremental = incremental,
      jobMode = mode.jobMode,
      syncStrategy = mode.syncStrategy,
      executionMode = mode.executionMode,
      streamingTrigger = mode.trigger)
  }

  /**
   * Resolve the execution engine and validate mode-specific job fields. The parser has already
   * required `job.mode`; this method enforces which of `job.strategy` and `job.trigger` are valid.
   */
  private def resolveMode(jobConf: JobConf): ModeSelection = {
    val executionMode = jobConf.mode.executionMode
    jobConf.mode match {
      case JobMode.Batch =>
        if (jobConf.strategy.isDefined) {
          throw new PuddingException(
            "job.mode=batch does not support job.strategy; use job.mode=sync for " +
              "full or incremental synchronization")
        }
        if (jobConf.trigger.isDefined) {
          throw new PuddingException(
            "job.mode=batch does not support job.trigger; use job.mode=streaming")
        }
        ModeSelection(JobMode.Batch, None, executionMode, None)

      case JobMode.Sync =>
        if (jobConf.trigger.isDefined) {
          throw new PuddingException(
            "job.mode=sync does not support job.trigger; use job.mode=streaming")
        }
        val strategy = jobConf.strategy.getOrElse {
          throw new PuddingException(
            "job.mode=sync requires job.strategy: full or incremental")
        }
        ModeSelection(JobMode.Sync, Some(strategy), executionMode, None)

      case JobMode.Streaming =>
        if (jobConf.strategy.isDefined) {
          throw new PuddingException(
            "job.mode=streaming does not support job.strategy; streaming is continuous")
        }
        jobConf.trigger.foreach(StreamingTrigger.validate)
        ModeSelection(JobMode.Streaming, None, executionMode, jobConf.trigger)
    }
  }

  private def validateModePolicy(
      mode: ModeSelection,
      sourceCfs: Seq[SourceConf],
      transformCfs: Seq[TransformConf],
      sinkCfs: Seq[SinkConf]): Unit = {
    val execution = mode.executionMode

    val sourceServices = DataSource.services()
    val transformServices = DataTransform.services()
    val sinkServices = DataSink.services()

    sourceCfs.foreach { sourceConf =>
      val source = sourceServices(sourceConf.`type`)
      this.requireExecutionMode(
        "source", sourceConf.id, sourceConf.`type`, source.supportedExecutionModes, execution)
    }
    transformCfs.foreach { transformConf =>
      val transform = transformServices(transformConf.`type`)
      this.requireExecutionMode(
        "transform", transformConf.id, transformConf.`type`,
        transform.supportedExecutionModes, execution)
      transform.validateConfig(transformConf.config, execution)
    }
    sinkCfs.foreach { sinkConf =>
      val sink = sinkServices(sinkConf.`type`)
      this.requireExecutionMode(
        "sink", sinkConf.id, sinkConf.`type`, sink.supportedExecutionModes, execution)
    }

    if (execution == ExecutionMode.Streaming) {
      sinkCfs.foreach { sinkConf =>
        val sink = sinkServices(sinkConf.`type`)
        StreamingOutputMode.configured(sinkConf.config).foreach { mode =>
          if (!sink.supportedStreamingOutputModes.contains(mode)) {
            throw new PuddingException(
              s"sink '${sinkConf.id}' (type '${sinkConf.`type`}') does not support " +
                s"streaming ${StreamingOutputMode.Key}=$mode; supported: " +
                sink.supportedStreamingOutputModes.toSeq.sorted.mkString(", "))
          }
        }
      }
    }

    val statefulTransforms = transformCfs.filter { transformConf =>
      transformServices.get(transformConf.`type`).exists(_.streamingStateful)
    }
    if (execution == ExecutionMode.Streaming && statefulTransforms.nonEmpty) {
      sinkCfs.foreach { sinkConf =>
        val sink = sinkServices(sinkConf.`type`)
        if (!sink.supportsStatefulStreaming) {
          throw new PuddingException(
            s"sink '${sinkConf.id}' (type '${sinkConf.`type`}') does not support stateful " +
              "streaming; stateful transforms require a sink with a stable checkpoint: " +
              statefulTransforms.map(_.id).mkString(", "))
        }
      }
    }

    // A run has one commit boundary. Multiple sinks would either need a distributed transaction
    // or leave earlier sinks committed when a later sink fails, so every mode writes one sink.
    this.requireSingleSink(sinkCfs, mode.jobMode)

    mode.jobMode match {
      case JobMode.Batch =>
        // Batch is the general bounded-processing mode. Incremental source configuration is
        // validated separately because it belongs to sync + incremental.
      case JobMode.Sync =>
        this.requireSingleSource(sourceCfs, JobMode.Sync)
        if (mode.syncStrategy.contains(SyncStrategy.Full)) {
          val sinkConf = sinkCfs.head
          val sink = sinkServices(sinkConf.`type`)
          if (!sink.supportsFullRefresh) {
            throw new PuddingException(
              s"sink '${sinkConf.id}' (type '${sinkConf.`type`}') does not support " +
                "full-refresh writes; choose a sink that can replace the target contents")
          }
          if (!sink.supportsAtomicReplace && !this.allowNonAtomicReplace(sinkConf)) {
            throw new PuddingException(
              s"sink '${sinkConf.id}' (type '${sinkConf.`type`}') cannot atomically " +
                "replace its target for sync full; choose an atomic sink or set " +
                s"config.${DataSink.AllowNonAtomicReplaceKey}=true to accept a non-atomic " +
                "refresh")
          }
        }

      case JobMode.Streaming =>
        // Every streaming sink starts a query and waits for it. Running sinks sequentially
        // means the first continuous query blocks all later sinks, and independent checkpoints
        // would not provide a shared commit boundary. Keep the user contract one-to-one.
        this.requireSingleSource(sourceCfs, JobMode.Streaming)
    }
  }

  private def requireSingleSource(sourceCfs: Seq[SourceConf], mode: JobMode): Unit = {
    if (sourceCfs.size != 1) {
      throw new PuddingException(
        s"job mode '${mode.name}' requires exactly one source, but got: ${sourceCfs.size}")
    }
  }

  private def requireSingleSink(sinkCfs: Seq[SinkConf], mode: JobMode): Unit = {
    if (sinkCfs.size != 1) {
      throw new PuddingException(
        s"job mode '${mode.name}' requires exactly one sink, but got: ${sinkCfs.size}")
    }
  }

  private def allowNonAtomicReplace(sinkConf: SinkConf): Boolean = {
    val key = DataSink.AllowNonAtomicReplaceKey
    sinkConf.config.flatMap(_.get(key)).filter(_ != null).map {
      case value: Boolean => value
      case value: String if value.trim.equalsIgnoreCase("true") => true
      case value: String if value.trim.equalsIgnoreCase("false") => false
      case _ =>
        throw new PuddingException(
          s"sink '${sinkConf.id}' config.$key must be true or false")
    }.getOrElse(false)
  }

  private def requireExecutionMode(
      component: String,
      id: String,
      componentType: String,
      supported: Set[ExecutionMode],
      expected: ExecutionMode): Unit = {
    if (!supported.contains(expected)) {
      throw new PuddingException(
        s"$component '$id' (type '$componentType') does not support " +
          s"${expected.name} execution; supported: " +
          supported.map(_.name).toSeq.sorted.mkString(", "))
    }
  }


  private def validateIncremental(
      sourceCfs: Seq[SourceConf],
      sinkCfs: Seq[SinkConf],
      transformCfs: Seq[TransformConf],
      mode: ModeSelection): Option[IncrementalPlan] = {
    val configured = sourceCfs.flatMap { sourceConf =>
      IncrementalConfig.parse(sourceConf.config, sourceConf.id).map(sourceConf -> _)
    }
    val strategy = mode.jobMode match {
      case JobMode.Sync => mode.syncStrategy
      case _ => None
    }

    if (configured.isEmpty) {
      if (mode.jobMode == JobMode.Sync && strategy.contains(SyncStrategy.Incremental)) {
        throw new PuddingException(
          "job.mode=sync with job.strategy=incremental requires an enabled " +
            "source.config.incremental block")
      }
      return None
    }

    if (mode.jobMode == JobMode.Batch) {
      throw new PuddingException(
        "job.mode=batch does not allow an incremental source configuration; use " +
          "job.mode=sync with job.strategy=incremental")
    }
    if (mode.jobMode == JobMode.Streaming) {
      throw new PuddingException(
        "job.mode=streaming does not allow an incremental source configuration")
    }
    if (strategy.contains(SyncStrategy.Full)) {
      throw new PuddingException(
        "job.mode=sync with job.strategy=full does not allow an incremental source " +
          "configuration")
    }

    val (sourceConf, spec) = configured.head
    val source = DataSource.services().getOrElse(sourceConf.`type`,
      throw new PuddingException(s"unknown source type: ${sourceConf.`type`}"))
    val incrementalSource = source match {
      case value: IncrementalSource => value
      case _ =>
        throw new PuddingException(
          s"source '${sourceConf.id}' (type '${sourceConf.`type`}') does not support " +
            "incremental reads")
    }

    if (!incrementalSource.supportedIncrementalSemantics.contains(spec.semantics)) {
      throw new PuddingException(
        s"source '${sourceConf.id}' supports incremental semantics " +
          s"${incrementalSource.supportedIncrementalSemantics.map(_.name).toSeq.sorted.mkString(", ")}, " +
          s"but '${spec.semantics.name}' was requested")
    }
    if (spec.semantics != IncrementalSemantics.Append && spec.keyFields.isEmpty) {
      throw new PuddingException(
        s"source '${sourceConf.id}' incremental semantics '${spec.semantics.name}' " +
          "requires keyFields")
    }
    if (!source.supportsIncrementalSync) {
      throw new PuddingException(
        s"source '${sourceConf.id}' (type '${sourceConf.`type`}') is not a supported " +
          "incremental source")
    }
    if (spec.statePath.isEmpty && spec.state.isEmpty) {
      throw new PuddingException(
        "job.mode=sync with job.strategy=incremental requires incremental.statePath " +
          "or incremental.state")
    }

    val transformServices = DataTransform.services()
    val transformById = transformCfs.map(transform => transform.id -> transform).toMap
    val incrementalPath = mutable.Set.empty[String]
    def visit(producerId: String): Unit = {
      transformById.get(producerId).foreach { transform =>
        if (incrementalPath.add(transform.id)) {
          transform.dependencies.foreach(visit)
        }
      }
    }
    sinkCfs.foreach(sink => visit(sink.dependency))
    // Only transforms that can feed the incremental sink must preserve the metadata. An
    // unrelated branch is not on the incremental path and may use any transform.
    val unsupportedTransforms = transformCfs
      .filter(transform => incrementalPath.contains(transform.id))
      .filterNot { transformConf =>
        transformServices.get(transformConf.`type`)
          .exists(_.preservesIncrementalMetadata)
      }
      .map(transform => s"${transform.id} (${transform.`type`})")
    if (unsupportedTransforms.nonEmpty) {
      throw new PuddingException(
        "sync incremental requires transforms that preserve key, operation and ordering " +
          s"metadata; unsupported: ${unsupportedTransforms.mkString(", ")}")
    }

    val sinkServices = DataSink.services()
    sinkCfs.foreach { sinkConf =>
      val sink = sinkServices.getOrElse(sinkConf.`type`,
        throw new PuddingException(s"unknown sink type: ${sinkConf.`type`}"))
      if (!sink.supportedIncrementalSemantics.contains(spec.semantics)) {
        throw new PuddingException(
          s"sink '${sinkConf.id}' (type '${sinkConf.`type`}') does not support " +
            s"incremental semantics '${spec.semantics.name}'; supported: " +
            sink.supportedIncrementalSemantics.map(_.name).toSeq.sorted.mkString(", "))
      }
      if (!sink.supportsIncrementalSync) {
        throw new PuddingException(
          s"sink '${sinkConf.id}' (type '${sinkConf.`type`}') is not a supported " +
            "incremental sink")
      }
    }

    Some(IncrementalPlan(sourceConf.id, spec))
  }

  private def validateJob(jobConf: JobConf): Unit = {
    if (jobConf == null) {
      throw new PuddingException("job configure can not be null!")
    }

    this.requireText(jobConf.appName, "job.appName")
    if (jobConf.deployMode != null && jobConf.deployMode.trim.nonEmpty &&
      !VALID_DEPLOY_MODES.contains(jobConf.deployMode.trim.toLowerCase(Locale.ROOT))) {
      throw new PuddingException(
        s"unsupported deployMode: ${jobConf.deployMode}, only client and cluster are supported!")
    }

  }

  private def validateConnections(connectionCfs: Seq[ConnectionConf]): Unit = {
    connectionCfs.zipWithIndex.foreach { case (connectionConf, index) =>
      if (connectionConf == null) {
        throw new PuddingException(s"connection[$index] can not be null!")
      }
      this.requireText(connectionConf.name, s"connection[$index].name")
      this.requireText(connectionConf.`type`, s"connection '${connectionConf.name}'.type")
    }

    this.validateUniqueNames(connectionCfs.map(_.name), "connection")
    val available = ConnectionProvider.identifiers
    val unknown = connectionCfs.map(_.`type`).distinct.filterNot(available.contains).sorted
    if (unknown.nonEmpty) {
      throw new PuddingException(
        s"unknown connection type: ${unknown.mkString(", ")}, available: " +
          this.available(available))
    }
  }

  private def validateCatalogs(
      catalogCfs: Seq[CatalogConf],
      connectionCfs: Seq[ConnectionConf]): Unit = {
    catalogCfs.zipWithIndex.foreach { case (catalogConf, index) =>
      if (catalogConf == null) {
        throw new PuddingException(s"catalog[$index] can not be null!")
      }
      this.requireText(catalogConf.name, s"catalog[$index].name")
      this.requireText(catalogConf.`type`, s"catalog '${catalogConf.name}'.type")
      catalogConf.connection.foreach { connection =>
        this.requireText(connection, s"catalog '${catalogConf.name}'.connection")
        if (!connectionCfs.exists(_.name == connection)) {
          throw new PuddingException(
            s"catalog '${catalogConf.name}' references unknown connection: $connection")
        }
      }
    }

    this.validateUniqueNames(catalogCfs.map(_.name), "catalog")
    val available = CatalogProvider.identifiers
    val unknown = catalogCfs.map(_.`type`).distinct.filterNot(available.contains).sorted
    if (unknown.nonEmpty) {
      throw new PuddingException(
        s"unknown catalog type: ${unknown.mkString(", ")}, available: " +
          this.available(available))
    }
  }

  /**
   * A connection or catalog reference on a component that cannot consume it is a configuration
   * error, not an ignored field. Checking here prevents a job from failing much later or,
   * worse, silently using a different endpoint.
   */
  private def validateSharedReferences(
      sourceCfs: Seq[SourceConf],
      sinkCfs: Seq[SinkConf],
      connectionCfs: Seq[ConnectionConf],
      catalogCfs: Seq[CatalogConf]): Unit = {
    val connectionNames = connectionCfs.map(_.name).toSet
    val catalogNames = catalogCfs.map(_.name).toSet
    val connectionTypes = connectionCfs.map(conf => conf.name -> conf.`type`).toMap
    val catalogTypes = catalogCfs.map(conf => conf.name -> conf.`type`).toMap
    val sourceCapabilities = DataSource.capabilities()
    val sinkCapabilities = DataSink.capabilities()

    sourceCfs.foreach { sourceConf =>
      val capabilities = sourceCapabilities.getOrElse(
        sourceConf.`type`, ComponentCapabilities())
      this.validateReference(
        "source", sourceConf.id, sourceConf.`type`, sourceConf.connection, sourceConf.catalog,
        connectionNames, catalogNames, connectionTypes, catalogTypes, capabilities)
    }

    sinkCfs.foreach { sinkConf =>
      val capabilities = sinkCapabilities.getOrElse(
        sinkConf.`type`, ComponentCapabilities())
      this.validateReference(
        "sink", sinkConf.id, sinkConf.`type`, sinkConf.connection, sinkConf.catalog,
        connectionNames, catalogNames, connectionTypes, catalogTypes, capabilities)
    }
  }

  private def validateReference(
      component: String,
      id: String,
      componentType: String,
      connection: Option[String],
      catalog: Option[String],
      connectionNames: Set[String],
      catalogNames: Set[String],
      connectionTypes: Map[String, String],
      catalogTypes: Map[String, String],
      capabilities: ComponentCapabilities): Unit = {
    if (connection.isDefined && catalog.isDefined) {
      throw new PuddingException(
        s"$component '$id' (type '$componentType') can not set both connection and catalog")
    }
    connection.foreach { name =>
      this.requireText(name, s"$component '$id'.connection")
      if (!connectionNames.contains(name)) {
        throw new PuddingException(
          s"$component '$id' references unknown connection: $name")
      }
      if (!capabilities.supportsConnection) {
        throw new PuddingException(
          s"$component '$id' (type '$componentType') does not support connection references")
      }
      val actualType = connectionTypes(name)
      if (capabilities.connectionTypes.nonEmpty &&
        !capabilities.connectionTypes.contains(actualType)) {
        throw new PuddingException(
          s"$component '$id' (type '$componentType') requires connection types " +
            s"${capabilities.connectionTypes.toSeq.sorted.mkString(", ")}, " +
            s"but '$name' is '$actualType'")
      }
    }
    catalog.foreach { name =>
      this.requireText(name, s"$component '$id'.catalog")
      if (!catalogNames.contains(name)) {
        throw new PuddingException(
          s"$component '$id' references unknown catalog: $name")
      }
      if (!capabilities.supportsCatalog) {
        throw new PuddingException(
          s"$component '$id' (type '$componentType') does not support catalog references")
      }
      val actualType = catalogTypes(name)
      // Gravitino is a metadata-only provider; its native catalog type is resolved on the driver.
      // The component's own post-resolution check rejects an incompatible native catalog.
      val typeSupported = actualType == "gravitino" ||
        capabilities.catalogTypes.contains(actualType)
      if (capabilities.catalogTypes.nonEmpty && !typeSupported) {
        throw new PuddingException(
          s"$component '$id' (type '$componentType') requires catalog types " +
            s"${capabilities.catalogTypes.toSeq.sorted.mkString(", ")}, " +
            s"but '$name' is '$actualType'")
      }
    }
  }

  private def validateUniqueNames(names: Seq[String], component: String): Unit = {
    val duplicated = names.groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }.toSeq.sorted
    if (duplicated.nonEmpty) {
      throw new PuddingException(s"duplicated $component name: ${duplicated.mkString(", ")}")
    }
  }

  private def available(values: Set[String]): String = {
    if (values.isEmpty) "none" else values.toSeq.sorted.mkString(", ")
  }

  private def validateSources(sourceCfs: Seq[SourceConf]): Unit = {
    sourceCfs.zipWithIndex.foreach { case (sourceConf, index) =>
      if (sourceConf == null) {
        throw new PuddingException(s"source[$index] can not be null!")
      }
      this.requireText(sourceConf.id, s"source[$index].id")
      this.requireText(sourceConf.`type`, s"source '${sourceConf.id}'.type")
    }
  }

  private def validateTransforms(transformCfs: Seq[TransformConf]): Unit = {
    transformCfs.zipWithIndex.foreach { case (transformConf, index) =>
      if (transformConf == null) {
        throw new PuddingException(s"transform[$index] can not be null!")
      }

      this.requireText(transformConf.id, s"transform[$index].id")
      this.requireText(transformConf.`type`, s"transform '${transformConf.id}'.type")

      if (transformConf.dependencies == null || transformConf.dependencies.isEmpty) {
        throw new PuddingException(
          s"transform '${transformConf.id}' must have at least one dependency!")
      }
      transformConf.dependencies.foreach { dependency =>
        this.requireText(dependency, s"transform '${transformConf.id}' dependency")
      }

      val duplicateDependencies = transformConf.dependencies
        .groupBy(identity)
        .collect { case (dependency, values) if values.size > 1 => dependency }
        .toSeq
        .sorted
      if (duplicateDependencies.nonEmpty) {
        throw new PuddingException(
          s"transform '${transformConf.id}' has duplicated dependencies: " +
            duplicateDependencies.mkString(", "))
      }
    }
  }

  private def validateSinks(sinkCfs: Seq[SinkConf]): Unit = {
    sinkCfs.zipWithIndex.foreach { case (sinkConf, index) =>
      if (sinkConf == null) {
        throw new PuddingException(s"sink[$index] can not be null!")
      }
      this.requireText(sinkConf.id, s"sink[$index].id")
      this.requireText(sinkConf.`type`, s"sink '${sinkConf.id}'.type")
      this.requireText(sinkConf.dependency, s"sink '${sinkConf.id}'.dependency")
    }
  }

  private def validateUniqueIds(
      sourceCfs: Seq[SourceConf],
      transformCfs: Seq[TransformConf],
      sinkCfs: Seq[SinkConf]): Unit = {
    val ids = sourceCfs.map(_.id) ++ transformCfs.map(_.id) ++ sinkCfs.map(_.id)
    val duplicatedIds = ids
      .groupBy(identity)
      .collect { case (id, values) if values.size > 1 => id }
      .toSeq
      .sorted

    if (duplicatedIds.nonEmpty) {
      throw new PuddingException(s"duplicated component id: ${duplicatedIds.mkString(", ")}")
    }
  }

  private def validateConnectorTypes(
      sourceCfs: Seq[SourceConf],
      transformCfs: Seq[TransformConf],
      sinkCfs: Seq[SinkConf]): Unit = {
    this.validateConnectorType("source", sourceCfs.map(_.`type`), DataSource.identifiers)
    this.validateConnectorType("transform", transformCfs.map(_.`type`), DataTransform.identifiers)
    this.validateConnectorType("sink", sinkCfs.map(_.`type`), DataSink.identifiers)
  }

  private def validateConnectorType(
      component: String,
      configuredTypes: Seq[String],
      availableTypes: Set[String]): Unit = {
    val missingTypes = configuredTypes.filterNot(availableTypes.contains).distinct.sorted
    if (missingTypes.nonEmpty) {
      throw new PuddingException(
        s"unknown $component type: ${missingTypes.mkString(", ")}, " +
          s"available: ${this.available(availableTypes)}")
    }
  }

  private def validateTransformDependencies(
      transformCfs: Seq[TransformConf],
      producerIds: Set[String]): Unit = {
    transformCfs.foreach { transformConf =>
      val missingDependencies = transformConf.dependencies
        .filterNot(producerIds.contains)
        .distinct
      if (missingDependencies.nonEmpty) {
        throw new PuddingException(
          s"transform '${transformConf.id}' has unknown dependencies: " +
            missingDependencies.mkString(", "))
      }
    }
  }

  private def validateSinkDependencies(sinkCfs: Seq[SinkConf], producerIds: Set[String]): Unit = {
    sinkCfs.foreach { sinkConf =>
      if (!producerIds.contains(sinkConf.dependency)) {
        throw new PuddingException(
          s"sink '${sinkConf.id}' depends on unknown component: ${sinkConf.dependency}")
      }
    }
  }

  /**
   * Order transforms so that every transform runs after its dependencies. Sources are the initial
   * completed set; if no transform can run while some remain, the dependencies form a cycle.
   */
  private def orderTransforms(
      transformCfs: Seq[TransformConf],
      sourceIds: Set[String]): Seq[TransformConf] = {
    val pending = mutable.LinkedHashMap[String, TransformConf]()
    transformCfs.foreach(transformConf => pending += transformConf.id -> transformConf)

    val completed = mutable.Set[String]()
    completed ++= sourceIds

    val ordered = mutable.ListBuffer[TransformConf]()
    while (pending.nonEmpty) {
      val ready = pending.values.filter { transformConf =>
        transformConf.dependencies.forall(completed.contains)
      }.toList

      if (ready.isEmpty) {
        throw new PuddingException(
          s"transform dependencies contain a cycle: ${pending.keys.mkString(", ")}")
      }

      ready.foreach { transformConf =>
        ordered += transformConf
        completed += transformConf.id
        pending -= transformConf.id
      }
    }

    ordered.toSeq
  }

  private def requireText(value: String, field: String): Unit = {
    if (value == null || value.trim.isEmpty) {
      throw new PuddingException(s"$field can not be empty!")
    }
  }
}
