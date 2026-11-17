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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import org.pudding.core.PuddingException
import org.pudding.core.catalog.{CatalogManager, ResolvedCatalog}
import org.pudding.core.configure.{
  ExecutionMode,
  JobMode,
  JobPipelineConf,
  SinkConf,
  SourceConf,
  SyncStrategy,
  TransformConf
}
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection, SecretResolver}
import org.pudding.core.definition._
import org.pudding.core.incremental.{
  IncrementalAudit,
  IncrementalMetrics,
  IncrementalPendingTransaction,
  IncrementalPlanDescriptor,
  IncrementalStateStore,
  IncrementalTransaction
}
import org.pudding.core.util.{ConfigRedactor, StreamingStateContract}

/**
 * Core classes for choreographing Spark Jobs
 */
object BuildJob extends Logging {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val SOURCE_COMPONENT = "source"

  private val TRANSFORM_COMPONENT = "transform"

  private val SINK_COMPONENT = "sink"

  private case class IncrementalExecution(
      transactionId: String,
      replay: Boolean,
      plan: IncrementalPlanDescriptor)

  private case class IncrementalRuntime(
      plan: Option[IncrementalPlan],
      stateStore: Option[IncrementalStateStore],
      executions: mutable.Map[String, IncrementalExecution],
      reads: mutable.Map[String, IncrementalRead])

  def buildSparkJob(jobPipelineConf: JobPipelineConf): Unit = {
    val plan = PipelineValidator.validate(jobPipelineConf)
    val jobConf = jobPipelineConf.job
    val stateContract = this.streamingStateContract(jobPipelineConf, plan)
    logInfo(s"jobConf is ${ConfigRedactor.redactToJson(jobConf)}")
    logInfo(
      s"validated pipeline: ${jobPipelineConf.sources.size} source(s), " +
        s"${plan.orderedTransforms.size} transform(s), ${jobPipelineConf.sinks.size} sink(s)")
    logInfo(
      s"job mode: ${plan.jobMode.name}, " +
        s"strategy: ${plan.syncStrategy.map(_.name).getOrElse("none")}, " +
        s"execution: ${plan.executionMode.name}")
    if (plan.incremental.nonEmpty) {
      IncrementalMetrics.registerJmx()
      IncrementalMetrics.increment(IncrementalMetrics.RunStarted)
    }

    var sparkSession: SparkSession = null
    var stateStore: Option[IncrementalStateStore] = None
    val incrementalExecutions = mutable.Map[String, IncrementalExecution]()
    try {
      logInfo(s"connectionCfs is ${ConfigRedactor.redactToJson(jobPipelineConf.connections)}")
      logInfo(s"catalogCfs is ${ConfigRedactor.redactToJson(jobPipelineConf.catalogs)}")

      val connections = ConnectionRegistry(jobPipelineConf.connections)
      // Resolve every referenced connection before Spark starts, so an invalid endpoint or
      // missing secret does not allocate a session first. Catalog resolution does the same for
      // connections referenced by catalogs.
      this.preflightConnections(jobPipelineConf, connections)
      val catalogs = CatalogManager.resolve(jobPipelineConf.catalogs, connections)
      val catalogsByName = catalogs.map(catalog => catalog.name -> catalog).toMap

      // Initial SparkSession
      logInfo("Initial SparkSession")
      val resolvedJobConf = jobConf.copy(
        config = jobConf.config.map(_.map { case (key, value) =>
          key -> SecretResolver.resolve(value)
        }))
      sparkSession = SessionHandler().sparkSessionBuilder(resolvedJobConf, catalogs)

      // Per-run state, so concurrent or sequential jobs never share dataframes
      val idDf = mutable.Map[String, DataFrame]()
      val incrementalReads = mutable.Map[String, IncrementalRead]()
      stateStore = plan.incremental.map { value =>
        IncrementalStateStore.open(
          value.spec, sparkSession.sparkContext.hadoopConfiguration)
      }
      plan.incremental.foreach { incremental =>
        stateStore.foreach(_.acquire(incremental.sourceId))
        logInfo(s"acquired incremental state lock for source '${incremental.sourceId}'")
      }
      val incrementalRuntime = IncrementalRuntime(
        plan.incremental, stateStore, incrementalExecutions, incrementalReads)

      // Initial data source
      logInfo("Initial data source.")
      this.initSource(
        jobPipelineConf.sources, sparkSession, idDf, connections, catalogsByName,
        incrementalRuntime, jobPipelineConf.sinks, plan.orderedTransforms, plan.executionMode,
        plan.streamingTrigger)

      // Initial transform, if transformCfs is none, do not process transform
      logInfo("Initial transform.")
      val incrementalTransform = plan.incremental.flatMap { incremental =>
        incrementalReads.get(incremental.sourceId).map { read =>
          IncrementalTransformContext(
            read.semantics,
            read.keyFields,
            read.opColumn,
            read.orderFields)
        }
      }
      this.initTransform(
        jobPipelineConf.transforms, plan.orderedTransforms, idDf,
        TransformContext(incrementalTransform))

      // Write sink
      logInfo("Write sink...")
      this.writeSink(
        jobPipelineConf.sinks, idDf, connections, catalogsByName,
        incrementalRuntime, plan.jobMode, plan.syncStrategy, plan.executionMode,
        plan.streamingTrigger, stateContract)

      plan.incremental.foreach { incremental =>
        stateStore.foreach { store =>
          incrementalExecutions.get(incremental.sourceId).foreach { execution =>
            val state = store.commit(incremental.sourceId, execution.transactionId)
            logInfo(
              s"committed incremental transaction '${execution.transactionId}' for source " +
                s"'${incremental.sourceId}' at cursor '${state.committedCursor.getOrElse("")}'")
          }
        }
      }

      logInfo("job completed successfully")
      if (plan.incremental.nonEmpty) {
        IncrementalMetrics.increment(IncrementalMetrics.RunCompleted)
        IncrementalAudit.runCompleted(IncrementalMetrics.snapshot())
      }
    } catch {
      case NonFatal(error) =>
        if (plan.incremental.nonEmpty) {
          IncrementalMetrics.increment(IncrementalMetrics.RunFailed)
        }
        plan.incremental.foreach { incremental =>
          stateStore.foreach { store =>
            try {
              val detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
              store.recordFailure(incremental.sourceId, ConfigRedactor.redactLine(detail))
            } catch {
              case NonFatal(recordError) =>
                logWarning(
                  s"failed to record incremental state failure for source " +
                    s"'${incremental.sourceId}': " +
                    ConfigRedactor.redactLine(recordError.getMessage))
            }
          }
        }
        throw error
    } finally {
      plan.incremental.foreach { incremental =>
        stateStore.foreach { store =>
          try {
            if (!store.release(incremental.sourceId)) {
              logWarning(
                s"incremental state lock for source '${incremental.sourceId}' could not be " +
                  "released; unlock it with 'pudding state unlock' before the next run")
            }
          } catch {
            case NonFatal(error) =>
              logWarning(
                s"failed to release incremental state lock for source " +
                  s"'${incremental.sourceId}': ${error.getMessage}")
          }
        }
      }
      stateStore.foreach { store =>
        try {
          store.close()
        } catch {
          case NonFatal(error) =>
            logWarning(s"failed to close incremental state store: ${error.getMessage}")
        }
      }
      this.closeSparkSession(sparkSession)
    }
  }

  /**
   * Fingerprint the stateful streaming pipeline before Spark starts. The fingerprint is only
   * produced when at least one transform maintains state, so stateless streaming jobs can still
   * change filters and projections without invalidating their checkpoint.
   */
  private def streamingStateContract(
      jobPipelineConf: JobPipelineConf,
      plan: PipelinePlan): Option[String] = {
    if (plan.executionMode != ExecutionMode.Streaming) {
      None
    } else {
      val transforms = DataTransform.services()
      val stateful = plan.orderedTransforms.exists { transformConf =>
        transforms.get(transformConf.`type`).exists(_.streamingStateful)
      }
      if (!stateful) {
        None
      } else {
        val source = jobPipelineConf.sources.headOption.getOrElse {
          throw new PuddingException("stateful streaming requires a source")
        }
        val contract = StreamingStateContract.fingerprint(
          source, plan.orderedTransforms)
        logInfo(s"streaming state contract: $contract")
        Some(contract)
      }
    }
  }

  private def initSource(
      sourceCfs: Seq[SourceConf],
      sparkSession: SparkSession,
      idDf: mutable.Map[String, DataFrame],
      connections: ConnectionRegistry,
      catalogs: Map[String, ResolvedCatalog],
      incremental: IncrementalRuntime,
      sinkCfs: Seq[SinkConf],
      transformCfs: Seq[TransformConf],
      executionMode: ExecutionMode,
      streamingTrigger: Option[String]): Unit = {
    logInfo(s"sourceCfs is ${ConfigRedactor.redactToJson(sourceCfs)}")

    val dataSources = DataSource.services()
    sourceCfs.foreach { sourceConf =>
      val dataSource = dataSources.getOrElse(sourceConf.`type`,
        throw new PuddingException(s"can not get DataSource implement for type: ${sourceConf.`type`}"))

      val (dataFrame, incrementalRead) =
        this.withComponentContext(SOURCE_COMPONENT, sourceConf.id, sourceConf.`type`) {
          val context = SourceContext(
            config = SecretResolver.resolveConfig(sourceConf.config),
            connection = sourceConf.connection.map(name => this.resolveConnection(name, connections)),
            catalog = sourceConf.catalog.map(name => this.resolveCatalog(name, catalogs)),
            executionMode = Some(executionMode),
            trigger = streamingTrigger)
          incremental.plan.filter(_.sourceId == sourceConf.id) match {
            case Some(plan) =>
              val source = dataSource match {
                case value: IncrementalSource => value
                case _ =>
                  throw new PuddingException(
                    s"source '${sourceConf.id}' does not implement IncrementalSource")
              }
              val store = incremental.stateStore.getOrElse {
                throw new PuddingException(
                  s"incremental source '${sourceConf.id}' requires incremental state")
              }
              val sinkConf = sinkCfs match {
                case Seq(value) => value
                case _ =>
                  throw new PuddingException(
                    s"incremental source '${sourceConf.id}' requires exactly one sink")
              }
              val descriptor = IncrementalPlanDescriptor.withContract(
                IncrementalPlanDescriptor.create(
                  sourceType = sourceConf.`type`,
                  sourceId = sourceConf.id,
                  sinkType = sinkConf.`type`,
                  sinkId = sinkConf.id,
                  semantics = plan.spec.semantics,
                  keyFields = plan.spec.keyFields,
                  opColumn = plan.spec.opColumn,
                  sourceConfig = this.incrementalSourceConfig(sourceConf),
                  sinkConfig = this.incrementalSinkConfig(sinkConf),
                  stateBackend = IncrementalStateStore.backendName(plan.spec)),
                this.incrementalContract(
                  sourceConf, sinkConf, transformCfs, connections, catalogs))
              val state = store.load(sourceConf.id)
              val pending = state.pending
              pending.foreach { transaction =>
                transaction.plan match {
                  case Some(stored)
                      if stored.copy(orderFields = Seq.empty).fingerprint ==
                        descriptor.fingerprint =>
                  case Some(_) =>
                    throw new PuddingException(
                      s"source '${sourceConf.id}' pending transaction " +
                        s"'${transaction.transactionId}' was created for a different source, " +
                        "sink or configuration; inspect it with the state tool and resolve it " +
                        "with --commit or --abort before changing the job")
                  case None =>
                    throw new PuddingException(
                      s"source '${sourceConf.id}' pending transaction " +
                        s"'${transaction.transactionId}' has no plan fingerprint; inspect it " +
                        "with the state tool and resolve it with --commit or --abort")
                }
              }
              if (pending.nonEmpty && !this.canReplay(plan.spec, sinkCfs)) {
                throw this.pendingTransaction(sourceConf.id, plan, pending.get, store.location)
              }
              pending.foreach { transaction =>
                logWarning(
                  s"replaying uncommitted incremental transaction " +
                    s"'${transaction.transactionId}' for source '${sourceConf.id}'")
              }
              if (pending.isEmpty && state.committedCursor.isEmpty &&
                plan.spec.start.isEmpty && plan.spec.initial == "none") {
                throw new PuddingException(
                  s"source '${sourceConf.id}' incremental read requires start or " +
                    "initial=latest/full for the first run; incremental state only stores " +
                    "progress and can not bootstrap a read")
              }
              val request = pending match {
                case Some(transaction) =>
                  // Read exactly the range recorded by the pending transaction. `initial=full`
                  // is only used when the original range started from a full snapshot.
                  IncrementalRequest(
                    semantics = plan.spec.semantics,
                    keyFields = plan.spec.keyFields,
                    opColumn = plan.spec.opColumn,
                    start = transaction.fromCursor,
                    end = Some(transaction.toCursor),
                    initial = if (transaction.fromCursor.isEmpty) "full" else "none")
                case None =>
                  IncrementalRequest(
                    semantics = plan.spec.semantics,
                    keyFields = plan.spec.keyFields,
                    opColumn = plan.spec.opColumn,
                    // A committed cursor always wins; `start` is only the bootstrap position.
                    start = state.committedCursor.orElse(plan.spec.start),
                    end = plan.spec.end,
                    initial = plan.spec.initial)
              }
              val read = source.readIncremental(sparkSession, context, request)
              val executionPlan = descriptor.copy(orderFields = read.orderFields)
              pending.foreach { transaction =>
                if (read.fromCursor != transaction.fromCursor ||
                  read.toCursor != transaction.toCursor) {
                  throw new PuddingException(
                    s"source '${sourceConf.id}' returned cursor range " +
                      s"'${read.fromCursor.getOrElse("")}'..'${read.toCursor}', but pending " +
                      s"transaction '${transaction.transactionId}' expects " +
                      s"'${transaction.fromCursor.getOrElse("")}'..'${transaction.toCursor}'")
                }
                if (!transaction.plan.exists(_.fingerprint == executionPlan.fingerprint)) {
                  throw new PuddingException(
                    s"source '${sourceConf.id}' returned ordering fields " +
                      s"'${read.orderFields.mkString(",")}' that do not match pending " +
                      s"transaction '${transaction.transactionId}'")
                }
              }
              val transactionId = pending.map(_.transactionId).getOrElse(
                IncrementalTransaction.id(
                  sourceConf.id,
                  read.fromCursor,
                  read.toCursor,
                  read.semantics,
                  read.keyFields,
                  read.opColumn,
                  read.orderFields,
                  Some(executionPlan)))
              incremental.executions += sourceConf.id ->
                IncrementalExecution(transactionId, replay = pending.nonEmpty, executionPlan)
              (read.dataFrame, Some(read))
            case None =>
              (dataSource.initData(sparkSession, context), None)
          }
        }
      idDf += sourceConf.id -> dataFrame
      incrementalRead.foreach(read => incremental.reads += sourceConf.id -> read)
    }
  }

  private def initTransform(
      transformCfsOption: Option[Seq[TransformConf]],
      orderedTransforms: Seq[TransformConf],
      idDf: mutable.Map[String, DataFrame],
      context: TransformContext): Unit = {
    transformCfsOption match {
      case Some(transformCfs) =>
        logInfo(s"transformCfs is ${ConfigRedactor.redactToJson(transformCfs)}")
        if (transformCfs.isEmpty) {
          logInfo("transform configure is empty")
        } else {
          val transforms = DataTransform(transformCfs)

          // Traverse transforms in topological order, not configuration order
          orderedTransforms.foreach { transformConf =>
            val transformIdDf = transformConf.dependencies.map { id =>
              id -> idDf.getOrElse(id, throw new PuddingException(
                s"transform '${transformConf.id}' can not get dataframe from dependency '$id'"))
            }.toMap

            val (transform, config) = transforms.getOrElse(
              transformConf.id,
              throw new PuddingException(s"can not get transform for id: ${transformConf.id}"))

            val dataFrame = this.withComponentContext(
              TRANSFORM_COMPONENT, transformConf.id, transformConf.`type`) {
              val output = transform.handle(transformIdDf, config, context)
              if (transform.preservesIncrementalMetadata) {
                context.incremental.foreach { metadata =>
                  this.validateIncrementalMetadata(output, metadata)
                }
              }
              output
            }
            idDf += transformConf.id -> dataFrame
          }
        }
      case None =>
        logInfo("transform configure non-existent")
    }
  }

  /**
   * Connector configuration is part of the plan fingerprint. The incremental block is excluded:
   * recovery, initial and state selection are operational settings and do not change the data
   * contract of a pending batch.
   */
  private def incrementalSourceConfig(sourceConf: SourceConf): Option[Map[String, Any]] = {
    this.incrementalConfig(
      sourceConf.config.map(_.filterNot { case (key, _) => key == "incremental" }),
      sourceConf.connection,
      sourceConf.catalog)
  }

  private def incrementalSinkConfig(sinkConf: SinkConf): Option[Map[String, Any]] = {
    this.incrementalConfig(sinkConf.config, sinkConf.connection, sinkConf.catalog)
  }

  /**
   * Hash the rest of the data contract that is not part of the connector config: the transform
   * chain and resolved connection/catalog settings. Only the redacted JSON is hashed; secrets
   * are never stored in the state journal.
   */
  private def incrementalContract(
      sourceConf: SourceConf,
      sinkConf: SinkConf,
      transformCfs: Seq[TransformConf],
      connections: ConnectionRegistry,
      catalogs: Map[String, ResolvedCatalog]): Option[Map[String, Any]] = {
    val hasResources = sourceConf.connection.nonEmpty || sourceConf.catalog.nonEmpty ||
      sinkConf.connection.nonEmpty || sinkConf.catalog.nonEmpty
    if (transformCfs.isEmpty && !hasResources) {
      // Preserve the fingerprints of existing jobs that predate the extended contract.
      None
    } else {
      Some(Map[String, Any](
        "transforms" -> transformCfs.map { transform =>
          Map[String, Any](
            "id" -> transform.id,
            "type" -> transform.`type`,
            "dependencies" -> transform.dependencies,
            "config" -> transform.config.getOrElse(Map.empty))
        },
        "source" -> this.incrementalResourceContract(
          sourceConf.connection, sourceConf.catalog, connections, catalogs),
        "sink" -> this.incrementalResourceContract(
          sinkConf.connection, sinkConf.catalog, connections, catalogs)))
    }
  }

  private def incrementalResourceContract(
      connection: Option[String],
      catalog: Option[String],
      connections: ConnectionRegistry,
      catalogs: Map[String, ResolvedCatalog]): Map[String, Any] = {
    val connectionContract = connection.flatMap(connections.get).map { resolved =>
      Map[String, Any](
        "name" -> resolved.name,
        "type" -> resolved.`type`,
        "endpoint" -> resolved.endpoint,
        "properties" -> resolved.properties,
        "options" -> resolved.options)
    }
    val catalogContract = catalog.flatMap(catalogs.get).map { resolved =>
      Map[String, Any](
        "name" -> resolved.name,
        "type" -> resolved.`type`,
        "sparkCatalogName" -> resolved.sparkCatalogName,
        "sessionCatalog" -> resolved.sessionCatalog,
        "sparkConf" -> resolved.sparkConf,
        "properties" -> resolved.properties)
    }
    Map[String, Any](
      "connection" -> connectionContract,
      "catalog" -> catalogContract)
  }

  /**
   * Add the referenced connection and catalog names to the hashed configuration. Their names
   * identify where a batch is read from and written to; a pending transaction must not replay
   * against a different connection that happens to share the same connector options.
   */
  private def incrementalConfig(
      config: Option[Map[String, Any]],
      connection: Option[String],
      catalog: Option[String]): Option[Map[String, Any]] = {
    val entries = connection.map(value => "_pudding.connection" -> value).toSeq ++
      catalog.map(value => "_pudding.catalog" -> value).toSeq
    config.map(_ ++ entries).orElse {
      if (entries.isEmpty) None else Some(entries.toMap)
    }
  }

  /**
   * Decide whether a pending transaction may be replayed automatically.
   */
  private def canReplay(spec: IncrementalSpec, sinkCfs: Seq[SinkConf]): Boolean = {
    spec.recovery match {
      case IncrementalRecoveryPolicy.Fail => false
      case IncrementalRecoveryPolicy.Retry => true
      case IncrementalRecoveryPolicy.Auto =>
        sinkCfs match {
          case Seq(sinkConf) =>
            DataSink.services().get(sinkConf.`type`)
              .exists(_.isIdempotentIncremental(spec.semantics))
          case _ => false
        }
    }
  }

  private def pendingTransaction(
      sourceId: String,
      incremental: IncrementalPlan,
      transaction: IncrementalPendingTransaction,
      location: String): PuddingException = {
    new PuddingException(
      s"source '$sourceId' has an uncommitted incremental transaction " +
        s"'${transaction.transactionId}' from " +
        s"'${transaction.fromCursor.getOrElse("<initial>")}' to '${transaction.toCursor}' " +
        s"(started ${transaction.startedAt}) in state '$location'; the sink may already " +
        s"contain this batch. Inspect it with 'pudding state show <state-target> $sourceId'; " +
        s"if the previous process crashed, remove the stale lock with " +
        s"'pudding state unlock <state-target> $sourceId'. Then use " +
        s"'pudding state resolve <state-target> $sourceId --commit' if the sink write " +
        s"succeeded, or '--abort' to replay it. Set incremental.recovery=retry only when " +
        s"the sink is idempotent for ${incremental.spec.semantics.name} semantics.")
  }

  private def writeSink(
      sinkCfs: Seq[SinkConf],
      idDf: mutable.Map[String, DataFrame],
      connections: ConnectionRegistry,
      catalogs: Map[String, ResolvedCatalog],
      incremental: IncrementalRuntime,
      jobMode: JobMode,
      syncStrategy: Option[SyncStrategy],
      executionMode: ExecutionMode,
      streamingTrigger: Option[String],
      stateContract: Option[String]): Unit = {
    logInfo(s"sinkCfs is ${ConfigRedactor.redactToJson(sinkCfs)}")

    val dataSinks = DataSink.services()
    for (sinkConf <- sinkCfs) {
      val dataFrame = idDf.getOrElse(
        sinkConf.dependency,
        throw new PuddingException(
          s"sink '${sinkConf.id}' can not get dataframe from dependency '${sinkConf.dependency}'"))

      val dataSink = dataSinks.getOrElse(sinkConf.`type`,
        throw new PuddingException(s"can not get DataSink implement for type: ${sinkConf.`type`}"))

      // Execute every sink once per run. A pending transaction is replayed only through the
      // explicit incremental.recovery policy, which is safe only for idempotent semantics.
      this.withComponentContext(SINK_COMPONENT, sinkConf.id, sinkConf.`type`) {
        val resolvedConfig = SecretResolver.resolveConfig(sinkConf.config)
        val fullRefresh =
          jobMode == JobMode.Sync && syncStrategy.contains(SyncStrategy.Full)
        val config = if (fullRefresh) {
          // sync + full is a complete refresh. Force both common save-mode keys so every
          // full-refresh-capable sink receives overwrite without sink-specific runtime logic.
          Some(resolvedConfig.getOrElse(Map.empty[String, Any]) ++ Map(
            "mode" -> "overwrite",
            "saveMode" -> "overwrite"))
        } else {
          resolvedConfig
        }
        val context = SinkContext(
          config = config,
          connection = sinkConf.connection.map(name => this.resolveConnection(name, connections)),
          catalog = sinkConf.catalog.map(name => this.resolveCatalog(name, catalogs)),
          executionMode = Some(executionMode),
          trigger = streamingTrigger,
          stateContract = stateContract)
        incremental.plan match {
          case Some(plan) =>
            val read = incremental.reads.getOrElse(plan.sourceId,
              throw new PuddingException(
                s"incremental result is missing for source '${plan.sourceId}'"))
            val execution = incremental.executions.getOrElse(plan.sourceId,
              throw new PuddingException(
                s"incremental transaction is missing for source '${plan.sourceId}'"))
            val store = incremental.stateStore.getOrElse(
              throw new PuddingException(
                s"incremental state is missing for source '${plan.sourceId}'"))
            if (execution.replay) {
              store.ensureLock(plan.sourceId)
              val replayed = store.recordAttempt(plan.sourceId, execution.transactionId)
              IncrementalMetrics.increment(IncrementalMetrics.TransactionReplay)
              IncrementalAudit.transactionReplay(
                plan.sourceId,
                execution.transactionId,
                replayed.attempts)
              val pending = store.load(plan.sourceId).pending
              if (!pending.exists(_.transactionId == execution.transactionId)) {
                throw new PuddingException(
                  s"pending incremental transaction '${execution.transactionId}' for source " +
                    s"'${plan.sourceId}' is no longer present")
              }
            } else {
              store.begin(
                plan.sourceId,
                read.fromCursor,
                read.toCursor,
                execution.transactionId,
                execution.plan)
            }
            store.ensureLock(plan.sourceId)
            val fencingToken = store.activeFencingToken(plan.sourceId)
              .orElse(store.lockInfo(plan.sourceId).map(_.fencingToken))
              .getOrElse {
                throw new PuddingException(
                  s"incremental state lock for source '${plan.sourceId}' is no longer owned")
              }
            val write = IncrementalWrite(
              semantics = read.semantics,
              keyFields = read.keyFields,
              opColumn = read.opColumn,
              fromCursor = read.fromCursor,
              toCursor = read.toCursor,
              cursorType = read.cursorType,
              orderFields = read.orderFields,
              transactionId = execution.transactionId,
              sourceId = plan.sourceId,
              fencingToken = fencingToken)
            this.validateIncrementalFrame(dataFrame, write)
            dataSink.writeIncremental(dataFrame, context, write)
            // Surface a lease loss before the cursor commit and before the next sink writes.
            store.ensureLock(plan.sourceId)
          case None =>
            if (fullRefresh) {
              dataSink.writeFullRefresh(dataFrame, context)
            } else {
              dataSink.writer(dataFrame, context)
            }
        }
      }
    }
  }

  private def validateIncrementalFrame(dataFrame: DataFrame, write: IncrementalWrite): Unit = {
    this.validateIncrementalMetadata(
      dataFrame,
      IncrementalTransformContext(
        write.semantics,
        write.keyFields,
        write.opColumn,
        write.orderFields))
  }

  private def validateIncrementalMetadata(
      dataFrame: DataFrame,
      metadata: IncrementalTransformContext): Unit = {
    val fields = dataFrame.columns.toSet
    if (metadata.semantics == IncrementalSemantics.Upsert ||
      metadata.semantics == IncrementalSemantics.Cdc) {
      val missingKeys = metadata.keyFields.filterNot(fields.contains)
      if (missingKeys.nonEmpty) {
        throw new PuddingException(
          s"incremental ${metadata.semantics.name} result is missing key fields: " +
            missingKeys.mkString(", "))
      }
    }
    if (metadata.semantics == IncrementalSemantics.Cdc && !fields.contains(metadata.opColumn)) {
      throw new PuddingException(
        s"incremental cdc result is missing operation column: ${metadata.opColumn}")
    }
    val missingOrder = metadata.orderFields.filterNot(fields.contains)
    if (missingOrder.nonEmpty) {
      throw new PuddingException(
        "incremental result is missing change ordering columns: " +
          missingOrder.mkString(", "))
    }
  }

  private def preflightConnections(
      jobPipelineConf: JobPipelineConf,
      connections: ConnectionRegistry): Unit = {
    val sourceConnections = Option(jobPipelineConf.sources).getOrElse(Seq.empty)
      .flatMap(source => Option(source).flatMap(_.connection))
    val sinkConnections = Option(jobPipelineConf.sinks).getOrElse(Seq.empty)
      .flatMap(sink => Option(sink).flatMap(_.connection))
    (sourceConnections ++ sinkConnections).distinct.foreach(connections.get)
  }

  private def resolveConnection(
      name: String,
      connections: ConnectionRegistry): ResolvedConnection = {
    connections.get(name).getOrElse {
      throw new PuddingException(s"can not resolve connection: $name")
    }
  }

  private def resolveCatalog(
      name: String,
      catalogs: Map[String, ResolvedCatalog]): ResolvedCatalog = {
    catalogs.getOrElse(name, throw new PuddingException(s"can not resolve catalog: $name"))
  }

  /**
   * Add component information to an error without including its configuration.
   */
  private def withComponentContext[T](
      component: String,
      id: String,
      componentType: String)(body: => T): T = {
    try {
      body
    } catch {
      case NonFatal(error) =>
        val detail = ConfigRedactor.redactLine(
          Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName))
        throw new PuddingException(
          s"$component '$id' (type '$componentType') failed: $detail", error)
    }
  }

  private def closeSparkSession(sparkSession: SparkSession): Unit = {
    if (sparkSession != null) {
      logInfo("close sparkSession")
      try {
        sparkSession.close()
      } catch {
        case NonFatal(error) =>
          logWarning(s"failed to close sparkSession: ${error.getMessage}")
      }
    }
  }
}
