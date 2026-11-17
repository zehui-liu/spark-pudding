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

package org.pudding.sink

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}
import org.apache.spark.sql.functions.col

import org.pudding.core.PuddingException
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{DataSink, IncrementalSemantics, IncrementalWrite, SinkContext}
import org.pudding.core.incremental.{
  IncrementalAudit,
  IncrementalFrames,
  IncrementalMetrics,
  StreamingCdc
}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Writes Hudi tables.
 *
 * Path mode uses the Hudi data source and supports COPY_ON_WRITE / MERGE_ON_READ tables, insert,
 * upsert, bulk insert, delete, insert overwrite, Hive sync and arbitrary Hudi write options.
 * Catalog mode writes a table through a Hudi catalog (`saveAsTable`) and is the preferred way to
 * create a table registered in the Spark session catalog.
 */
class HudiSink extends DataSink {

  private val TABLE_KEY = "table"

  private val TABLE_PATH_KEY = "tablePath"

  private val HUDI_TABLE_NAME_KEY = "hudiTableName"

  private val OPERATION_KEY = "operation"

  private val TABLE_TYPE_KEY = "tableType"

  private val RECORD_KEY_FIELDS_KEY = "recordKeyFields"

  private val PARTITION_PATH_FIELD_KEY = "partitionPathField"

  private val PARTITION_BY_KEY = "partitionBy"

  private val PRECOMBINE_FIELD_KEY = "preCombineField"

  private val HIVE_SYNC_ENABLED_KEY = "hiveSyncEnabled"

  private val HIVE_SYNC_MODE_KEY = "hiveSyncMode"

  private val HIVE_DATABASE_KEY = "hiveDatabase"

  private val HIVE_TABLE_KEY = "hiveTableName"

  private val HIVE_METASTORE_URIS_KEY = "hiveMetastoreUris"

  private val OPTIONS_KEY = "options"

  private val MODE_KEY = "mode"

  private val IS_STREAMING_KEY = "isStreaming"

  private val STREAM_WRITE_KEY = "streamWrite"

  private val STREAM_WRITER_KEY = "streamWriter"

  private val CHECKPOINT_LOCATION_KEY = "checkpointLocation"

  private val HIVE_SYNC_ENABLED_OPTION = "hoodie.datasource.hive_sync.enable"

  private val HIVE_SYNC_MODE_OPTION = "hoodie.datasource.hive_sync.mode"

  private val HIVE_SYNC_DATABASE_OPTION = "hoodie.datasource.hive_sync.database"

  private val HIVE_SYNC_TABLE_OPTION = "hoodie.datasource.hive_sync.table"

  private val HIVE_SYNC_METASTORE_OPTION = "hoodie.datasource.hive_sync.metastore.uris"

  /**
   * Hudi copies options beginning with the commit metadata prefix into the commit metadata. The
   * default prefix is `_`; a custom prefix is rejected for append transactions because the
   * replay marker would be lost.
   */
  private val COMMIT_METADATA_PREFIX_KEY =
    "hoodie.datasource.write.commitmeta.key.prefix"

  private val TRANSACTION_METADATA_KEY = "_pudding.transaction.id"

  private val FENCING_METADATA_KEY = "_pudding.fencing.token"

  private val ArchivedCommitActions = Set(
    HoodieTimeline.COMMIT_ACTION,
    HoodieTimeline.DELTA_COMMIT_ACTION,
    HoodieTimeline.REPLACE_COMMIT_ACTION)

  private val SOURCE_METADATA_KEY = "_pudding.source.id"

  override def identifier: String = "hudi-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedStreamingOutputModes: Set[String] =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update)

  override def supportsStreamingUpsert: Boolean = true

  override def supportsFullRefresh: Boolean = true

  override def supportsAtomicReplace: Boolean = true

  override def supportsLakeIncremental: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("hudi")

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert, IncrementalSemantics.Cdc)

  /**
   * Upsert and CDC replace rows by record key. Append records its transaction id in Hudi commit
   * metadata and skips an already committed transaction, so all three semantics are safe to
   * replay when BuildJob supplies the transaction id.
   */
  override def isIdempotentIncremental(semantics: IncrementalSemantics): Boolean = true

  override def supportsIncrementalFencing: Boolean = true

  /**
   * Replace the whole target with one Hudi `insert_overwrite_table` replace commit. Hudi exposes
   * either the old or the new file slice to readers, so a full refresh does not require the
   * non-atomic opt-in. The operation is independent of the configured save mode.
   */
  override def writeFullRefresh(
      dataFrame: DataFrame,
      context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    var next = cfg ++ Map(
      OPERATION_KEY -> "insert_overwrite_table",
      MODE_KEY -> "append")
    // Spark's saveAsTable path does not translate the Hudi replace operation into a replace
    // commit for an existing catalog table. Resolve its base path and use the path writer, which
    // does. A missing catalog table falls back to saveAsTable and is created normally. Fail
    // closed if the catalog reports an existing table but its location can not be resolved,
    // because saveAsTable would append instead of replacing it.
    val targetPath = this.optionalString(cfg, TABLE_PATH_KEY)
      .orElse(if (context.catalog.nonEmpty) {
        if (dataFrame.sparkSession.catalog.tableExists(requiredString(cfg, TABLE_KEY))) {
          Some(this.tablePath(dataFrame, cfg).getOrElse {
            throw new PuddingException(
              "hudi full refresh could not resolve the existing catalog table location")
          })
        } else {
          None
        }
      } else {
        None
      })
    targetPath.foreach(path => next += TABLE_PATH_KEY -> path)
    this.writer(dataFrame, context.copy(config = Some(next)))
  }

  override def writeIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: IncrementalWrite): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    if (this.streamingEnabled(cfg, context.executionMode)) {
      throw new PuddingException("hudi-sink does not support streaming incremental writes")
    }
    val timeline = this.timelineState(dataFrame, cfg, incremental)
    // An equal fence is only valid for a replay of the same transaction, which already recorded
    // its marker. Any other equal fence belongs to a different writer that reused the generation.
    val stale = incremental.fencingToken > 0L &&
      (timeline.newestFencingToken > incremental.fencingToken ||
        (timeline.newestFencingToken == incremental.fencingToken &&
          !timeline.transactionCommitted))
    if (stale) {
      throw this.staleWriter(incremental, timeline.newestFencingToken)
    }
    if (incremental.fromCursor.nonEmpty && incremental.fencingToken > 0L &&
      timeline.newestFencingToken <= 0L) {
      logWarning(
        "hudi sink found no retained fencing metadata for incremental source " +
          s"'${incremental.sourceId}'; the fencing check can not see writers whose commits " +
          "were cleaned up. Keep the Hudi commit retention window longer than the maximum " +
          "delayed writer lifetime.")
    }
    incremental.semantics match {
      case IncrementalSemantics.Append =>
        if (timeline.transactionCommitted) {
          logInfo(
            s"hudi append transaction '${incremental.transactionId}' is already committed; " +
              "skipping the sink write")
        } else {
          this.writer(dataFrame, this.incrementalContext(context, cfg, "insert", incremental))
        }
      case IncrementalSemantics.Upsert =>
        this.writer(dataFrame, this.incrementalContext(context, cfg, "upsert", incremental))
      case IncrementalSemantics.Cdc =>
        val opColumn = incremental.opColumn
        if (!dataFrame.columns.contains(opColumn)) {
          throw new PuddingException(
            s"hudi cdc input is missing operation column: $opColumn")
        }
        val op = col(opColumn)
        val unknown = dataFrame.filter(op.isNull || !op.isin("I", "U", "D")).limit(1).count()
        if (unknown > 0) {
          throw new PuddingException(
            s"hudi cdc operation column '$opColumn' must contain I, U or D")
        }
        val orderFields = if (incremental.orderFields.nonEmpty) {
          incremental.orderFields
        } else {
          // A batch CDC source may not expose ordering metadata. Honor the sink's
          // cdcOrderFields before reporting that the batch is ambiguous.
          StreamingCdc.orderFields(cfg)
        }
        val ordered = IncrementalFrames.latestByKey(
          dataFrame, incremental.keyFields, orderFields)
        if (orderFields.isEmpty && incremental.keyFields.nonEmpty) {
          val duplicate = ordered
            .groupBy(incremental.keyFields.map(key => col(key)): _*)
            .count()
            .filter(col("count") > 1)
            .limit(1)
            .count() > 0
          if (duplicate) {
            throw new PuddingException(
              "hudi cdc input has multiple changes per key but no ordering metadata; " +
                "set cdcOrderFields or provide source ordering")
          }
        }
        val upserts = ordered.filter(op.isin("I", "U")).drop(opColumn)
        val deletes = ordered.filter(op === "D").drop(opColumn)
        if (deletes.limit(1).count() > 0) {
          this.writer(deletes, this.incrementalContext(context, cfg, "delete", incremental))
        }
        if (upserts.limit(1).count() > 0) {
          this.writer(upserts, this.incrementalContext(context, cfg, "upsert", incremental))
        }
    }
  }

  private def incrementalContext(
      context: SinkContext,
      cfg: Map[String, Any],
      operation: String,
      incremental: IncrementalWrite): SinkContext = {
    val keys = incremental.keyFields
    var next = cfg + (OPERATION_KEY -> operation) + (MODE_KEY -> "append")
    if (keys.nonEmpty) {
      next += RECORD_KEY_FIELDS_KEY -> keys
    }
    var metadata = Map.empty[String, String]
    if (incremental.transactionId.nonEmpty) {
      metadata += TRANSACTION_METADATA_KEY -> incremental.transactionId
    }
    if (incremental.sourceId.nonEmpty) {
      metadata += SOURCE_METADATA_KEY -> incremental.sourceId
    }
    if (incremental.fencingToken > 0L) {
      metadata += FENCING_METADATA_KEY -> incremental.fencingToken.toString
    }
    if (metadata.nonEmpty) {
      val options = this.getStringOptions(cfg, OPTIONS_KEY)
      val prefix = options.getOrElse(COMMIT_METADATA_PREFIX_KEY, "_")
      if (prefix != "_") {
        throw new PuddingException(
          s"hudi incremental fencing requires $COMMIT_METADATA_PREFIX_KEY=_," +
            s" but got '$prefix'")
      }
      next += OPTIONS_KEY -> (options ++ metadata)
    }
    context.copy(config = Some(next))
  }

  private case class TimelineState(
      transactionCommitted: Boolean,
      newestFencingToken: Long)

  /**
   * Inspect the Hudi active timeline once for the append transaction marker and for the newest
   * fencing token committed by the same source. A missing table or path is a first write.
   */
  private def timelineState(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      incremental: IncrementalWrite): TimelineState = {
    if ((incremental.transactionId == null || incremental.transactionId.isEmpty) &&
      (incremental.sourceId.isEmpty || incremental.fencingToken <= 0L)) {
      TimelineState(transactionCommitted = false, newestFencingToken = 0L)
    } else {
      this.tablePath(dataFrame, cfg).map { path =>
        val sparkSession = dataFrame.sparkSession
        val hadoopConf = sparkSession.sessionState.newHadoopConf()
        val fileSystem = new Path(path).getFileSystem(hadoopConf)
        if (!fileSystem.exists(new Path(path))) {
          TimelineState(transactionCommitted = false, newestFencingToken = 0L)
        } else {
          try {
            val meta = HoodieTableMetaClient.builder()
              .setConf(hadoopConf)
              .setBasePath(path)
              .build()
            val timeline = meta.getActiveTimeline
            var committed = false
            var newestFence = 0L
            timeline.getCommitsTimeline.filterCompletedInstants.getInstants.asScala.foreach {
              instant =>
                val details = timeline.getInstantDetails(instant)
                if (details.isPresent) {
                  val metadata = HoodieCommitMetadata.fromBytes(
                    details.get(), classOf[HoodieCommitMetadata])
                  if (incremental.transactionId.nonEmpty &&
                    Option(metadata.getMetadata(TRANSACTION_METADATA_KEY))
                      .contains(incremental.transactionId)) {
                    committed = true
                  }
                  if (incremental.sourceId.nonEmpty &&
                    Option(metadata.getMetadata(SOURCE_METADATA_KEY))
                      .contains(incremental.sourceId)) {
                    Option(metadata.getMetadata(FENCING_METADATA_KEY)).foreach { value =>
                      val fence = try {
                        value.trim.toLong
                      } catch {
                        case _: NumberFormatException =>
                          // Fail closed: ignoring an unreadable fence would let a stale writer
                          // through after a newer commit.
                          throw new PuddingException(
                            s"hudi commit metadata at $path has a non-numeric " +
                              s"$FENCING_METADATA_KEY for source '${incremental.sourceId}'")
                      }
                      if (fence < 0L) {
                        throw new PuddingException(
                          s"hudi commit metadata at $path has a negative " +
                            s"$FENCING_METADATA_KEY for source '${incremental.sourceId}'")
                      }
                      newestFence = math.max(newestFence, fence)
                    }
                  }
                }
            }
            if (!committed && incremental.transactionId.nonEmpty) {
              val archived = this.archivedTimelineState(meta, incremental, path)
              committed = archived.transactionCommitted
              newestFence = math.max(newestFence, archived.newestFencingToken)
            }
            TimelineState(committed, newestFence)
          } catch {
            case error: PuddingException => throw error
            case NonFatal(error) =>
              throw new PuddingException(
                s"failed to inspect hudi commit metadata at $path: " +
                  error.getClass.getSimpleName, error)
          }
        }
      }.getOrElse(TimelineState(transactionCommitted = false, newestFencingToken = 0L))
    }
  }

  /**
   * A commit can be archived between the sink write and the cursor commit. Scan the archived
   * timeline before deciding that an append transaction is new. The scan is only used when the
   * active timeline did not contain the transaction.
   *
   * The source cursor (a Hudi commit, JDBC timestamp or Kafka offset) is not necessarily a
   * target-table instant, so using it as an archived-timeline lower bound could skip the target
   * commit. The scan therefore covers the whole archived timeline; Hudi commit retention bounds
   * its size. Non-commit instants (clean/rollback) are skipped, but an unreadable commit instant
   * fails the job: appending could duplicate a batch that can no longer be proven committed.
   */
  private def archivedTimelineState(
      meta: HoodieTableMetaClient,
      incremental: IncrementalWrite,
      path: String): TimelineState = {
    val archived = try {
      meta.getArchivedTimeline()
    } catch {
      case NonFatal(error) =>
        throw new PuddingException(
          s"failed to read the archived hudi timeline at $path: " +
            error.getClass.getSimpleName, error)
    }
    var committed = false
    var newestFence = 0L
    archived.getInstants.asScala
      .filter(instant => instant.isCompleted &&
        Option(instant.getAction)
          .map(_.toLowerCase(Locale.ROOT))
          .exists(ArchivedCommitActions.contains))
      .foreach { instant =>
        val details = try {
          archived.getInstantDetails(instant)
        } catch {
          case NonFatal(error) =>
            throw new PuddingException(
              s"failed to read archived hudi instant ${instant.getTimestamp} at $path: " +
                error.getClass.getSimpleName, error)
        }
        if (!details.isPresent) {
          // A commit instant without metadata can not prove whether the transaction was
          // applied. Fail closed instead of appending the batch again.
          throw new PuddingException(
            s"archived hudi instant ${instant.getTimestamp} at $path has no commit metadata")
        }
        val metadata = try {
          HoodieCommitMetadata.fromBytes(details.get(), classOf[HoodieCommitMetadata])
        } catch {
          case NonFatal(error) =>
            throw new PuddingException(
              s"failed to parse archived hudi commit metadata at $path for instant " +
                instant.getTimestamp + ": " + error.getClass.getSimpleName, error)
        }
        if (incremental.transactionId.nonEmpty &&
          Option(metadata.getMetadata(TRANSACTION_METADATA_KEY))
            .contains(incremental.transactionId)) {
          committed = true
        }
        if (incremental.sourceId.nonEmpty &&
          Option(metadata.getMetadata(SOURCE_METADATA_KEY))
            .contains(incremental.sourceId)) {
          Option(metadata.getMetadata(FENCING_METADATA_KEY)).foreach { value =>
            val fence = try {
              value.trim.toLong
            } catch {
              case _: NumberFormatException =>
                throw new PuddingException(
                  s"hudi archived timeline at $path has a non-numeric " +
                    s"$FENCING_METADATA_KEY for source '${incremental.sourceId}'")
            }
            if (fence < 0L) {
              throw new PuddingException(
                s"hudi archived timeline at $path has a negative " +
                  s"$FENCING_METADATA_KEY for source '${incremental.sourceId}'")
            }
            newestFence = math.max(newestFence, fence)
          }
        }
      }
    TimelineState(committed, newestFence)
  }

  private def staleWriter(
      incremental: IncrementalWrite,
      newestFencingToken: Long): PuddingException = {
    IncrementalMetrics.increment(IncrementalMetrics.SinkFencingRejected)
    IncrementalAudit.fencingRejected(
      this.identifier,
      incremental.sourceId,
      incremental.transactionId,
      incremental.fencingToken,
      newestFencingToken)
    new PuddingException(
      s"hudi sink rejected a stale incremental writer for source '${incremental.sourceId}': " +
        s"fencing token ${incremental.fencingToken} is not newer than the committed token " +
        newestFencingToken)
  }

  private def tablePath(
      dataFrame: DataFrame,
      cfg: Map[String, Any]): Option[String] = {
    this.optionalString(cfg, TABLE_PATH_KEY).orElse {
      this.optionalString(cfg, TABLE_KEY).flatMap { table =>
        val identifier = dataFrame.sparkSession.sessionState.sqlParser
          .parseTableIdentifier(table)
        try {
          Some(dataFrame.sparkSession.sessionState.catalog
            .getTableMetadata(identifier).location.toString)
        } catch {
          case _: AnalysisException => None
        }
      }
    }
  }

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    context.catalog.foreach { resolved =>
      if (resolved.`type` != "hudi") {
        throw new PuddingException(
          s"hudi-sink requires a hudi catalog, but got: ${resolved.`type`}")
      }
    }

    val table = this.requiredString(cfg, TABLE_KEY)
    val tablePath = this.optionalString(cfg, TABLE_PATH_KEY)
    if (tablePath.isEmpty && context.catalog.isEmpty) {
      throw new PuddingException(
        s"$TABLE_PATH_KEY is required when no hudi catalog is configured!")
    }

    val options = this.writeOptions(cfg, table)
    if (this.streamingEnabled(cfg, context.executionMode)) {
      // Hudi 0.14 only has a path-based streaming writer. A catalog table still exposes its
      // base path, so catalog mode can reuse that writer without asking the job for tablePath.
      val streamingPath = tablePath.orElse(this.tablePath(dataFrame, cfg))
      this.stream(
        dataFrame, cfg, options, table, streamingPath, context.trigger, context.stateContract)
    } else {
      this.batch(dataFrame, cfg, options, table, tablePath)
    }
  }

  private def batch(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      table: String,
      tablePath: Option[String]): Unit = {
    val writer = dataFrame.write
      .format("org.apache.hudi")
      .options(options)
      .mode(this.saveMode(cfg))
    val partitionBy = this.stringSeq(cfg.get(PARTITION_BY_KEY), PARTITION_BY_KEY)
    val partitionedWriter = if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    } else {
      writer
    }
    tablePath match {
      case Some(path) => partitionedWriter.save(path)
      case None => partitionedWriter.saveAsTable(table)
    }
  }

  private def stream(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      table: String,
      tablePath: Option[String],
      trigger: Option[String],
      stateContract: Option[String]): Unit = {
    val path = tablePath.getOrElse {
      throw new PuddingException(
        s"hudi streaming write requires $TABLE_PATH_KEY or a catalog table with a resolvable " +
          "location")
    }
    val checkpoint = this.optionalString(cfg, CHECKPOINT_LOCATION_KEY)
      .orElse(options.get(CHECKPOINT_LOCATION_KEY))
      .getOrElse {
        throw new PuddingException(
          s"$CHECKPOINT_LOCATION_KEY is required for a hudi streaming write!")
      }
    val outputMode = this.streamingOutputMode(cfg)
    this.rejectContinuousTrigger(trigger)
    StreamingCheckpoint.validate(cfg, trigger, Some(checkpoint), stateContract)
    StreamingCdc.opColumn(cfg) match {
      case Some(opColumn) =>
        if (outputMode == StreamingOutputMode.Update) {
          throw new PuddingException(
            "hudi streaming cdc uses append output; outputMode=update is for full-row upserts")
        }
        this.streamCdc(
          dataFrame, cfg, options, table, path, checkpoint, opColumn, trigger)
      case None if outputMode == StreamingOutputMode.Update =>
        this.streamUpsert(
          dataFrame, cfg, options, table, path, checkpoint, trigger)
      case None =>
        // Hudi updates its metadata table on every streaming commit and fails with
        // "Positive number of partitions required" when a microbatch has no records.
        // Empty batches are normal while a CDC snapshot starts or a Kafka backlog drains,
        // so commit the offset and skip the Hudi write instead of failing the query.
        val batchConfig = cfg + (MODE_KEY -> "append")
        val writer = dataFrame.writeStream
          .foreachBatch { (batch: DataFrame, _: Long) =>
            if (this.hasRows(batch)) {
              this.batch(batch, batchConfig, options, table, Some(path))
            }
          }
          .option(CHECKPOINT_LOCATION_KEY, checkpoint)
          .outputMode(outputMode)
        val query = StreamingTrigger.applyTo(writer, trigger).start()
        StreamingCheckpoint.awaitTermination(query, cfg, trigger, Some(checkpoint))
    }
  }

  /**
   * Apply Spark update output as Hudi upserts. A stateful aggregation emits one full row per
   * changed key; each micro-batch is written as a Hudi upsert commit against the existing table.
   */
  private def streamUpsert(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      table: String,
      path: String,
      checkpoint: String,
      trigger: Option[String]): Unit = {
    val keys = this.stringSeq(cfg.get(RECORD_KEY_FIELDS_KEY), RECORD_KEY_FIELDS_KEY)
    if (keys.isEmpty) {
      throw new PuddingException(
        s"hudi streaming update requires $RECORD_KEY_FIELDS_KEY")
    }
    val operation = this.resolveOperation(cfg, keys)
    if (operation != DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL) {
      throw new PuddingException(
        s"hudi streaming update requires $OPERATION_KEY=upsert, but got '$operation'")
    }
    this.rejectContinuousTrigger(trigger)
    this.requireExistingTable(dataFrame, path)
    val upsertOptions = options +
      (DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
    val batchConfig = cfg + (MODE_KEY -> "append")

    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val changes = StreamingCdc.dropInternal(batch).dropDuplicates(keys)
        if (this.hasRows(changes)) {
          this.batch(changes, batchConfig, upsertOptions, table, Some(path))
        }
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode(StreamingOutputMode.Update)
    val query = StreamingTrigger.applyTo(writer, trigger).start()
    StreamingCheckpoint.awaitTermination(query, cfg, trigger, Some(checkpoint))
  }

  private def streamCdc(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      table: String,
      path: String,
      checkpoint: String,
      opColumn: String,
      trigger: Option[String]): Unit = {
    val keys = this.stringSeq(cfg.get(RECORD_KEY_FIELDS_KEY), RECORD_KEY_FIELDS_KEY)
    if (keys.isEmpty) {
      throw new PuddingException(
        s"hudi cdc streaming requires $RECORD_KEY_FIELDS_KEY")
    }
    val operation = this.resolveOperation(cfg, keys)
    if (operation != DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL) {
      throw new PuddingException(
        s"hudi cdc streaming requires $OPERATION_KEY=upsert, but got '$operation'")
    }
    this.requireExistingTable(dataFrame, path)

    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        this.writeCdcBatch(batch, cfg, options, table, path, keys, opColumn)
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode("append")
    val query = StreamingTrigger.applyTo(writer, trigger).start()
    StreamingCheckpoint.awaitTermination(query, cfg, trigger, Some(checkpoint))
  }

  private def writeCdcBatch(
      batch: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      table: String,
      path: String,
      keys: Seq[String],
      opColumn: String): Unit = {
    val prepared = StreamingCdc.prepare(
      batch, keys, opColumn, StreamingCdc.orderFields(cfg))
    val op = col(opColumn)
    val deletes = StreamingCdc.dropInternal(prepared.filter(op === "D"), Seq(opColumn))
    val changes = StreamingCdc.dropInternal(prepared.filter(op =!= "D"), Seq(opColumn))
    val batchConfig = cfg + (MODE_KEY -> "append")

    if (this.hasRows(deletes)) {
      val deleteOptions = options +
        (DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      this.batch(deletes, batchConfig, deleteOptions, table, Some(path))
    }
    if (this.hasRows(changes)) {
      this.batch(changes, batchConfig, options, table, Some(path))
    }
  }

  private def streamingOutputMode(cfg: Map[String, Any]): String = {
    StreamingOutputMode.resolve(
      Some(cfg), identifier, this.supportedStreamingOutputModes)
  }

  private def rejectContinuousTrigger(trigger: Option[String]): Unit = {
    if (trigger.exists(_.trim.toLowerCase(Locale.ROOT).startsWith("continuous"))) {
      throw new PuddingException(
        "hudi streaming does not support the continuous trigger")
    }
  }

  private def requireExistingTable(dataFrame: DataFrame, path: String): Unit = {
    val target = new Path(path, ".hoodie")
    val fileSystem = target.getFileSystem(dataFrame.sparkSession.sessionState.newHadoopConf())
    if (!fileSystem.exists(target)) {
      throw new PuddingException(
        s"hudi cdc streaming requires an existing table at $path; create it first")
    }
  }

  private def hasRows(dataFrame: DataFrame): Boolean =
    dataFrame.limit(1).count() > 0

  private def writeOptions(
      cfg: Map[String, Any],
      table: String): Map[String, String] = {
    val userOptions = this.getStringOptions(cfg, OPTIONS_KEY)
    val recordKeys = this.stringSeq(cfg.get(RECORD_KEY_FIELDS_KEY), RECORD_KEY_FIELDS_KEY)
    val operation = this.resolveOperation(cfg, recordKeys)
    val tableType = this.resolveTableType(cfg)
    val preCombineField = this.optionalString(cfg, PRECOMBINE_FIELD_KEY)
    val partitionBy = this.stringSeq(cfg.get(PARTITION_BY_KEY), PARTITION_BY_KEY)
    val partitionPathField = this.optionalString(cfg, PARTITION_PATH_FIELD_KEY)
      .map(Seq(_))
      .getOrElse(partitionBy)
    val hudiTableName = this.optionalString(cfg, HUDI_TABLE_NAME_KEY)
      .getOrElse(table.split("\\.").last.replace("`", ""))

    var options = userOptions
    options += DataSourceWriteOptions.OPERATION.key -> operation
    options += DataSourceWriteOptions.TABLE_TYPE.key -> tableType
    options += "hoodie.table.name" -> hudiTableName
    if (recordKeys.nonEmpty) {
      val recordKeysValue = recordKeys.mkString(",")
      options += DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeysValue
      // Hudi's catalog maps SQL short names when it creates a table.
      options += "primaryKey" -> recordKeysValue
    }
    if (partitionPathField.nonEmpty) {
      options += DataSourceWriteOptions.PARTITIONPATH_FIELD.key ->
        partitionPathField.mkString(",")
    }
    preCombineField.foreach { value =>
      options += DataSourceWriteOptions.PRECOMBINE_FIELD.key -> value
      options += "preCombineField" -> value
    }
    // Hudi's SQL catalog reads short table properties when it creates a table.
    options += "type" -> this.sqlTableType(tableType)

    this.optionalBoolean(cfg, HIVE_SYNC_ENABLED_KEY).foreach(value =>
      options += HIVE_SYNC_ENABLED_OPTION -> value.toString)
    this.optionalString(cfg, HIVE_SYNC_MODE_KEY).foreach(value =>
      options += HIVE_SYNC_MODE_OPTION -> value)
    this.optionalString(cfg, HIVE_DATABASE_KEY).foreach(value =>
      options += HIVE_SYNC_DATABASE_OPTION -> value)
    this.optionalString(cfg, HIVE_TABLE_KEY).foreach(value =>
      options += HIVE_SYNC_TABLE_OPTION -> value)
    this.optionalString(cfg, HIVE_METASTORE_URIS_KEY).foreach(value =>
      options += HIVE_SYNC_METASTORE_OPTION -> value)
    options
  }

  private def resolveOperation(
      cfg: Map[String, Any],
      recordKeys: Seq[String]): String = {
    val configured = this.optionalString(cfg, OPERATION_KEY)
    if (configured.isEmpty && recordKeys.isEmpty) {
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
    } else {
      val operation = configured
        .map(value => value.toLowerCase(Locale.ROOT).replace("-", "_"))
        .getOrElse("upsert")
      operation match {
        case "insert" => DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
        case "upsert" =>
          this.requireRecordKeys(recordKeys, operation)
          DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
        case "bulk_insert" | "bulkinsert" =>
          DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL
        case "delete" =>
          this.requireRecordKeys(recordKeys, operation)
          DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL
        case "insert_overwrite" =>
          DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL
        case "insert_overwrite_table" =>
          DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
        case "delete_partition" =>
          DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL
        case "bootstrap" =>
          DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL
        case other =>
          throw new PuddingException(s"unsupported hudi $OPERATION_KEY: $other")
      }
    }
  }

  private def requireRecordKeys(recordKeys: Seq[String], operation: String): Unit = {
    if (recordKeys.isEmpty) {
      throw new PuddingException(
        s"hudi $OPERATION_KEY '$operation' requires $RECORD_KEY_FIELDS_KEY")
    }
  }

  private def resolveTableType(cfg: Map[String, Any]): String = {
    val normalized = this.optionalString(cfg, TABLE_TYPE_KEY)
      .map(_.toLowerCase(Locale.ROOT).replace("-", "").replace("_", ""))
      .getOrElse("copyonwrite")
    normalized match {
      case "cow" | "copyonwrite" => DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL
      case "mor" | "mergeonread" => DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
      case other =>
        throw new PuddingException(
          s"unsupported hudi $TABLE_TYPE_KEY: $other, supported: COPY_ON_WRITE, MERGE_ON_READ")
    }
  }

  private def sqlTableType(tableType: String): String = {
    if (tableType == DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) "mor" else "cow"
  }

  private def saveMode(cfg: Map[String, Any]): SaveMode = {
    val mode = cfg.get(MODE_KEY) match {
      case Some(value) => value.toString.trim.toLowerCase(Locale.ROOT)
      case None => "append"
    }
    mode match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "ignore" => SaveMode.Ignore
      case "error" | "errorifexists" => SaveMode.ErrorIfExists
      case other => throw new PuddingException(s"unsupported save mode: $other")
    }
  }

  private def streamingEnabled(
      cfg: Map[String, Any],
      executionMode: Option[ExecutionMode]): Boolean = {
    val configured = Seq(IS_STREAMING_KEY, STREAM_WRITE_KEY, STREAM_WRITER_KEY)
      .iterator
      .flatMap(key => this.optionalBoolean(cfg, key))
      .exists(_ == true)
    this.effectiveStreaming(configured, executionMode)
  }

  private def stringSeq(value: Option[Any], key: String): Seq[String] = value match {
    case Some(text: String) =>
      text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    case Some(seq: Seq[_]) =>
      seq.map(item => this.requiredText(item, key)).toSeq
    case Some(array: Array[_]) =>
      array.map(item => this.requiredText(item, key)).toSeq
    case Some(list: java.util.List[_]) =>
      list.asScala.map(item => this.requiredText(item, key)).toSeq
    case Some(_) => throw new PuddingException(s"$key must be a string or a list of strings!")
    case None => Seq.empty
  }


  private def requiredText(value: Any, key: String): String = value match {
    case text: String if text.trim.nonEmpty => text.trim
    case _: String => throw new PuddingException(s"$key can not contain an empty value!")
    case _ => throw new PuddingException(s"$key must contain strings!")
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => throw new PuddingException(s"can not find param: $key, $key must be exist!")
    }
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def optionalBoolean(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ => throw new PuddingException(s"$key must be true or false, but got: $value")
        }
      case value => throw new PuddingException(s"$key must be a boolean, but got: $value")
    }
  }
}
