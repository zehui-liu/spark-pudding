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

import java.util.{Collections, Locale}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.fs.{FileIO, Path => PaimonPath}
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.commands.PaimonSparkWriter
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}
import org.apache.paimon.types.RowKind
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, lit, when}

import org.pudding.core.PuddingException
import org.pudding.core.catalog.CatalogTableName
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{DataSink, IncrementalSemantics, IncrementalWrite, SinkContext}
import org.pudding.core.incremental.{
  IncrementalAudit,
  IncrementalFrames,
  IncrementalMetrics,
  IncrementalStateCodec,
  StreamingCdc
}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Writes Apache Paimon tables.
 *
 * Catalog mode creates or writes a catalog table with `saveAsTable`; table creation supports
 * primary keys, partitions, buckets and arbitrary Paimon table properties. Path mode writes an
 * existing table. Batch and streaming writes are supported.
 */
class PaimonSink extends DataSink {

  private val TABLE_KEY = "table"

  private val PATH_KEY = "path"

  private val PRIMARY_KEY_KEY = "primaryKey"

  private val PARTITION_KEY = "partition"

  private val PARTITION_BY_KEY = "partitionBy"

  private val BUCKET_KEY = "bucket"

  private val OPTIONS_KEY = "options"

  private val TABLE_OPTIONS_KEY = "tableOptions"

  private val MODE_KEY = "mode"

  private val IS_STREAMING_KEY = "isStreaming"

  private val STREAM_WRITE_KEY = "streamWrite"

  private val STREAM_WRITER_KEY = "streamWriter"

  private val CHECKPOINT_LOCATION_KEY = "checkpointLocation"

  private val OUTPUT_MODE_KEY = "outputMode"

  /** Table option that prefixes the Paimon commit user for append replay protection. */
  private val COMMIT_USER_PREFIX_KEY = "commit.user-prefix"

  override def identifier: String = "paimon-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedStreamingOutputModes: Set[String] =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update)

  override def supportsStreamingUpsert: Boolean = true

  override def supportsFullRefresh: Boolean = true

  /**
   * Paimon's Spark catalog is not a staging catalog: Spark implements saveAsTable overwrite by
   * replacing the table, not by an atomic staged commit. Keep full refresh available only with
   * the explicit `allowNonAtomicReplace` opt-in.
   */
  override def supportsAtomicReplace: Boolean = false

  override def supportsLakeIncremental: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("paimon")

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert, IncrementalSemantics.Cdc)

  /**
   * Upsert and CDC merge rows by primary key. Append stores its transaction id in the Paimon
   * commit user and skips an already committed transaction, so all three semantics are safe to
   * replay when BuildJob supplies the transaction id.
   */
  override def isIdempotentIncremental(semantics: IncrementalSemantics): Boolean = true

  override def supportsIncrementalFencing: Boolean = true

  override def writeIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: IncrementalWrite): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    if (this.streamingEnabled(cfg, context.executionMode)) {
      throw new PuddingException("paimon-sink does not support streaming incremental writes")
    }
    if (incremental.semantics == IncrementalSemantics.Append &&
      (incremental.transactionId.nonEmpty || incremental.fencingToken > 0L)) {
      this.writeAppendTransaction(dataFrame, context, cfg, incremental)
      return
    }
    var next = cfg + (MODE_KEY -> "append")
    val input = incremental.semantics match {
      case IncrementalSemantics.Append =>
        dataFrame
      case IncrementalSemantics.Upsert =>
        dataFrame
      case IncrementalSemantics.Cdc =>
        val opColumn = incremental.opColumn
        if (!dataFrame.columns.contains(opColumn)) {
          throw new PuddingException(
            s"paimon cdc input is missing operation column: $opColumn")
        }
        val op = col(opColumn)
        val unknown = dataFrame.filter(op.isNull || !op.isin("I", "U", "D")).limit(1).count()
        if (unknown > 0) {
          throw new PuddingException(
            s"paimon cdc operation column '$opColumn' must contain I, U or D")
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
              "paimon cdc input has multiple changes per key but no ordering metadata; " +
                "set cdcOrderFields or provide source ordering")
          }
        }
        ordered.withColumn("_row_kind_",
          when(op === "I", lit(RowKind.INSERT.toByteValue()))
            .when(op === "U", lit(RowKind.UPDATE_AFTER.toByteValue()))
            .otherwise(lit(RowKind.DELETE.toByteValue())))
          .drop(opColumn)
    }

    if (incremental.keyFields.nonEmpty) {
      next += PRIMARY_KEY_KEY -> incremental.keyFields
    }
    if (incremental.semantics == IncrementalSemantics.Upsert ||
      incremental.semantics == IncrementalSemantics.Cdc) {
      val table = this.loadFileStoreTable(dataFrame.sparkSession, context, next)
      this.validatePrimaryKeys(table, incremental)
      this.validateFencing(table, incremental)
      // Paimon's Spark DataFrame writer does not accept the internal _row_kind_ column, and its
      // v2 writer ignores row kind and writer-level commit options. PaimonSparkWriter is the v1
      // writer used by SQL commands and applies row kind directly; a table view with
      // `commit.user-prefix` carries the transaction and fencing marker into the timeline.
      this.writeDirect(table, input, incremental)
    } else {
      this.writer(input, context.copy(config = Some(next)))
    }
  }

  private def validatePrimaryKeys(
      table: FileStoreTable,
      incremental: IncrementalWrite): Unit = {
    val primaryKeys = table.primaryKeys().asScala.toSet
    if (primaryKeys != incremental.keyFields.toSet) {
      throw new PuddingException(
        s"paimon ${incremental.semantics.name} keyFields " +
          s"${incremental.keyFields.mkString(", ")} must match the target table primary keys " +
          primaryKeys.toSeq.sorted.mkString(", "))
    }
  }

  private def loadFileStoreTable(
      sparkSession: SparkSession,
      context: SinkContext,
      cfg: Map[String, Any]): FileStoreTable = {
    this.optionalString(cfg, PATH_KEY) match {
      case Some(location) =>
        val path = new PaimonPath(location)
        val catalogContext = CatalogContext.create(
          new Options(), sparkSession.sessionState.newHadoopConf())
        FileStoreTableFactory.create(FileIO.get(path, catalogContext), path)
      case None =>
        this.loadCatalogTable(
          sparkSession, context, this.catalogTableName(context, cfg))
    }
  }

  private def catalogTableName(
      context: SinkContext,
      cfg: Map[String, Any]): String = {
    val name = this.optionalString(cfg, TABLE_KEY).getOrElse {
      throw new PuddingException(s"paimon-sink requires $TABLE_KEY or $PATH_KEY!")
    }
    context.catalog
      .map(catalog => CatalogTableName.qualify(catalog, name))
      .getOrElse(name)
  }

  private def loadCatalogTable(
      sparkSession: SparkSession,
      context: SinkContext,
      fullTable: String): FileStoreTable = {
    val configuredCatalogName = context.catalog
      .map(_.sparkCatalogName)
      .getOrElse(sparkSession.sessionState.catalogManager.currentCatalog.name())
    val parts = fullTable.split("\\.").map(_.trim).filter(_.nonEmpty)
    val catalogName =
      if (parts.length >= 3) parts.head else configuredCatalogName
    val prefix = s"$catalogName."
    val stripped =
      if (fullTable.startsWith(prefix)) fullTable.substring(prefix.length) else fullTable
    val tableParts = stripped.split("\\.").map(_.trim).filter(_.nonEmpty)
    val namespaces =
      if (tableParts.length > 1) tableParts.dropRight(1) else Array("default")
    val identifier = Identifier.of(namespaces, tableParts.last)
    val catalog = sparkSession.sessionState.catalogManager.catalog(catalogName)
      .asInstanceOf[TableCatalog]
    catalog.loadTable(identifier).asInstanceOf[SparkTable].getTable
      .asInstanceOf[FileStoreTable]
  }

  private case class TimelineState(
      transactionCommitted: Boolean,
      newestFencingToken: Long)

  /** Commit user pattern: `<transaction>-f<fence>-s<source-hash>_<uuid>`. */
  private val CommitUserPattern = """^(.+)-f(\d+)-s([0-9a-f]+)_[^_]+$""".r

  /**
   * Write an append batch whose Paimon commit user carries the transaction id, fencing token and
   * source hash.
   *
   * The Spark DataFrame writer ignores `commit.user-prefix` for existing tables because the
   * commit user comes from the table options. PaimonSparkWriter is built from a table view whose
   * options carry the marker, so the marker reaches the Paimon timeline. A missing table is
   * created with an empty commit before the transaction write; the empty commit has no marker and
   * a replay simply writes the data again.
   */
  private def writeAppendTransaction(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      incremental: IncrementalWrite): Unit = {
    val transactionId = incremental.transactionId
    var next = cfg + (MODE_KEY -> "append")
    if (incremental.keyFields.nonEmpty) {
      next += PRIMARY_KEY_KEY -> incremental.keyFields
    }
    val table = this.existingTable(dataFrame.sparkSession, context, next) match {
      case Some(existing) => existing
      case None =>
        this.writer(dataFrame.limit(0), context.copy(config = Some(next)))
        this.loadFileStoreTable(dataFrame.sparkSession, context, next)
    }
    val timeline = this.timelineState(table, incremental)
    if (this.isStale(incremental, timeline)) {
      throw this.staleWriter(incremental, timeline.newestFencingToken)
    }
    if (this.missingRetainedFence(incremental, timeline)) {
      logWarning(this.retentionWarning())
    }
    if (timeline.transactionCommitted) {
      logInfo(
        s"paimon append transaction '$transactionId' is already committed; " +
          "skipping the sink write")
    } else {
      this.writeDirect(table, dataFrame, incremental)
    }
  }

  private def validateFencing(
      table: FileStoreTable,
      incremental: IncrementalWrite): Unit = {
    val timeline = this.timelineState(table, incremental)
    if (this.isStale(incremental, timeline)) {
      throw this.staleWriter(incremental, timeline.newestFencingToken)
    }
    if (this.missingRetainedFence(incremental, timeline)) {
      logWarning(this.retentionWarning())
    }
  }

  private def missingRetainedFence(
      incremental: IncrementalWrite,
      timeline: TimelineState): Boolean = {
    incremental.fromCursor.nonEmpty && incremental.fencingToken > 0L &&
      timeline.newestFencingToken <= 0L
  }

  private def retentionWarning(): String = {
    "paimon sink found no retained fencing metadata for incremental source; the fencing check " +
      "can not see writers whose snapshots were cleaned up. Keep the Paimon snapshot retention " +
      "window longer than the maximum delayed writer lifetime."
  }

  /**
   * An equal fence is accepted only when the same transaction marker is already committed. A
   * stale writer that reused the generation has no marker and must be rejected.
   */
  private def isStale(
      incremental: IncrementalWrite,
      timeline: TimelineState): Boolean = {
    incremental.fencingToken > 0L &&
      (timeline.newestFencingToken > incremental.fencingToken ||
        (timeline.newestFencingToken == incremental.fencingToken &&
          !timeline.transactionCommitted))
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
      s"paimon sink rejected a stale incremental writer for source '${incremental.sourceId}': " +
        s"fencing token ${incremental.fencingToken} is not newer than the committed token " +
        newestFencingToken)
  }

  /** Inspect the Paimon timeline once for the transaction marker and the newest source fence. */
  private def timelineState(
      table: FileStoreTable,
      incremental: IncrementalWrite): TimelineState = {
    val sourceHash = this.sourceHash(incremental.sourceId)
    val iterator = table.snapshotManager().snapshots()
    var committed = false
    var newestFence = 0L
    while (iterator.hasNext) {
      val snapshot = iterator.next()
      val commitUser = Option(snapshot.commitUser()).getOrElse("")
      if (incremental.transactionId.nonEmpty) {
        if (commitUser.startsWith(incremental.transactionId + "_") ||
          commitUser.startsWith(incremental.transactionId + "-f") ||
          CommitUserPattern.findFirstMatchIn(commitUser)
            .exists(_.group(1) == incremental.transactionId)) {
          committed = true
        }
      }
      CommitUserPattern.findFirstMatchIn(commitUser) match {
        case Some(matched) =>
          val fence = try {
            matched.group(2).toLong
          } catch {
            case _: NumberFormatException =>
              throw new PuddingException(
                s"paimon snapshot for source '${incremental.sourceId}' has a fencing token " +
                  "that does not fit in a long")
          }
          if (sourceHash.contains(matched.group(3))) {
            newestFence = math.max(newestFence, fence)
          }
        case None if sourceHash.exists(hash => commitUser.contains("-s" + hash)) =>
          // Fail closed: a commit user that names this source but has no readable fence could
          // otherwise hide a newer generation from the stale-writer check.
          throw new PuddingException(
            s"paimon snapshot for source '${incremental.sourceId}' has an unreadable fencing " +
              "marker in its commit user")
        case None => ()
      }
    }
    TimelineState(committed, newestFence)
  }

  private def writeDirect(
      table: FileStoreTable,
      dataFrame: DataFrame,
      incremental: IncrementalWrite): Unit = {
    val prefix = this.commitUserPrefix(incremental)
    val target = if (prefix.nonEmpty) {
      table.copy(Collections.singletonMap(COMMIT_USER_PREFIX_KEY, prefix))
    } else {
      table
    }
    val writer = PaimonSparkWriter(target)
    val messages = writer.write(this.alignToTable(target, dataFrame))
    writer.commit(messages)
  }

  /**
   * PaimonSparkWriter maps DataFrame columns to table fields by position, so project the batch
   * into the table's column order and fail when a target column is missing. The internal
   * `_row_kind_` marker is kept when present.
   */
  private def alignToTable(table: FileStoreTable, dataFrame: DataFrame): DataFrame = {
    val targetColumns = table.schema().fields().asScala.map(_.name())
    val actual = dataFrame.columns.map(name => name.toLowerCase(Locale.ROOT) -> name).toMap
    val missing = targetColumns.filterNot(name => actual.contains(name.toLowerCase(Locale.ROOT)))
    if (missing.nonEmpty) {
      throw new PuddingException(
        s"paimon batch is missing target columns: ${missing.mkString(", ")}")
    }
    val rowKind = dataFrame.columns.find(_.equalsIgnoreCase("_row_kind_")).toSeq
    val ordered = targetColumns.map(name => col(actual(name.toLowerCase(Locale.ROOT)))) ++
      rowKind.map(col)
    dataFrame.select(ordered: _*)
  }

  private def commitUserPrefix(incremental: IncrementalWrite): String = {
    val transaction = Option(incremental.transactionId).filter(_.nonEmpty).getOrElse("pudding")
    this.sourceHash(incremental.sourceId) match {
      case Some(hash) =>
        List(transaction, s"f${math.max(0L, incremental.fencingToken)}", s"s$hash").mkString("-")
      case None if incremental.fencingToken > 0L =>
        s"$transaction-f${incremental.fencingToken}"
      case None =>
        transaction
    }
  }

  private def sourceHash(sourceId: String): Option[String] = {
    Option(sourceId).map(_.trim).filter(_.nonEmpty)
      .map(value => IncrementalStateCodec.sha256(value).take(12))
  }

  private def existingTable(
      sparkSession: SparkSession,
      context: SinkContext,
      cfg: Map[String, Any]): Option[FileStoreTable] = {
    this.optionalString(cfg, PATH_KEY) match {
      case Some(location) =>
        val hadoopPath = new HadoopPath(location)
        val fileSystem = hadoopPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
        if (!fileSystem.exists(new HadoopPath(hadoopPath, "schema"))) {
          None
        } else {
          Some(this.loadFileStoreTable(sparkSession, context, cfg))
        }
      case None =>
        val fullTable = this.catalogTableName(context, cfg)
        try {
          if (!sparkSession.catalog.tableExists(fullTable)) {
            None
          } else {
            Some(this.loadCatalogTable(sparkSession, context, fullTable))
          }
        } catch {
          case _: org.apache.spark.sql.AnalysisException => None
        }
    }
  }

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "paimon") {
        throw new PuddingException(
          s"paimon-sink requires a paimon catalog, but got: ${catalog.`type`}")
      }
    }

    val table = this.optionalString(cfg, TABLE_KEY)
    val path = this.optionalString(cfg, PATH_KEY)
    if (table.isEmpty && path.isEmpty) {
      throw new PuddingException(s"paimon-sink requires $TABLE_KEY or $PATH_KEY!")
    }

    val fullTable = table.map { name =>
      context.catalog
        .map(catalog => CatalogTableName.qualify(catalog, name))
        .getOrElse(name)
    }
    val options = this.writeOptions(cfg)
    val partitionBy = this.partitionColumns(cfg)
    val outputMode = this.optionalString(cfg, OUTPUT_MODE_KEY).getOrElse("append")

    if (this.streamingEnabled(cfg, context.executionMode)) {
      val checkpoint = this.optionalString(cfg, CHECKPOINT_LOCATION_KEY)
        .orElse(options.get(CHECKPOINT_LOCATION_KEY))
        .getOrElse {
          throw new PuddingException(
            s"$CHECKPOINT_LOCATION_KEY is required for a paimon streaming write!")
        }
      val resolvedMode = this.streamingOutputMode(outputMode)
      this.rejectContinuousTrigger(context.trigger)
      StreamingCheckpoint.validate(
        cfg, context.trigger, Some(checkpoint), context.stateContract)
      val existing = this.existingTable(dataFrame.sparkSession, context, cfg).getOrElse {
        throw new PuddingException(
          "paimon streaming requires an existing table; create it before starting the stream")
      }
      StreamingCdc.opColumn(cfg) match {
        case Some(opColumn) =>
          if (resolvedMode == StreamingOutputMode.Update) {
            throw new PuddingException(
              "paimon streaming cdc uses append output; outputMode=update is for full-row " +
                "upserts")
          }
          this.streamCdc(
            dataFrame, context, cfg, checkpoint, opColumn)
        case None if resolvedMode == StreamingOutputMode.Update =>
          this.streamUpsert(dataFrame, context, cfg, checkpoint)
        case None =>
          // Paimon 1.3.2 does not provide catalog-based `toTable` streaming. A catalog table
          // still has a concrete table path, so the native path writer can append to it without
          // asking the job to repeat that path in its own configuration.
          val location = path.getOrElse(existing.location().toString)
          // The table already exists. Passing creation/table properties such as
          // `primary-key` or `bucket` makes Paimon try to alter the table and fail with
          // "Change 'primary-key' is not supported yet"; only runtime write options belong
          // on the streaming writer.
          val runtimeOptions = this.getStringOptions(cfg, OPTIONS_KEY)
          // Paimon's streaming append writer reads the frame positionally. Map columns by
          // name to the existing table schema so source column order (for example an
          // unordered valueSchema map) does not shift the primary-key column.
          val aligned = this.alignToTable(existing, dataFrame)
          val writer = aligned.writeStream
            .format("paimon")
            .options(runtimeOptions)
            .option(CHECKPOINT_LOCATION_KEY, checkpoint)
            .outputMode(resolvedMode)
          val partitionedWriter = if (partitionBy.nonEmpty) {
            writer.partitionBy(partitionBy: _*)
          } else {
            writer
          }
          val triggeredWriter = StreamingTrigger.applyTo(partitionedWriter, context.trigger)
          val query = triggeredWriter.start(location)
          StreamingCheckpoint.awaitTermination(
            query, cfg, context.trigger, Some(checkpoint))
      }
    } else {
      val writer = dataFrame.write
        .format("paimon")
        .options(options)
        .mode(this.saveMode(cfg))
      val partitionedWriter = if (partitionBy.nonEmpty) {
        writer.partitionBy(partitionBy: _*)
      } else {
        writer
      }
      path match {
        case Some(location) => partitionedWriter.save(location)
        case None => partitionedWriter.saveAsTable(fullTable.get)
      }
    }
  }

  private def streamCdc(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      checkpoint: String,
      opColumn: String): Unit = {
    val sparkSession = dataFrame.sparkSession
    val table = this.existingTable(sparkSession, context, cfg).getOrElse {
      throw new PuddingException("paimon cdc streaming requires an existing table")
    }
    val tableKeys = table.primaryKeys().asScala.toSeq
    if (tableKeys.isEmpty) {
      throw new PuddingException(
        "paimon cdc streaming requires a primary-key table")
    }
    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        this.writeCdcBatch(batch, sparkSession, context, cfg, tableKeys, opColumn)
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode("append")
    val query = StreamingTrigger.applyTo(writer, context.trigger).start()
    StreamingCheckpoint.awaitTermination(
      query, cfg, context.trigger, Some(checkpoint))
  }

  private def writeCdcBatch(
      batch: DataFrame,
      sparkSession: SparkSession,
      context: SinkContext,
      cfg: Map[String, Any],
      keys: Seq[String],
      opColumn: String): Unit = {
    val prepared = StreamingCdc.prepare(
      batch, keys, opColumn, StreamingCdc.orderFields(cfg))
    val op = col(opColumn)
    val withKind = prepared.withColumn(
      "_row_kind_",
      when(op === "I", lit(RowKind.INSERT.toByteValue()))
        .when(op === "U", lit(RowKind.UPDATE_AFTER.toByteValue()))
        .otherwise(lit(RowKind.DELETE.toByteValue())))
    val output = StreamingCdc.dropInternal(withKind, Seq(opColumn))
    if (this.hasRows(output)) {
      val table = this.loadFileStoreTable(sparkSession, context, cfg)
      val writer = PaimonSparkWriter(table)
      val messages = writer.write(this.alignToTable(table, output))
      writer.commit(messages)
    }
  }

  private def streamingOutputMode(mode: String): String = {
    StreamingOutputMode.requireSupported(
      mode, identifier, this.supportedStreamingOutputModes)
  }

  /**
   * Apply Spark update output to a Paimon primary-key table. Each micro-batch writes full rows
   * through PaimonSparkWriter, so an existing key is merged by Paimon's primary-key merge engine
   * and a new key is inserted.
   */
  private def streamUpsert(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      checkpoint: String): Unit = {
    val sparkSession = dataFrame.sparkSession
    val table = this.existingTable(sparkSession, context, cfg).getOrElse {
      throw new PuddingException("paimon streaming update requires an existing table")
    }
    val keys = table.primaryKeys().asScala.toSeq
    if (keys.isEmpty) {
      throw new PuddingException(
        "paimon streaming update requires a primary-key table")
    }
    val configuredKeys =
      this.stringSeq(cfg.get(PRIMARY_KEY_KEY), PRIMARY_KEY_KEY)
    if (configuredKeys.nonEmpty && configuredKeys.toSet != keys.toSet) {
      throw new PuddingException(
        s"paimon streaming update $PRIMARY_KEY_KEY must match the target table primary keys " +
          keys.toSeq.sorted.mkString(", "))
    }

    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val changes = StreamingCdc.dropInternal(batch)
        val missingKeys = keys.filterNot(changes.columns.contains)
        if (missingKeys.nonEmpty) {
          throw new PuddingException(
            s"paimon streaming update rows are missing primary keys: " +
              missingKeys.mkString(", "))
        }
        val deduplicated = changes.dropDuplicates(keys)
        if (this.hasRows(deduplicated)) {
          val current = this.loadFileStoreTable(sparkSession, context, cfg)
          val targetColumns = current.rowType().getFieldNames.asScala.toSeq
          val missing = targetColumns.filterNot(deduplicated.columns.contains)
          if (missing.nonEmpty) {
            throw new PuddingException(
              s"paimon streaming update rows are missing target columns: " +
                missing.mkString(", "))
          }
          // PaimonSparkWriter maps the DataFrame positionally, so project into table order.
          val aligned = deduplicated.select(targetColumns.map(col): _*)
          val paimonWriter = PaimonSparkWriter(current)
          val messages = paimonWriter.write(aligned)
          paimonWriter.commit(messages)
        }
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode(StreamingOutputMode.Update)
    val query = StreamingTrigger.applyTo(writer, context.trigger).start()
    StreamingCheckpoint.awaitTermination(
      query, cfg, context.trigger, Some(checkpoint))
  }

  private def rejectContinuousTrigger(trigger: Option[String]): Unit = {
    if (trigger.exists(_.trim.toLowerCase(Locale.ROOT).startsWith("continuous"))) {
      throw new PuddingException(
        "paimon streaming does not support the continuous trigger")
    }
  }

  private def hasRows(dataFrame: DataFrame): Boolean =
    dataFrame.limit(1).count() > 0

  private def writeOptions(cfg: Map[String, Any]): Map[String, String] = {
    val userOptions = this.getStringOptions(cfg, OPTIONS_KEY)
    val tableOptions = this.getStringOptions(cfg, TABLE_OPTIONS_KEY)
    var options = userOptions ++ tableOptions

    this.stringSeq(cfg.get(PRIMARY_KEY_KEY), PRIMARY_KEY_KEY).foreach { primaryKey =>
      options += "primary-key" -> primaryKey.mkString(",")
    }
    cfg.get(BUCKET_KEY).filter(_ != null).foreach { value =>
      options += "bucket" -> value.toString
    }
    options
  }

  /**
   * Paimon accepts `partition` as a table property. The Spark writer also needs the columns for
   * table creation, so both names are accepted and used for partitionBy.
   */
  private def partitionColumns(cfg: Map[String, Any]): Seq[String] = {
    this.stringSeq(cfg.get(PARTITION_KEY), PARTITION_KEY)
      .orElse(this.stringSeq(cfg.get(PARTITION_BY_KEY), PARTITION_BY_KEY))
      .getOrElse(Seq.empty)
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
      .flatMap(key => this.booleanOption(cfg, key))
      .exists(_ == true)
    this.effectiveStreaming(configured, executionMode)
  }

  private def stringSeq(value: Option[Any], key: String): Option[Seq[String]] = value.map {
    case text: String =>
      val values = text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      if (values.isEmpty) {
        throw new PuddingException(s"$key can not be empty!")
      }
      values
    case seq: Seq[_] =>
      val values = seq.map(item => this.requiredText(item, key)).toSeq
      if (values.isEmpty) {
        throw new PuddingException(s"$key can not be empty!")
      }
      values
    case array: Array[_] =>
      val values = array.map(item => this.requiredText(item, key)).toSeq
      if (values.isEmpty) {
        throw new PuddingException(s"$key can not be empty!")
      }
      values
    case list: java.util.List[_] =>
      val values = list.asScala.map(item => this.requiredText(item, key)).toSeq
      if (values.isEmpty) {
        throw new PuddingException(s"$key can not be empty!")
      }
      values
    case _ => throw new PuddingException(s"$key must be a string or a list of strings!")
  }


  private def requiredText(value: Any, key: String): String = value match {
    case text: String if text.trim.nonEmpty => text.trim
    case _: String => throw new PuddingException(s"$key can not contain an empty value!")
    case _ => throw new PuddingException(s"$key must contain strings!")
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def booleanOption(cfg: Map[String, Any], key: String): Option[Boolean] = {
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
