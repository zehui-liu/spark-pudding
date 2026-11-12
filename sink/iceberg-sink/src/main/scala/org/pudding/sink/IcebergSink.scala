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

package org.pudding.sink

import java.util.{Locale, UUID}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

import org.pudding.core.PuddingException
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{
  DataSink,
  IncrementalSemantics,
  IncrementalWrite,
  SinkContext
}
import org.pudding.core.incremental.{
  IncrementalAudit,
  IncrementalFrames,
  IncrementalMetrics,
  StreamingCdc
}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * write dataframe to an Apache Iceberg table.
 *
 * The Spark session must have an Iceberg catalog configured by the job (the admin provider adds
 * the spark.sql.catalog.* settings). Existing tables are written with the Iceberg table API so
 * their schema, partition spec and properties stay intact. A missing table is created on the
 * first write; partitionBy is only used for that initial creation.
 */
class IcebergSink extends DataSink {

  private val TABLE_KEY = "table"

  private val MODE_KEY = "mode"

  private val OPERATION_KEY = "operation"

  private val PARTITION_BY_KEY = "partitionBy"

  private val DEFAULT_MODE = "append"

  private val MISSING_TABLE_ERROR = "TABLE_OR_VIEW_NOT_FOUND"

  private val TRANSACTION_SUMMARY_KEY = "pudding.transaction.id"

  private val FENCING_SUMMARY_KEY = "pudding.fencing.token"

  private val SOURCE_SUMMARY_KEY = "pudding.source.id"

  private val SNAPSHOT_PROPERTY_PREFIX = "snapshot-property."

  private val FENCING_PROPERTY_PREFIX = "pudding.fencing."

  private val FENCING_TRANSACTION_PREFIX = "pudding.fencing.transaction."

  private val IS_STREAMING_KEY = "isStreaming"

  private val STREAM_WRITE_KEY = "streamWrite"

  private val STREAM_WRITER_KEY = "streamWriter"

  private val CHECKPOINT_LOCATION_KEY = "checkpointLocation"

  private val RECORD_KEY_FIELDS_KEY = "recordKeyFields"

  override def identifier: String = "iceberg-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportedStreamingOutputModes: Set[String] =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update)

  override def supportsStreamingUpsert: Boolean = true

  override def supportsFullRefresh: Boolean = true

  override def supportsAtomicReplace: Boolean = true

  override def supportsLakeIncremental: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("iceberg")

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Upsert, IncrementalSemantics.Cdc)

  /**
   * Append transactions are recorded in the Iceberg snapshot summary and skipped on replay.
   * Upsert and CDC use an idempotent MERGE, so replaying the same bounded batch produces the same
   * table state.
   */
  override def isIdempotentIncremental(semantics: IncrementalSemantics): Boolean = true

  override def supportsIncrementalFencing: Boolean = true

  /**
   * Incremental append always appends, even if the sink config says `overwrite`; overwriting the
   * target would discard earlier batches and make the cursor meaningless. Upsert and CDC are
   * applied as bounded MERGE operations. Append also records transaction/fencing metadata; the
   * merge paths rely on their own idempotence and do not write a snapshot marker.
   */
  override def writeIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: IncrementalWrite): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    if (this.streamingEnabled(cfg, context.executionMode)) {
      throw new PuddingException("iceberg-sink does not support streaming incremental writes")
    }
    incremental.semantics match {
      case IncrementalSemantics.Append =>
        val marker = if (incremental.transactionId.nonEmpty || incremental.fencingToken > 0L) {
          Some(incremental)
        } else {
          None
        }
        this.write(
          dataFrame,
          context.copy(config = Some(cfg + (MODE_KEY -> "append"))),
          marker)
      case IncrementalSemantics.Upsert =>
        this.writeUpsertIncremental(dataFrame, context, cfg, incremental)
      case IncrementalSemantics.Cdc =>
        this.writeCdcIncremental(dataFrame, context, cfg, incremental)
    }
  }

  /**
   * Apply a bounded upsert batch. Duplicate keys are reduced before the MERGE: a source-provided
   * ordering keeps the latest change, otherwise duplicate keys are rejected by the merge rather
   * than being silently applied in an arbitrary order.
   */
  private def writeUpsertIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      incremental: IncrementalWrite): Unit = {
    val keys = incremental.keyFields
    if (keys.isEmpty) {
      throw new PuddingException("iceberg upsert incremental write requires keyFields")
    }
    this.requireMergeSupport(dataFrame, "upsert")
    val table = this.resolveTable(context, cfg)
    val sparkSession = dataFrame.sparkSession
    val orderFields = if (incremental.orderFields.nonEmpty) {
      incremental.orderFields
    } else {
      // A batch source may not expose ordering metadata; cdcOrderFields is the documented
      // fallback for resolving multiple rows for the same key.
      StreamingCdc.orderFields(cfg)
    }
    val ordered = if (orderFields.nonEmpty) {
      IncrementalFrames.latestByKey(dataFrame, keys, orderFields)
    } else {
      val duplicate = dataFrame
        .groupBy(keys.map(key => col(key)): _*)
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count() > 0
      if (duplicate) {
        throw new PuddingException(
          "iceberg upsert incremental write has multiple rows per key but no ordering " +
            "metadata; set cdcOrderFields or provide source ordering")
      }
      dataFrame
    }
    val changes = StreamingCdc.dropInternal(ordered)
    if (!tableExists(sparkSession, table)) {
      this.createTable(changes, table, cfg)
    }
    this.validateFencing(
      sparkSession, table, incremental, transactionCommitted = false, allowEqual = true)
    if (this.hasRows(changes)) {
      val targetColumns = this.targetColumns(dataFrame, table)
      val missingKeys = keys.filterNot(targetColumns.contains)
      if (missingKeys.nonEmpty) {
        throw new PuddingException(
          s"iceberg upsert keyFields are not columns of $table: " +
            missingKeys.mkString(", "))
      }
      val missingColumns = targetColumns.filterNot(changes.columns.contains)
      if (missingColumns.nonEmpty) {
        throw new PuddingException(
          s"iceberg upsert rows are missing target columns: " +
            missingColumns.mkString(", "))
      }
      this.mergeChanges(sparkSession, table, changes, keys, targetColumns)
      this.recordFencing(sparkSession, table, incremental)
    }
  }

  /**
   * Apply a bounded CDC batch. A full initial read is a snapshot of rows already accompanied by
   * the source's insert operation, so no ordering is required. Later ranges are normalized and
   * deduplicated with the source ordering before deletes and upserts are merged.
   */
  private def writeCdcIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      incremental: IncrementalWrite): Unit = {
    val keys = incremental.keyFields
    if (keys.isEmpty) {
      throw new PuddingException("iceberg cdc incremental write requires keyFields")
    }
    val opColumn = incremental.opColumn
    if (!dataFrame.columns.contains(opColumn)) {
      throw new PuddingException(
        s"iceberg cdc input is missing operation column: $opColumn")
    }
    this.requireMergeSupport(dataFrame, "cdc")
    val table = this.resolveTable(context, cfg)
    val sparkSession = dataFrame.sparkSession
    val prepared = if (incremental.fromCursor.isEmpty) {
      val op = col(opColumn)
      val unknown = dataFrame.filter(op.isNull || !op.isin("I", "U", "D")).limit(1).count()
      if (unknown > 0) {
        throw new PuddingException(
          s"iceberg cdc operation column '$opColumn' must contain I, U or D")
      }
      dataFrame.dropDuplicates(keys)
    } else {
      val normalized = StreamingCdc.normalizeOps(dataFrame, opColumn)
      val orderFields =
        if (incremental.orderFields.nonEmpty) incremental.orderFields
        else StreamingCdc.orderFields(cfg)
      if (orderFields.nonEmpty) {
        IncrementalFrames.latestByKey(normalized, keys, orderFields)
      } else {
        val duplicate = normalized
          .groupBy(keys.map(key => col(key)): _*)
          .count()
          .filter(col("count") > 1)
          .limit(1)
          .count() > 0
        if (duplicate) {
          throw new PuddingException(
            "iceberg cdc incremental write has multiple changes per key but no ordering " +
              "metadata; set cdcOrderFields or provide source ordering")
        }
        normalized
      }
    }
    val op = col(opColumn)
    val deletes = StreamingCdc.dropInternal(prepared.filter(op === "D"), Seq(opColumn))
    val changes = StreamingCdc.dropInternal(prepared.filter(op =!= "D"), Seq(opColumn))
    if (!tableExists(sparkSession, table)) {
      this.createTable(changes, table, cfg)
    }
    val targetColumns = this.targetColumns(dataFrame, table)
    val missingKeys = keys.filterNot(targetColumns.contains)
    if (missingKeys.nonEmpty) {
      throw new PuddingException(
        s"iceberg cdc keyFields are not columns of $table: " + missingKeys.mkString(", "))
    }
    val missingColumns = targetColumns.filterNot(changes.columns.contains)
    if (this.hasRows(changes) && missingColumns.nonEmpty) {
      throw new PuddingException(
        s"iceberg cdc update/insert rows are missing target columns: " +
          missingColumns.mkString(", "))
    }
    this.validateFencing(
      sparkSession, table, incremental, transactionCommitted = false, allowEqual = true)
    val wroteDeletes = this.hasRows(deletes)
    val wroteChanges = this.hasRows(changes)
    if (wroteDeletes) {
      this.mergeDeletes(sparkSession, table, deletes, keys)
    }
    if (wroteChanges) {
      this.mergeChanges(sparkSession, table, changes, keys, targetColumns)
    }
    if (wroteDeletes || wroteChanges) {
      this.recordFencing(sparkSession, table, incremental)
    }
  }

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    this.write(dataFrame, context, None)
  }

  private def write(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: Option[IncrementalWrite]): Unit = {
    val cfg = context.config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val table = this.resolveTable(context, cfg)

    val mode = cfg.get(MODE_KEY) match {
      case Some(value: String) => parseMode(value)
      case Some(other) => throw new PuddingException(
        s"mode must be string, but got: ${other.getClass.getSimpleName}")
      case None => DEFAULT_MODE
    }

    val partitionBy = this.partitionColumns(cfg)

    val sparkSession = dataFrame.sparkSession
    if (this.streamingEnabled(cfg, context.executionMode)) {
      this.stream(dataFrame, context, cfg, table)
      return
    }
    val transactionId = incremental.map(_.transactionId).filter(_.nonEmpty)
    val exists = tableExists(sparkSession, table)
    if (exists) {
      val committed =
        transactionId.exists(id => transactionCommitted(sparkSession, table, id))
      incremental.foreach(value =>
        this.validateFencing(sparkSession, table, value, committed))
      mode match {
        case "append" =>
          if (committed) {
            logInfo(
              s"iceberg append transaction '${transactionId.get}' is already committed; " +
                "skipping the sink write")
          } else {
            val writer = this.appendWriter(dataFrame, table, incremental)
            writer.append()
          }
        case "overwrite" =>
          // Overwrite every row while preserving the existing table's schema and partition spec.
          dataFrame.writeTo(table).overwrite(lit(true))
        case "ignore" =>
          logInfo(s"iceberg table $table already exists, ignoring write")
        case _ =>
          throw new PuddingException(
            s"iceberg table $table already exists and mode is $mode")
      }
    } else {
      incremental match {
        case Some(_) =>
          // Create the table with an empty schema-only commit, then append the transaction in a
          // second commit. A crash before the append leaves an empty table and no transaction
          // marker, so the next run safely writes the batch once.
          val empty = dataFrame.limit(0)
          val creator = empty.writeTo(table).using("iceberg")
          val partitionedCreator =
            if (partitionBy.nonEmpty) {
              val columns: Seq[org.apache.spark.sql.Column] =
                partitionBy.map(name => col(name))
              creator.partitionedBy(columns.head, columns.tail: _*)
            } else {
              creator
            }
          partitionedCreator.create()
          this.appendWriter(dataFrame, table, incremental).append()
        case None =>
          if (partitionBy.nonEmpty) {
            val columns: Seq[org.apache.spark.sql.Column] =
              partitionBy.map(name => col(name))
            dataFrame.writeTo(table).using("iceberg")
              .partitionedBy(columns.head, columns.tail: _*)
              .create()
          } else {
            dataFrame.writeTo(table).using("iceberg").create()
          }
      }
    }
  }

  private def resolveTable(
      context: SinkContext,
      cfg: Map[String, Any]): String = {
    val configuredTable = cfg.get(TABLE_KEY) match {
      case Some(value: String) => value.trim
      case Some(other) => throw new PuddingException(
        s"table must be string, but got: ${other.getClass.getSimpleName}")
      case None => throw new PuddingException("table can not be empty!")
    }
    if (configuredTable.isEmpty) {
      throw new PuddingException("table can not be empty!")
    }
    context.catalog match {
      case Some(catalog) =>
        if (catalog.`type` != "iceberg") {
          throw new PuddingException(
            s"iceberg-sink requires an iceberg catalog, but got: ${catalog.`type`}")
        }
        IcebergSink.qualify(configuredTable, catalog.sparkCatalogName, catalog.name)
      case None => configuredTable
    }
  }

  private def partitionColumns(cfg: Map[String, Any]): Seq[String] = {
    cfg.get(PARTITION_BY_KEY) match {
      case Some(columns: Seq[_]) => columns.map(_.toString.trim).filter(_.nonEmpty)
      case Some(columns: String) =>
        columns.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      case Some(_) => throw new PuddingException(
        "partitionBy must be an array or a comma-separated string!")
      case None => Seq.empty[String]
    }
  }

  private def createTable(
      dataFrame: DataFrame,
      table: String,
      cfg: Map[String, Any]): Unit = {
    val empty = dataFrame.limit(0)
    val creator = empty.writeTo(table).using("iceberg")
    val partitionBy = this.partitionColumns(cfg)
    val partitionedCreator = if (partitionBy.nonEmpty) {
      val columns: Seq[org.apache.spark.sql.Column] = partitionBy.map(name => col(name))
      creator.partitionedBy(columns.head, columns.tail: _*)
    } else {
      creator
    }
    partitionedCreator.create()
  }

  private def requireMergeSupport(dataFrame: DataFrame, operation: String): Unit = {
    val extensions = dataFrame.sparkSession.conf.get("spark.sql.extensions", "")
    if (!extensions.contains("IcebergSparkSessionExtensions")) {
      throw new PuddingException(
        s"iceberg $operation requires Spark SQL MERGE support; configure " +
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    }
  }

  private def appendWriter(
      dataFrame: DataFrame,
      table: String,
      incremental: Option[IncrementalWrite]): org.apache.spark.sql.DataFrameWriterV2[org.apache.spark.sql.Row] = {
    val writer = dataFrame.writeTo(table)
    incremental match {
      case Some(value) =>
        var next = writer
        if (value.transactionId.nonEmpty) {
          next = next.option(
            SNAPSHOT_PROPERTY_PREFIX + TRANSACTION_SUMMARY_KEY, value.transactionId)
        }
        if (value.sourceId.nonEmpty) {
          next = next.option(SNAPSHOT_PROPERTY_PREFIX + SOURCE_SUMMARY_KEY, value.sourceId)
        }
        if (value.fencingToken > 0L) {
          next = next.option(
            SNAPSHOT_PROPERTY_PREFIX + FENCING_SUMMARY_KEY, value.fencingToken.toString)
        }
        next
      case None => writer
    }
  }

  /**
   * Reject a writer whose fencing token is not newer than the newest token committed for the
   * same source. An equal token is accepted for an already committed append transaction or for a
   * merge replay of the exact transaction stored with the fence, so a replay of the same batch
   * stays a no-op while a different writer that reused the generation is rejected.
   */
  private def readTableProperties(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String): Map[String, String] = {
    sparkSession.sql(s"SHOW TBLPROPERTIES $table").collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
  }

  private def validateFencing(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String,
      incremental: IncrementalWrite,
      transactionCommitted: Boolean,
      allowEqual: Boolean = false): Unit = {
    if (incremental.fencingToken <= 0L || incremental.sourceId.isEmpty) {
      return
    }
    val escapedSource = incremental.sourceId.replace("'", "''")
    val query =
      s"SELECT summary['$FENCING_SUMMARY_KEY'] " +
        s"FROM $table.snapshots " +
        s"WHERE summary['$SOURCE_SUMMARY_KEY'] = '$escapedSource' " +
        s"AND summary['$FENCING_SUMMARY_KEY'] IS NOT NULL"
    val snapshotNewest = try {
      sparkSession.sql(query).collect().foldLeft(0L) { (current, row) =>
        val text = row.getString(0)
        val fence = try {
          text.trim.toLong
        } catch {
          case _: NumberFormatException =>
            // A malformed value must not be treated as NULL/zero: doing so would let a stale
            // writer pass the fencing check.
            throw new PuddingException(
              s"iceberg snapshot for source '${incremental.sourceId}' has a non-numeric " +
                s"$FENCING_SUMMARY_KEY")
        }
        if (fence < 0L) {
          throw new PuddingException(
            s"iceberg snapshot for source '${incremental.sourceId}' has a negative " +
              s"$FENCING_SUMMARY_KEY")
        }
        math.max(current, fence)
      }
    } catch {
      case error: AnalysisException if isMissingTable(error) => 0L
      case error: PuddingException => throw error
      case NonFatal(error) =>
        throw new PuddingException(
          s"failed to inspect iceberg fencing metadata for $table: " +
            error.getClass.getSimpleName, error)
    }
    // Merge paths cannot set a snapshot summary property through Spark SQL. They persist the
    // generation and transaction id as table properties after the MERGE so a later writer can
    // still see them.
    val propertyKey = FENCING_PROPERTY_PREFIX + incremental.sourceId
    val transactionKey = FENCING_TRANSACTION_PREFIX + incremental.sourceId
    val properties = try {
      // SHOW TBLPROPERTIES works through the configured Iceberg catalog. The metadata table
      // form (`table.properties`) is not resolved by every Spark/Iceberg version combination.
      this.readTableProperties(sparkSession, table)
    } catch {
      case error: AnalysisException if isMissingTable(error) => Map.empty[String, String]
      case error: PuddingException => throw error
      case NonFatal(error) =>
        throw new PuddingException(
          s"failed to inspect iceberg table fencing metadata for $table: " +
            error.getClass.getSimpleName, error)
    }
    val propertyNewest = properties.get(propertyKey).map { text =>
      val fence = try {
        text.trim.toLong
      } catch {
        case _: NumberFormatException =>
          throw new PuddingException(
            s"iceberg table property for source '${incremental.sourceId}' has a " +
              s"non-numeric $propertyKey")
      }
      if (fence < 0L) {
        throw new PuddingException(
          s"iceberg table property for source '${incremental.sourceId}' has a negative " +
            propertyKey)
      }
      fence
    }.getOrElse(0L)
    val storedTransaction = properties.get(transactionKey).map(_.trim).filter(_.nonEmpty)
    val newest = math.max(snapshotNewest, propertyNewest)
    if (incremental.fromCursor.nonEmpty && newest <= 0L) {
      logWarning(
        "iceberg sink found no retained fencing metadata for incremental source " +
          s"'${incremental.sourceId}'; the fencing check can not see writers whose snapshots " +
          "were expired. Keep the Iceberg snapshot retention window longer than the maximum " +
          "delayed writer lifetime.")
    }
    // An equal token is accepted for a committed append transaction, or for a merge replay of
    // the exact transaction whose id was stored with the fence. A different transaction that
    // reuses the generation is stale.
    val equalAccepted = newest == incremental.fencingToken &&
      (transactionCommitted ||
        (allowEqual && storedTransaction.contains(incremental.transactionId)))
    val stale = newest > incremental.fencingToken ||
      (newest == incremental.fencingToken && !equalAccepted)
    if (stale) {
      IncrementalMetrics.increment(IncrementalMetrics.SinkFencingRejected)
      IncrementalAudit.fencingRejected(
        this.identifier,
        incremental.sourceId,
        incremental.transactionId,
        incremental.fencingToken,
        newest)
      throw new PuddingException(
        s"iceberg sink rejected a stale incremental writer for source " +
          s"'${incremental.sourceId}': fencing token ${incremental.fencingToken} is not newer " +
          s"than the committed token $newest")
    }
  }

  /**
   * Persist the merge writer's generation and transaction id after the MERGE. Append snapshots
   * use a snapshot summary property, but Spark SQL MERGE has no per-statement snapshot-property
   * option. The table property is a separate metadata commit; it narrows the stale-writer window
   * without rewriting data. A property is never downgraded to an older fence, and an equal fence
   * from a different transaction is not overwritten.
   */
  private def recordFencing(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String,
      incremental: IncrementalWrite): Unit = {
    if (incremental.fencingToken <= 0L || incremental.sourceId.isEmpty) {
      return
    }
    val tokenKey = FENCING_PROPERTY_PREFIX + incremental.sourceId
    val transactionKey = FENCING_TRANSACTION_PREFIX + incremental.sourceId
    try {
      val properties = this.readTableProperties(sparkSession, table)
      val currentToken = properties.get(tokenKey).map { text =>
        try {
          text.trim.toLong
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(
              s"iceberg table property for source '${incremental.sourceId}' has a " +
                s"non-numeric $tokenKey")
        }
      }.getOrElse(0L)
      if (currentToken < 0L) {
        throw new PuddingException(
          s"iceberg table property for source '${incremental.sourceId}' has a negative " +
            tokenKey)
      }
      val currentTransaction = properties.get(transactionKey).map(_.trim).filter(_.nonEmpty)
      if (currentToken > incremental.fencingToken) {
        logWarning(
          s"iceberg sink is not downgrading the fencing property for source " +
            s"'${incremental.sourceId}' from $currentToken to ${incremental.fencingToken}")
        return
      }
      if (currentToken == incremental.fencingToken &&
        currentTransaction.exists(_ != incremental.transactionId)) {
        logWarning(
          "iceberg sink is not overwriting fencing metadata for a different transaction " +
            s"of source '${incremental.sourceId}' with the same token")
        return
      }
      val escapedTokenKey = tokenKey.replace("'", "''")
      val escapedTransactionKey = transactionKey.replace("'", "''")
      val propertiesToSet = if (incremental.transactionId.nonEmpty) {
        val escapedTransaction = incremental.transactionId.replace("'", "''")
        s"'$escapedTokenKey' = '${incremental.fencingToken}', " +
          s"'$escapedTransactionKey' = '$escapedTransaction'"
      } else {
        s"'$escapedTokenKey' = '${incremental.fencingToken}'"
      }
      sparkSession.sql(s"ALTER TABLE $table SET TBLPROPERTIES ($propertiesToSet)")
    } catch {
      case error: AnalysisException if isMissingTable(error) =>
        throw new PuddingException(
          s"iceberg table $table disappeared before its fencing metadata was recorded", error)
      case error: PuddingException => throw error
      case NonFatal(error) =>
        throw new PuddingException(
          s"failed to record iceberg fencing metadata for $table: " +
            error.getClass.getSimpleName, error)
    }
  }

  private def transactionCommitted(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String,
      transactionId: String): Boolean = {
    if (!tableExists(sparkSession, table)) {
      false
    } else {
      val escapedTransaction = transactionId.replace("'", "''")
      val query =
        s"SELECT 1 FROM $table.snapshots " +
          s"WHERE summary['$TRANSACTION_SUMMARY_KEY'] = '$escapedTransaction' LIMIT 1"
      try {
        sparkSession.sql(query).limit(1).count() > 0
      } catch {
        case error: AnalysisException if isMissingTable(error) => false
        case NonFatal(error) =>
          throw new PuddingException(
            s"failed to inspect iceberg snapshot metadata for $table: " +
              error.getClass.getSimpleName, error)
      }
    }
  }

  /**
   * A missing Iceberg catalog or table must be distinguishable from other analysis failures, so
   * only TABLE_OR_VIEW_NOT_FOUND is treated as "create it". Anything else is a real error.
   */
  private def tableExists(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String): Boolean = {
    try {
      sparkSession.sql(s"SELECT * FROM $table WHERE 1 = 0")
      true
    } catch {
      case error: AnalysisException if isMissingTable(error) => false
    }
  }

  private def isMissingTable(error: AnalysisException): Boolean = {
    val errorClass = Option(error.getErrorClass)
    val message = Option(error.getMessage)
    errorClass.contains(MISSING_TABLE_ERROR) ||
      message.exists(_.contains(MISSING_TABLE_ERROR))
  }

  private def stream(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      table: String): Unit = {
    context.catalog match {
      case Some(catalog) if catalog.`type` == "iceberg" =>
      case Some(catalog) =>
        throw new PuddingException(
          s"iceberg-sink requires an iceberg catalog, but got: ${catalog.`type`}")
      case None =>
        throw new PuddingException(
          "iceberg streaming requires an iceberg catalog reference")
    }
    val sparkSession = dataFrame.sparkSession
    if (!tableExists(sparkSession, table)) {
      throw new PuddingException(
        s"iceberg streaming requires an existing table: $table; create it first")
    }
    val configuredMode = cfg.get(MODE_KEY).filter(_ != null).map(_.toString)
    if (configuredMode.exists(mode => this.parseMode(mode) != "append")) {
      throw new PuddingException(
        "iceberg streaming supports only mode=append")
    }
    val options = this.getStringOptions(cfg, "options")
    val checkpoint = this.optionalString(cfg, CHECKPOINT_LOCATION_KEY)
      .orElse(options.get(CHECKPOINT_LOCATION_KEY))
      .getOrElse {
        throw new PuddingException(
          s"$CHECKPOINT_LOCATION_KEY is required for an iceberg streaming write")
      }
    val outputMode = StreamingOutputMode.resolve(
      Some(cfg), identifier, this.supportedStreamingOutputModes)
    if (context.trigger.exists(_.trim.toLowerCase(Locale.ROOT).startsWith("continuous"))) {
      throw new PuddingException(
        "iceberg streaming does not support the continuous trigger")
    }

    StreamingCheckpoint.validate(
      cfg, context.trigger, Some(checkpoint), context.stateContract)
    val operation = this.optionalString(cfg, OPERATION_KEY)
      .map(_.toLowerCase(Locale.ROOT))
    StreamingCdc.opColumn(cfg) match {
      case Some(opColumn) =>
        if (outputMode == StreamingOutputMode.Update) {
          throw new PuddingException(
            "iceberg streaming cdc uses append output; outputMode=update is for " +
              "full-row upserts")
        }
        this.streamCdc(dataFrame, context, cfg, table, checkpoint, opColumn)
      case None if outputMode == StreamingOutputMode.Update =>
        if (operation.exists(_ != "upsert")) {
          throw new PuddingException(
            "iceberg streaming outputMode=update requires operation=upsert or no operation")
        }
        this.streamMerge(dataFrame, context, cfg, table, checkpoint)
      case None if operation.contains("upsert") =>
        this.streamUpsert(dataFrame, context, cfg, table, checkpoint)
      case None if operation.exists(_ != "append") =>
        throw new PuddingException(
          s"iceberg streaming supports operation=append or operation=upsert, " +
            s"but got '$operation'")
      case None =>
        val writer = dataFrame.writeStream
          .format("iceberg")
          .options(this.getStringOptions(cfg, "options"))
          .option(CHECKPOINT_LOCATION_KEY, checkpoint)
          .outputMode(StreamingOutputMode.Append)
        val query = StreamingTrigger.applyTo(writer, context.trigger).toTable(table)
        StreamingCheckpoint.awaitTermination(
          query, cfg, context.trigger, Some(checkpoint))
    }
  }

  /**
   * Apply Spark update output as Iceberg MERGE. Update output contains one complete row per
   * changed key, so no ordering field is needed; the target table is matched on record keys and
   * new keys are inserted.
   */
  private def streamMerge(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      table: String,
      checkpoint: String): Unit = {
    val keys = this.stringSeq(cfg.get(RECORD_KEY_FIELDS_KEY), RECORD_KEY_FIELDS_KEY)
    if (keys.isEmpty) {
      throw new PuddingException(
        s"iceberg streaming update requires $RECORD_KEY_FIELDS_KEY")
    }
    val extensions = dataFrame.sparkSession.conf.get("spark.sql.extensions", "")
    if (!extensions.contains("IcebergSparkSessionExtensions")) {
      throw new PuddingException(
        "iceberg streaming update requires Spark SQL MERGE support; configure " +
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    }
    val targetColumns = this.targetColumns(dataFrame, table)
    val missingKeys = keys.filterNot(targetColumns.contains)
    if (missingKeys.nonEmpty) {
      throw new PuddingException(
        s"iceberg streaming update keys are not columns of $table: " +
          missingKeys.mkString(", "))
    }
    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val changes = StreamingCdc.dropInternal(batch).dropDuplicates(keys)
        val missing = targetColumns.filterNot(changes.columns.contains)
        if (this.hasRows(changes) && missing.nonEmpty) {
          throw new PuddingException(
            s"iceberg streaming update rows are missing target columns: " +
              missing.mkString(", "))
        }
        if (this.hasRows(changes)) {
          this.mergeChanges(batch.sparkSession, table, changes, keys, targetColumns)
        }
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode(StreamingOutputMode.Update)
    val query = StreamingTrigger.applyTo(writer, context.trigger).start()
    StreamingCheckpoint.awaitTermination(
      query, cfg, context.trigger, Some(checkpoint))
  }

  private def streamCdc(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      table: String,
      checkpoint: String,
      opColumn: String): Unit = {
    val keys = this.stringSeq(cfg.get(RECORD_KEY_FIELDS_KEY), RECORD_KEY_FIELDS_KEY)
    if (keys.isEmpty) {
      throw new PuddingException(
        s"iceberg cdc streaming requires $RECORD_KEY_FIELDS_KEY")
    }
    val extensions = dataFrame.sparkSession.conf.get("spark.sql.extensions", "")
    if (!extensions.contains("IcebergSparkSessionExtensions")) {
      throw new PuddingException(
        "iceberg cdc streaming requires Spark SQL MERGE support; configure " +
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    }
    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        this.mergeCdcBatch(batch, table, keys, opColumn, cfg)
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode("append")
    val query = StreamingTrigger.applyTo(writer, context.trigger).start()
    StreamingCheckpoint.awaitTermination(
      query, cfg, context.trigger, Some(checkpoint))
  }

  /**
   * Upsert full rows without an operation column. The batch must contain at least one key field
   * and a deterministic ordering field; duplicate keys are reduced to their latest row before
   * the MERGE. Deletes require the CDC operation-column path.
   */
  private def streamUpsert(
      dataFrame: DataFrame,
      context: SinkContext,
      cfg: Map[String, Any],
      table: String,
      checkpoint: String): Unit = {
    val keys = this.stringSeq(cfg.get(RECORD_KEY_FIELDS_KEY), RECORD_KEY_FIELDS_KEY)
    if (keys.isEmpty) {
      throw new PuddingException(
        s"iceberg streaming upsert requires $RECORD_KEY_FIELDS_KEY")
    }
    val extensions = dataFrame.sparkSession.conf.get("spark.sql.extensions", "")
    if (!extensions.contains("IcebergSparkSessionExtensions")) {
      throw new PuddingException(
        "iceberg streaming upsert requires Spark SQL MERGE support; configure " +
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    }
    val orderFields = StreamingCdc.resolveOrderFields(
      dataFrame, StreamingCdc.orderFields(cfg))
    if (orderFields.isEmpty) {
      throw new PuddingException(
        "iceberg streaming upsert requires cdcOrderFields or Kafka ordering metadata")
    }
    val writer = dataFrame.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val ordered = IncrementalFrames.latestByKey(batch, keys, orderFields)
        val changes = StreamingCdc.dropInternal(ordered)
        val targetColumns = this.targetColumns(batch, table)
        val missing = targetColumns.filterNot(changes.columns.contains)
        if (this.hasRows(changes) && missing.nonEmpty) {
          throw new PuddingException(
            s"iceberg streaming upsert rows are missing target columns: " +
              missing.mkString(", "))
        }
        if (this.hasRows(changes)) {
          this.mergeChanges(batch.sparkSession, table, changes, keys, targetColumns)
        }
      }
      .option(CHECKPOINT_LOCATION_KEY, checkpoint)
      .outputMode("append")
    val query = StreamingTrigger.applyTo(writer, context.trigger).start()
    StreamingCheckpoint.awaitTermination(
      query, cfg, context.trigger, Some(checkpoint))
  }

  private def targetColumns(dataFrame: DataFrame, table: String): Seq[String] = {
    dataFrame.sparkSession.sql(s"SELECT * FROM $table LIMIT 0").schema.fieldNames.toSeq
  }

  private def mergeCdcBatch(
      batch: DataFrame,
      table: String,
      keys: Seq[String],
      opColumn: String,
      cfg: Map[String, Any]): Unit = {
    val sparkSession = batch.sparkSession
    val targetColumns = sparkSession.sql(s"SELECT * FROM $table LIMIT 0")
      .schema.fieldNames.toSeq
    val missingKeys = keys.filterNot(targetColumns.contains)
    if (missingKeys.nonEmpty) {
      throw new PuddingException(
        s"iceberg cdc keyFields are not columns of $table: ${missingKeys.mkString(", ")}")
    }
    val prepared = StreamingCdc.prepare(
      batch, keys, opColumn, StreamingCdc.orderFields(cfg))
    val op = col(opColumn)
    val deletes = StreamingCdc.dropInternal(prepared.filter(op === "D"), Seq(opColumn))
    val changes = StreamingCdc.dropInternal(prepared.filter(op =!= "D"), Seq(opColumn))
    val missingColumns = targetColumns.filterNot(changes.columns.contains)
    if (this.hasRows(changes) && missingColumns.nonEmpty) {
      throw new PuddingException(
        s"iceberg cdc update/insert rows are missing target columns: " +
          missingColumns.mkString(", "))
    }

    if (this.hasRows(deletes)) {
      this.mergeDeletes(sparkSession, table, deletes, keys)
    }
    if (this.hasRows(changes)) {
      this.mergeChanges(sparkSession, table, changes, keys, targetColumns)
    }
  }

  private def mergeDeletes(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String,
      deletes: DataFrame,
      keys: Seq[String]): Unit = {
    this.requireNonNullKeys(deletes, keys)
    val view = this.temporaryView(deletes)
    try {
      val condition = keys.map(key => s"t.${this.quote(key)} = s.${this.quote(key)}")
        .mkString(" AND ")
      sparkSession.sql(
        s"MERGE INTO $table t USING $view s ON $condition WHEN MATCHED THEN DELETE")
    } finally {
      sparkSession.catalog.dropTempView(view)
    }
  }

  private def mergeChanges(
      sparkSession: org.apache.spark.sql.SparkSession,
      table: String,
      changes: DataFrame,
      keys: Seq[String],
      targetColumns: Seq[String]): Unit = {
    this.requireNonNullKeys(changes, keys)
    val view = this.temporaryView(changes)
    try {
      val condition = keys.map(key => s"t.${this.quote(key)} = s.${this.quote(key)}")
        .mkString(" AND ")
      val updates = targetColumns
        .map(column => s"t.${this.quote(column)} = s.${this.quote(column)}")
        .mkString(", ")
      val columns = targetColumns.map(this.quote).mkString(", ")
      val values = targetColumns.map(column => s"s.${this.quote(column)}").mkString(", ")
      sparkSession.sql(
        s"""MERGE INTO $table t USING $view s ON $condition
           |WHEN MATCHED THEN UPDATE SET $updates
           |WHEN NOT MATCHED THEN INSERT ($columns) VALUES ($values)""".stripMargin)
    } finally {
      sparkSession.catalog.dropTempView(view)
    }
  }

  private def requireNonNullKeys(dataFrame: DataFrame, keys: Seq[String]): Unit = {
    if (keys.nonEmpty) {
      val nullKey = keys.map(key => col(key).isNull).reduce(_ || _)
      if (dataFrame.filter(nullKey).limit(1).count() > 0) {
        throw new PuddingException(
          s"iceberg merge rows contain a null key: ${keys.mkString(", ")}")
      }
    }
  }

  private def temporaryView(dataFrame: DataFrame): String = {
    val name = "_pudding_stream_" + UUID.randomUUID().toString.replace("-", "")
    dataFrame.createOrReplaceTempView(name)
    name
  }

  private def quote(column: String): String =
    s"`${column.replace("`", "``")}`"

  private def hasRows(dataFrame: DataFrame): Boolean =
    dataFrame.limit(1).count() > 0

  private def streamingEnabled(
      cfg: Map[String, Any],
      executionMode: Option[ExecutionMode]): Boolean = {
    val configured = Seq(IS_STREAMING_KEY, STREAM_WRITE_KEY, STREAM_WRITER_KEY)
      .iterator
      .flatMap(key => this.booleanValue(cfg, key))
      .exists(_ == true)
    this.effectiveStreaming(configured, executionMode)
  }

  private def booleanValue(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case other => throw new PuddingException(s"$key must be true or false, got '$other'")
        }
      case value =>
        throw new PuddingException(s"$key must be a boolean, got ${value.getClass.getSimpleName}")
    }
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def stringSeq(value: Option[Any], key: String): Seq[String] = value.filter(_ != null) match {
    case Some(text: String) =>
      text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    case Some(seq: Seq[_]) =>
      seq.map(item => this.requiredText(item, key))
    case Some(array: Array[_]) =>
      array.map(item => this.requiredText(item, key)).toSeq
    case Some(list: java.util.List[_]) =>
      list.asScala.map(item => this.requiredText(item, key)).toSeq
    case Some(_) =>
      throw new PuddingException(s"$key must be a string or a list of strings!")
    case None => Seq.empty
  }

  private def requiredText(value: Any, key: String): String = {
    if (value == null || value.toString.trim.isEmpty) {
      throw new PuddingException(s"$key can not contain an empty value!")
    }
    value.toString.trim
  }

  private def parseMode(mode: String): String = {
    val normalized = mode.trim.toLowerCase(Locale.ROOT)
    normalized match {
      case "append" | "overwrite" | "ignore" | "error" | "errorifexists" => normalized
      case _ => throw new PuddingException(s"unsupported save mode: $mode")
    }
  }
}

private[sink] object IcebergSink {

  /**
   * Qualify a table with the Spark catalog name when the sink references a catalog. A named
   * Iceberg catalog needs catalog.database.table; the session catalog can keep db.table.
   */
  def qualify(table: String, sparkCatalogName: String, catalogName: String): String = {
    val parts = table.split("\\.", -1).map(_.trim)
    if (parts.exists(_.isEmpty)) {
      throw new PuddingException(s"invalid Iceberg table identifier: $table")
    }
    if (parts.length >= 3 || sparkCatalogName == "spark_catalog") {
      table
    } else if (parts.length == 2) {
      s"$sparkCatalogName.$table"
    } else {
      throw new PuddingException(
        s"table '$table' must be database.table when catalog '$catalogName' is configured")
    }
  }
}
