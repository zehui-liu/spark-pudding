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

package org.pudding.source

import scala.collection.mutable.ListBuffer

import org.apache.iceberg.{DataOperations, Snapshot, Table}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, lit, lower, udf, when}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{
  DataSource,
  IncrementalRead,
  IncrementalRequest,
  IncrementalSemantics,
  IncrementalSource,
  SourceContext
}
import org.pudding.core.incremental.IncrementalFrames

/**
 * Reads an Apache Iceberg table through a catalog or a direct path, including snapshot, timestamp,
 * branch and tag selectors.
 */
class IcebergSource extends DataSource with IncrementalSource {

  private val TABLE_KEY = "table"

  private val PATH_KEY = "path"

  private val CATALOG_KEY = "catalog"

  private val OPTIONS_KEY = "options"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  private val FILTER_EXPR_KEY = "filterExpr"

  private val SNAPSHOT_ID_KEY = "snapshotId"

  private val AS_OF_TIMESTAMP_KEY = "asOfTimestamp"

  private val BRANCH_KEY = "branch"

  private val TAG_KEY = "tag"

  private val STREAM_READ_KEY = "streamRead"

  private val IS_STREAMING_KEY = "isStreaming"

  override def identifier: String = "iceberg-source"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportsLakeIncremental: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("iceberg")

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(
      IncrementalSemantics.Append,
      IncrementalSemantics.Upsert,
      IncrementalSemantics.Cdc)

  override def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "iceberg") {
        throw new PuddingException(
          s"iceberg-source requires an iceberg catalog, but got: ${catalog.`type`}")
      }
    }
    val tableIdentifier = this.optionalString(cfg, TABLE_KEY)
      .orElse(this.optionalString(cfg, PATH_KEY))
      .getOrElse {
        throw new PuddingException(
          "Either 'table' or 'path' parameter must be specified for Iceberg source!")
      }
    val configuredCatalogName = this.optionalString(cfg, CATALOG_KEY)
      .getOrElse("spark_catalog")
    val catalogName = context.catalog.map(_.sparkCatalogName).getOrElse(configuredCatalogName)
    val fullTableIdentifier = if (cfg.contains(TABLE_KEY)) {
      IcebergSource.qualify(
        tableIdentifier, catalogName, context.catalog.isDefined, configuredCatalogName)
    } else {
      tableIdentifier
    }
    val table = this.loadTable(sparkSession, context, cfg, fullTableIdentifier)

    val end = request.end.map { value =>
      try value.toLong catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"iceberg snapshot cursor must be a number: $value")
      }
    }.getOrElse {
      val current = table.currentSnapshot()
      if (current == null) {
        throw new PuddingException("iceberg table has no snapshot")
      }
      current.snapshotId()
    }
    val start = request.start.map { value =>
      try value.toLong catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"iceberg snapshot cursor must be a number: $value")
      }
    }.orElse {
      request.initial match {
        case "latest" => Some(end)
        case "full" => None
        case _ =>
          throw new PuddingException(
            "iceberg incremental read requires incremental.start, initial=latest or initial=full")
      }
    }
    val fullRead = start.isEmpty
    start.foreach { from =>
      this.validateSnapshotRange(table, from, end)
      if (request.semantics == IncrementalSemantics.Append) {
        this.validateAppendOnly(table, from, end)
      }
    }

    val reader = sparkSession.read.format("iceberg")
      .options(this.getStringOptions(cfg, OPTIONS_KEY))
    val changesRead = request.semantics == IncrementalSemantics.Cdc ||
      request.semantics == IncrementalSemantics.Upsert
    val dataframe = if (fullRead) {
      reader.option("snapshot-id", end.toString).load(fullTableIdentifier)
    } else if (changesRead) {
      if (!cfg.contains(TABLE_KEY)) {
        throw new PuddingException(
          s"iceberg ${request.semantics.name} incremental reads require a catalog table; " +
            "the .changes metadata table is not available in path mode")
      }
      if (request.semantics == IncrementalSemantics.Upsert &&
        request.keyFields.isEmpty) {
        throw new PuddingException("iceberg upsert incremental read requires keyFields")
      }
      reader
        .option("start-snapshot-id", start.get.toString)
        .option("end-snapshot-id", end.toString)
        .load(s"$fullTableIdentifier.changes")
    } else {
      reader
        .option("start-snapshot-id", start.get.toString)
        .option("end-snapshot-id", end.toString)
        .load(fullTableIdentifier)
    }
    val (changeData, sequenceColumn) =
      if (changesRead && !fullRead) {
        this.withChangeSequence(dataframe, table, start.get, end)
      } else {
        dataframe -> "_pudding_change_sequence"
      }
    val (normalized, orderFields) = request.semantics match {
      case IncrementalSemantics.Cdc if fullRead =>
        (changeData.withColumn(request.opColumn, lit("I")), Seq.empty[String])
      case IncrementalSemantics.Cdc =>
        this.normalizeCdc(changeData, request.opColumn, sequenceColumn)
      case IncrementalSemantics.Upsert if fullRead =>
        (changeData, Seq.empty[String])
      case IncrementalSemantics.Upsert =>
        (this.normalizeUpsert(changeData, request.keyFields, sequenceColumn),
          Seq.empty[String])
      case _ => (changeData, Seq.empty[String])
    }
    val filtered = this.optionalString(cfg, FILTER_EXPR_KEY)
      .map(expression => normalized.filter(expression)).getOrElse(normalized)
    val preserve = request.semantics match {
      case IncrementalSemantics.Cdc => Seq(request.opColumn) ++ orderFields
      case IncrementalSemantics.Upsert => request.keyFields
      case _ => Seq.empty[String]
    }
    val result = this.resolveSchema(filtered, cfg, preserve)

    IncrementalRead(
      dataFrame = result,
      semantics = request.semantics,
      keyFields = request.keyFields,
      opColumn = request.opColumn,
      fromCursor = start.map(_.toString),
      toCursor = end.toString,
      cursorType = "snapshot_id",
      orderFields = orderFields)
  }

  /**
   * Iceberg snapshot ids are random, so ordering must use the snapshot sequence number.
   */
  private def validateSnapshotRange(table: Table, start: Long, end: Long): Unit = {
    this.snapshotsBetween(table, start, end)
  }

  /**
   * Iceberg's incremental append scan only returns newly added files. Running it across an
   * overwrite or delete snapshot would silently miss changed rows, so reject that plan and ask
   * for cdc semantics instead.
   */
  private def validateAppendOnly(table: Table, start: Long, end: Long): Unit = {
    val nonAppend = this.snapshotsBetween(table, start, end)
      .filterNot(_.operation() == DataOperations.APPEND)
      .map(snapshot => s"${snapshot.snapshotId()}:${snapshot.operation()}")
    if (nonAppend.nonEmpty) {
      throw new PuddingException(
        "iceberg append semantics require an append-only snapshot range, but found " +
          s"non-append snapshots: ${nonAppend.mkString(", ")}; use cdc semantics instead")
    }
  }

  /**
   * Walk end's ancestry up to (but excluding) start. This ignores unrelated branch snapshots and
   * also proves that start is an ancestor of end.
   */
  private def snapshotsBetween(table: Table, start: Long, end: Long): Seq[Snapshot] = {
    val startSnapshot = table.snapshot(start)
    val endSnapshot = table.snapshot(end)
    if (startSnapshot == null || endSnapshot == null) {
      throw new PuddingException(
        s"iceberg incremental snapshot range $start..$end contains an unknown snapshot")
    }
    val snapshots = ListBuffer[Snapshot]()
    var current: Snapshot = endSnapshot
    while (current != null && current.snapshotId() != start) {
      snapshots += current
      val parentId = current.parentId()
      current = if (parentId == null) null else table.snapshot(parentId)
    }
    if (current == null) {
      throw new PuddingException(
        s"iceberg incremental start $start is not an ancestor of end $end")
    }
    snapshots.toSeq.reverse
  }

  private def loadTable(
      sparkSession: SparkSession,
      context: SourceContext,
      cfg: Map[String, Any],
      fullTableIdentifier: String): Table = {
    if (!cfg.contains(TABLE_KEY)) {
      return new HadoopTables(sparkSession.sessionState.newHadoopConf()).load(fullTableIdentifier)
    }
    val configuredCatalogName = cfg.get(CATALOG_KEY)
      .map(_.toString).filter(_.trim.nonEmpty).getOrElse("spark_catalog")
    val configuredCatalog = context.catalog.map(_.sparkCatalogName).getOrElse(configuredCatalogName)
    val parts = fullTableIdentifier.split("\\.").map(_.trim).filter(_.nonEmpty)
    // A qualified table starts with its Spark catalog name, which is not part of the identifier
    // passed to TableCatalog.loadTable. A two-part name under spark_catalog or a legacy
    // catalog config is database.table.
    val catalogName =
      if (parts.length >= 3) parts.head
      else configuredCatalog
    val prefix = s"$catalogName."
    val stripped =
      if (fullTableIdentifier.startsWith(prefix)) fullTableIdentifier.substring(prefix.length)
      else fullTableIdentifier
    val tableParts = stripped.split("\\.").map(_.trim).filter(_.nonEmpty)
    val namespaces = if (tableParts.length > 1) tableParts.dropRight(1) else Array("default")
    val identifier = Identifier.of(namespaces, tableParts.last)
    val catalog = sparkSession.sessionState.catalogManager.catalog(catalogName)
      .asInstanceOf[TableCatalog]
    catalog.loadTable(identifier).asInstanceOf[SparkTable].table()
  }

  /**
   * Convert an Iceberg changelog range to upsert rows. Only the latest INSERT/UPDATE_AFTER row
   * per key survives. A DELETE can not be represented by upsert semantics, so it fails the read
   * instead of being silently lost; callers that need deletes must use cdc semantics.
   */
  private def normalizeUpsert(
      dataFrame: DataFrame,
      keyFields: Seq[String],
      sequenceColumn: String): DataFrame = {
    if (keyFields.isEmpty) {
      throw new PuddingException("iceberg upsert incremental read requires keyFields")
    }
    val changeColumn = dataFrame.columns.find(_.equalsIgnoreCase("_change_type")).getOrElse {
      throw new PuddingException("iceberg changelog result does not contain _change_type")
    }
    val change = lower(col(changeColumn))
    val filtered = dataFrame.filter(change =!= "update_before")
    val orderFields = Seq(sequenceColumn, "_change_ordinal")
      .filter(dataFrame.columns.contains)
    if (orderFields.isEmpty) {
      throw new PuddingException(
        "iceberg upsert incremental read requires change ordering metadata")
    }
    // Iceberg can encode an update as a delete followed by an insert in the same commit and
    // ordinal. Prefer the replacement row when the change ordering ties.
    val priorityColumn = this.uniqueColumn(dataFrame, "_pudding_change_priority")
    val prioritized = filtered.withColumn(
      priorityColumn,
      when(change.isin("insert", "update_after"), lit(2)).otherwise(lit(1)))
    val latest = IncrementalFrames.latestByKey(
      prioritized, keyFields, orderFields :+ priorityColumn)
    // A key whose last change is a delete has no replacement row, so upsert can not represent it.
    if (latest.filter(change === "delete").limit(1).count() > 0) {
      throw new PuddingException(
        "iceberg upsert incremental read encountered a key deleted without a replacement; " +
          "use cdc semantics")
    }
    val metadata = latest.columns.filter { column =>
      column.equalsIgnoreCase(changeColumn) ||
        column.equalsIgnoreCase("_change_ordinal") ||
        column.equalsIgnoreCase("_commit_snapshot_id") ||
        column.equalsIgnoreCase(sequenceColumn) ||
        column.equalsIgnoreCase(priorityColumn)
    }
    latest.drop(metadata: _*)
  }

  private def uniqueColumn(dataFrame: DataFrame, base: String): String = {
    val names = dataFrame.columns.map(_.toLowerCase(java.util.Locale.ROOT)).toSet
    var candidate = base
    var suffix = 0
    while (names.contains(candidate.toLowerCase(java.util.Locale.ROOT))) {
      suffix += 1
      candidate = s"$base$suffix"
    }
    candidate
  }

  private def normalizeCdc(
      dataFrame: DataFrame,
      opColumn: String,
      sequenceColumn: String): (DataFrame, Seq[String]) = {
    val changeColumn = dataFrame.columns.find(_.equalsIgnoreCase("_change_type")).getOrElse {
      throw new PuddingException("iceberg changelog result does not contain _change_type")
    }
    val change = lower(col(changeColumn))
    val priorityColumn = this.uniqueColumn(dataFrame, "_pudding_change_priority")
    val normalized = dataFrame
      .filter(change =!= "update_before")
      .withColumn(priorityColumn,
        when(change.isin("insert", "update_after"), lit(2)).otherwise(lit(1)))
      .withColumn(opColumn,
        when(change === "insert", lit("I"))
          .when(change === "update_after", lit("U"))
          .when(change === "delete", lit("D"))
          .otherwise(lit("?")))
    // The priority column is added by `normalized`, so derive the ordering from the normalized
    // frame. Deriving it from `dataFrame` silently dropped the insert/delete tie-breaker.
    val orderFields = Seq(sequenceColumn, "_change_ordinal", priorityColumn)
      .filter(normalized.columns.contains)
    val metadata = normalized.columns.filter(column =>
      column.equalsIgnoreCase(changeColumn) ||
        (column.equalsIgnoreCase("_change_ordinal") && !orderFields.contains(column)) ||
        (column.equalsIgnoreCase("_commit_snapshot_id") && !orderFields.contains(column)))
    (normalized.drop(metadata: _*), orderFields)
  }

  /**
   * Iceberg snapshot ids are random. Map each changelog row's commit snapshot id to its snapshot
   * sequence number, which is the only monotonic ordering value across commits.
   */
  private def withChangeSequence(
      dataFrame: DataFrame,
      table: Table,
      start: Long,
      end: Long): (DataFrame, String) = {
    val commitColumn = dataFrame.columns.find(_.equalsIgnoreCase("_commit_snapshot_id"))
      .getOrElse {
        throw new PuddingException(
          "iceberg changelog result does not contain _commit_snapshot_id")
      }
    val sequenceColumn = this.uniqueColumn(dataFrame, "_pudding_change_sequence")
    val sequenceById = this.snapshotsBetween(table, start, end)
      .map(snapshot => snapshot.snapshotId() -> snapshot.sequenceNumber())
      .toMap
    if (sequenceById.isEmpty) {
      (dataFrame.withColumn(sequenceColumn, lit(0L)), sequenceColumn)
    } else {
      // Look up a broadcast mapping instead of nesting one `when` per snapshot. A large range
      // could otherwise create a very deep expression tree.
      val mapping = dataFrame.sparkSession.sparkContext.broadcast(sequenceById)
      val lookupSequence = udf((snapshotId: java.lang.Long) => {
        if (snapshotId == null) {
          0L
        } else {
          mapping.value.getOrElse(snapshotId.longValue(), 0L)
        }
      })
      val sequenced = dataFrame.withColumn(
        sequenceColumn,
        lookupSequence(col(commitColumn).cast("long")))
      (sequenced, sequenceColumn)
    }
  }

  private def resolveSchema(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      preserve: Seq[String]): DataFrame = {
    if (!booleanValue(cfg, FORCE_CONVERT_SCHEMA_KEY)) {
      dataFrame
    } else {
      val schemas = this.getStringOptions(cfg, SCHEMA_KEY)
      if (schemas.isEmpty) {
        throw new PuddingException(
          s"$FORCE_CONVERT_SCHEMA_KEY set true, $SCHEMA_KEY must have a value")
      }
      val selected = this.getColumns(schemas)
      val extra = preserve
        .filterNot(column => schemas.keys.exists(_.equalsIgnoreCase(column)))
        .map(column => col(column))
      dataFrame.select((selected ++ extra).toArray: _*)
    }
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => Some(value.trim)
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def optionalBoolean(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(java.util.Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ =>
            throw new PuddingException(s"$key must be true or false, but got: $value")
        }
      case value =>
        throw new PuddingException(s"$key must be boolean, but got: $value")
    }
  }

  private def booleanValue(cfg: Map[String, Any], key: String): Boolean = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: Boolean) => value
      case Some(value: String) =>
        value.trim.toLowerCase(java.util.Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ =>
            throw new PuddingException(s"$key must be true or false, but got: $value")
        }
      case Some(value) =>
        throw new PuddingException(s"$key must be boolean, but got: $value")
      case None => false
    }
  }

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val cfg = context.config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val resolvedCatalog = context.catalog.map { catalog =>
      if (catalog.`type` != "iceberg") {
        throw new PuddingException(
          s"iceberg-source requires an iceberg catalog, but got: ${catalog.`type`}")
      }
      catalog
    }

    // A catalog-backed session has the Iceberg extension installed by CatalogManager. Path-only
    // reads use the Iceberg data source and do not require a SQL extension.
    val tableIdentifier = this.optionalString(cfg, TABLE_KEY)
      .orElse(this.optionalString(cfg, PATH_KEY))
      .getOrElse {
        throw new PuddingException(
          "Either 'table' or 'path' parameter must be specified for Iceberg source!")
      }

    // The inline catalog key remains a fallback when no catalog reference is configured.
    val configuredCatalogName = this.optionalString(cfg, CATALOG_KEY)
      .getOrElse("spark_catalog")
    val catalogName = resolvedCatalog
      .map(_.sparkCatalogName)
      .getOrElse(configuredCatalogName)
    val fullTableIdentifier = if (cfg.contains(TABLE_KEY)) {
      IcebergSource.qualify(
        tableIdentifier, catalogName, resolvedCatalog.isDefined, configuredCatalogName)
    } else {
      tableIdentifier
    }

    val options = this.getStringOptions(cfg, OPTIONS_KEY)
    val configuredStreaming = Seq(STREAM_READ_KEY, IS_STREAMING_KEY)
      .flatMap(key => this.optionalBoolean(cfg, key))
      .exists(identity)
    val streaming = this.effectiveStreaming(configuredStreaming, context.executionMode)

    val originalDataFrame = if (streaming) {
      val batchOnly = Seq(SNAPSHOT_ID_KEY, AS_OF_TIMESTAMP_KEY, TAG_KEY)
        .filter(key => cfg.get(key).filter(_ != null).isDefined)
      if (batchOnly.nonEmpty) {
        throw new PuddingException(
          "iceberg streaming does not support batch snapshot selectors: " +
            batchOnly.mkString(", ") +
            "; use options stream-from-timestamp or streaming-snapshot options")
      }
      var streamOptions = options
      this.optionalString(cfg, BRANCH_KEY)
        .foreach(branch => streamOptions += "branch" -> branch)
      sparkSession.readStream
        .format("iceberg")
        .options(streamOptions)
        .load(fullTableIdentifier)
    } else {
      // Read the base DataFrame
      val dataFrameReader = sparkSession.read.format("iceberg")

      // Apply options to the reader
      val readerWithOptions = options.foldLeft(dataFrameReader) {
        case (reader, (key, value)) => reader.option(key, value)
      }

      val readerWithSnapshotId = cfg.get(SNAPSHOT_ID_KEY) match {
        case Some(snapshotId) =>
          readerWithOptions.option("snapshot-id", snapshotId.toString)
        case None => readerWithOptions
      }

      val readerWithTimestamp = cfg.get(AS_OF_TIMESTAMP_KEY) match {
        case Some(timestamp) =>
          readerWithSnapshotId.option("as-of-timestamp", timestamp.toString)
        case None => readerWithSnapshotId
      }

      val readerWithBranch = this.optionalString(cfg, BRANCH_KEY) match {
        case Some(branch) =>
          readerWithTimestamp.option("branch", branch)
        case None => readerWithTimestamp
      }

      val finalReader = this.optionalString(cfg, TAG_KEY) match {
        case Some(tag) =>
          readerWithBranch.option("tag", tag)
        case None => readerWithBranch
      }

      finalReader.load(fullTableIdentifier)
    }

    // Apply filter expression if provided
    val filteredDataFrame = this.optionalString(cfg, FILTER_EXPR_KEY) match {
      case Some(filterExpr) => originalDataFrame.filter(filterExpr)
      case None => originalDataFrame
    }

    this.resolveSchema(filteredDataFrame, cfg, Seq.empty)
  }
}

private[source] object IcebergSource {

  /**
   * Build the fully qualified table identifier. A named Spark catalog needs three parts
   * (catalog.database.table), except when the Iceberg catalog replaces spark_catalog.
   */
  def qualify(
      table: String,
      catalogName: String,
      catalogConfigured: Boolean,
      legacyCatalogName: String): String = {
    val parts = table.split("\\.", -1).map(_.trim)
    if (parts.exists(_.isEmpty)) {
      throw new PuddingException(s"invalid Iceberg table identifier: $table")
    }
    if (parts.length >= 3 || catalogName == "spark_catalog") {
      table
    } else if (parts.length == 2) {
      s"$catalogName.$table"
    } else if (catalogConfigured) {
      throw new PuddingException(
        s"table '$table' must be database.table when catalog '$catalogName' is configured")
    } else {
      // Legacy behavior for a named catalog configured inside the source config.
      s"$legacyCatalogName.$table"
    }
  }

}
