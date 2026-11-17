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

package org.pudding.connector.jdbc

import java.sql.Connection
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import org.pudding.core.PuddingException
import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.definition.{
  IncrementalSemantics,
  IncrementalWrite,
  SinkContext
}
import org.pudding.core.incremental.{IncrementalFrames, StreamingCdc}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Structured-streaming support shared by `jdbc-sink` and `mysql-sink`.
 *
 * `append` writes each microbatch through Spark's JDBC writer. `update` treats every microbatch
 * as complete rows and merges them by `recordKeyFields`:
 *
 *  1. the batch is aligned to the target column order and checked for null or duplicate keys;
 *  2. the rows are written to a private staging table;
 *  3. one database transaction merges the staging rows into the target using `MERGE` (PostgreSQL
 *     and H2) or a keyed `UPDATE ... JOIN` plus `INSERT ... WHERE NOT EXISTS` (MySQL and
 *     MariaDB). The MySQL form matches only the configured keys, unlike `ON DUPLICATE KEY
 *     UPDATE`, which also follows unrelated unique indexes.
 *
 * The target table must exist and have a primary key or unique index exactly matching
 * `recordKeyFields`. The merge is idempotent for full rows, so a checkpoint replay after a
 * committed merge does not duplicate data. Rows are not deleted; CDC operation columns are not
 * supported.
 */
object JdbcStreaming {

  val CheckpointLocationKey = StreamingCheckpoint.LocationKey

  val RecordKeyFieldsKey = "recordKeyFields"

  val OutputModeKey = StreamingOutputMode.Key

  private val StreamingKeys = Seq("isStreaming", "streamWrite", "streamWriter")

  private val UrlKey = "url"

  private val UserKey = "user"

  private val PasswordKey = "password"

  private val TableKey = "dbtable"

  private val ModeKey = "mode"

  private val SupportedOutputModes =
    Set(StreamingOutputMode.Append, StreamingOutputMode.Update)

  /**
   * Whether a direct connector call should use the streaming writer. BuildJob always supplies an
   * explicit execution mode, which is authoritative over connector-level flags.
   */
  def streamingEnabled(
      cfg: Map[String, Any],
      executionMode: Option[ExecutionMode]): Boolean = {
    val configured = StreamingKeys.iterator
      .flatMap(key => this.booleanOption(cfg, key))
      .exists(identity)
    executionMode.map(_ == ExecutionMode.Streaming).getOrElse(configured)
  }

  /**
   * Start one checkpoint-bound streaming write. `options` are the Spark JDBC options built by
   * the sink and include the connection and credentials; they are never logged here.
   */
  def write(
      dataFrame: DataFrame,
      context: SinkContext,
      options: Map[String, String],
      sinkType: String): Unit = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    this.validateMode(cfg)
    val checkpoint = this.checkpoint(cfg)
    val outputMode = StreamingOutputMode.resolve(
      Some(cfg), sinkType, SupportedOutputModes)
    this.rejectContinuousTrigger(context.trigger, sinkType)
    StreamingCheckpoint.validate(
      cfg, context.trigger, Some(checkpoint), context.stateContract)

    val writer = StreamingCdc.opColumn(cfg) match {
      case Some(opColumn) =>
        if (outputMode == StreamingOutputMode.Update) {
          throw new PuddingException(
            "JDBC streaming cdc applies deletes and upserts itself; use outputMode=append, " +
              "not update")
        }
        this.requireDialect(options)
        val keys = this.configuredKeys(cfg)
        if (keys.isEmpty) {
          throw new PuddingException(
            s"JDBC streaming cdc requires $RecordKeyFieldsKey")
        }
        val target = this.inspectTarget(options)
        val canonical = this.validateTargetKeys(target, keys)
        this.validateBatchSchema(dataFrame, target)
        val orderFields = StreamingCdc.orderFields(cfg)
        dataFrame.writeStream
          .foreachBatch { (batch: DataFrame, _: Long) =>
            this.writeCdcFrame(batch, options, target, canonical, orderFields, opColumn)
          }
          .option(CheckpointLocationKey, checkpoint)
          .outputMode(StreamingOutputMode.Append)
      case None =>
        outputMode match {
          case StreamingOutputMode.Update =>
            this.requireDialect(options)
            val configuredKeys = this.configuredKeys(cfg)
            val target = this.inspectTarget(options)
            val keys = this.validateTargetKeys(target, configuredKeys)
            this.validateBatchSchema(dataFrame, target)
            dataFrame.writeStream
              .foreachBatch { (batch: DataFrame, _: Long) =>
                this.upsertBatch(batch, options, keys, target)
              }
              .option(CheckpointLocationKey, checkpoint)
              .outputMode(StreamingOutputMode.Update)
          case _ =>
            if (cfg.get(RecordKeyFieldsKey).filter(_ != null).isDefined) {
              throw new PuddingException(
                s"$RecordKeyFieldsKey requires $OutputModeKey=${StreamingOutputMode.Update}")
            }
            dataFrame.writeStream
              .foreachBatch { (batch: DataFrame, _: Long) =>
                this.appendBatch(batch, options)
              }
              .option(CheckpointLocationKey, checkpoint)
              .outputMode(StreamingOutputMode.Append)
        }
    }

    val query = StreamingTrigger.applyTo(writer, context.trigger).start()
    StreamingCheckpoint.awaitTermination(
      query, cfg, context.trigger, Some(checkpoint))
  }

  /**
   * Apply one bounded sync-incremental result.
   *
   * `append` writes rows as-is. `upsert` keeps the latest row per key when the source supplied
   * ordering metadata and then merges complete rows. `cdc` applies deletes and upserts in one
   * database transaction after keeping the latest operation per key.
   */
  def writeIncremental(
      dataFrame: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      incremental: IncrementalWrite): Unit = {
    incremental.semantics match {
      case IncrementalSemantics.Append =>
        this.appendBatch(dataFrame, options)
      case IncrementalSemantics.Upsert =>
        this.requireDialect(options)
        val target = this.inspectTarget(options)
        val keys = this.validateTargetKeys(target, incremental.keyFields)
        this.validateBatchSchema(dataFrame, target)
        val orderFields = if (incremental.orderFields.nonEmpty) {
          incremental.orderFields
        } else {
          // A batch source may not expose ordering metadata; honor the sink's cdcOrderFields
          // before falling back to the duplicate-key rejection below.
          StreamingCdc.orderFields(cfg)
        }
        val latest = IncrementalFrames.latestByKey(
          dataFrame, incremental.keyFields, orderFields)
        this.upsertBatch(latest, options, keys, target)
      case IncrementalSemantics.Cdc =>
        this.writeCdc(dataFrame, cfg, options, incremental)
    }
  }

  /**
   * Apply one bounded batch with key-based upsert semantics. This is the operation used by every
   * streaming microbatch and is exposed for integrations that manage their own batches.
   */
  def merge(
      dataFrame: DataFrame,
      options: Map[String, String],
      keys: Seq[String]): Unit = {
    if (keys.isEmpty) {
      throw new PuddingException(s"$RecordKeyFieldsKey can not be empty")
    }
    this.requireDialect(options)
    val target = this.inspectTarget(options)
    val canonicalKeys = this.validateTargetKeys(target, keys)
    this.validateBatchSchema(dataFrame, target)
    this.upsertBatch(dataFrame, options, canonicalKeys, target)
  }

  private def writeCdc(
      batch: DataFrame,
      cfg: Map[String, Any],
      options: Map[String, String],
      incremental: IncrementalWrite): Unit = {
    this.requireDialect(options)
    val target = this.inspectTarget(options)
    val keys = this.validateTargetKeys(target, incremental.keyFields)
    val orderFields =
      if (incremental.orderFields.nonEmpty) incremental.orderFields
      else StreamingCdc.orderFields(cfg)
    this.writeCdcFrame(
      batch, options, target, keys, orderFields, incremental.opColumn)
  }

  /** Apply one I/U/D frame; deletes and upserts commit in one database transaction. */
  private def writeCdcFrame(
      batch: DataFrame,
      options: Map[String, String],
      target: TargetTable,
      keys: Seq[String],
      orderFields: Seq[String],
      opColumn: String): Unit = {
    val prepared = StreamingCdc.prepare(batch, keys, opColumn, orderFields)
    val deletes = prepared
      .filter(col(opColumn) === "D")
      .select(keys.map(key => col(key)): _*)
    val upserts = prepared
      .filter(col(opColumn) =!= "D")
      .drop(opColumn)

    val hasUpserts = this.hasRows(upserts)
    val hasDeletes = this.hasRows(deletes)
    if (!hasUpserts && !hasDeletes) {
      return
    }
    if (hasUpserts) {
      // A null key never matches an existing row in the staging MERGE, so it would be inserted
      // as an orphan instead of updating the intended row. Validate both operation streams.
      this.validateKeys(upserts, keys)
    }
    if (hasDeletes) {
      this.validateKeys(deletes, keys)
    }

    val upsertStaging = if (hasUpserts) {
      Some(JdbcTables.stagingTable(target.table, JdbcTables.StreamingPrefix))
    } else None
    val deleteStaging = if (hasDeletes) {
      Some(JdbcTables.stagingTable(target.table, JdbcTables.StreamingPrefix))
    } else None
    try {
      upsertStaging.foreach { staging =>
        this.createStaging(options, target, staging)
        this.align(upserts, target).write
          .format("jdbc")
          .options(options + (TableKey -> staging))
          .mode("append")
          .save()
      }
      deleteStaging.foreach { staging =>
        this.createKeyStaging(options, target, staging, keys)
        deletes.write
          .format("jdbc")
          .options(options + (TableKey -> staging))
          .mode("append")
          .save()
      }
      this.applyCdcStaging(
        options, target, keys, upsertStaging, deleteStaging)
    } finally {
      upsertStaging.foreach(staging => this.dropStaging(options, staging))
      deleteStaging.foreach(staging => this.dropStaging(options, staging))
    }
  }

  private def applyCdcStaging(
      options: Map[String, String],
      target: TargetTable,
      keys: Seq[String],
      upsertStaging: Option[String],
      deleteStaging: Option[String]): Unit = {
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    val dialect = this.dialect(url)
    JdbcTables.withConnection(url, user, password) { connection =>
      val statements = mutable.ArrayBuffer[String]()
      deleteStaging.foreach { staging =>
        statements ++= this.deleteStatements(connection, target, staging, keys, dialect)
      }
      upsertStaging.foreach { staging =>
        statements ++= this.mergeStatements(connection, target, staging, keys, dialect)
      }
      this.executeTransaction(connection, statements)
    }
  }

  private def createKeyStaging(
      options: Map[String, String],
      target: TargetTable,
      staging: String,
      keys: Seq[String]): Unit = {
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    JdbcTables.withConnection(url, user, password) { connection =>
      val keyColumns = keys
        .map(key => JdbcTables.quoteIdentifier(connection, key))
      JdbcTables.execute(
        connection,
        s"CREATE TABLE ${JdbcTables.quoteTable(connection, staging)} AS " +
          s"SELECT ${keyColumns.mkString(", ")} FROM ${target.quoted} WHERE 1=0")
    }
  }

  private def deleteStatements(
      connection: Connection,
      target: TargetTable,
      staging: String,
      keys: Seq[String],
      dialect: Dialect): Seq[String] = {
    val stagingName = JdbcTables.quoteTable(connection, staging)
    val conditions = keys.map { key =>
      val column = JdbcTables.quoteIdentifier(connection, key)
      s"d.$column = ${target.quoted}.$column"
    }.mkString(" AND ")
    dialect match {
      case MySqlDialect =>
        // MySQL rejects a correlated subquery that reads the target table in DELETE.
        val aliased = keys.map { key =>
          val column = JdbcTables.quoteIdentifier(connection, key)
          s"d.$column = t.$column"
        }.mkString(" AND ")
        Seq(
          s"DELETE t FROM ${target.quoted} AS t JOIN $stagingName AS d ON ($aliased)")
      case PostgreSqlDialect | H2Dialect =>
        Seq(
          s"DELETE FROM ${target.quoted} WHERE EXISTS (" +
            s"SELECT 1 FROM $stagingName AS d WHERE $conditions)")
    }
  }

  private def executeTransaction(
      connection: Connection,
      statements: Seq[String]): Unit = {
    connection.setAutoCommit(false)
    try {
      statements.foreach(statement => JdbcTables.execute(connection, statement))
      connection.commit()
    } catch {
      case NonFatal(error) =>
        try connection.rollback()
        catch {
          case NonFatal(_) => ()
        }
        throw new PuddingException(
          "JDBC CDC write failed; the batch was rolled back: " +
            error.getClass.getSimpleName,
          error)
    } finally {
      try connection.setAutoCommit(true)
      catch {
        case NonFatal(_) => ()
      }
    }
  }

  private def validateMode(cfg: Map[String, Any]): Unit = {
    cfg.get(ModeKey).filter(_ != null).foreach { _ =>
      val mode = JdbcConnectorConfig.saveMode(cfg)
      if (mode != "append") {
        throw new PuddingException(
          s"JDBC streaming supports $ModeKey=append only; use " +
            s"$OutputModeKey=${StreamingOutputMode.Update} for key-based upsert")
      }
    }
  }

  private def checkpoint(cfg: Map[String, Any]): String = {
    cfg.get(CheckpointLocationKey).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) =>
        throw new PuddingException(s"$CheckpointLocationKey can not be empty")
      case Some(_) =>
        throw new PuddingException(s"$CheckpointLocationKey must be a string")
      case None =>
        throw new PuddingException(
          s"$CheckpointLocationKey is required for a JDBC streaming write")
    }
  }

  private def rejectContinuousTrigger(
      trigger: Option[String],
      sinkType: String): Unit = {
    if (trigger.exists(_.trim.toLowerCase(Locale.ROOT).startsWith("continuous"))) {
      throw new PuddingException(
        s"$sinkType streaming does not support the continuous trigger")
    }
  }

  private def configuredKeys(cfg: Map[String, Any]): Seq[String] = {
    val keys = this.stringSeq(cfg.get(RecordKeyFieldsKey), RecordKeyFieldsKey)
    if (keys.isEmpty) {
      throw new PuddingException(
        s"JDBC streaming $OutputModeKey=${StreamingOutputMode.Update} requires " +
          RecordKeyFieldsKey)
    }
    val duplicates = keys.groupBy(_.toLowerCase(Locale.ROOT)).collect {
      case (_, values) if values.size > 1 => values.head
    }
    if (duplicates.nonEmpty) {
      throw new PuddingException(
        s"$RecordKeyFieldsKey contains duplicate columns: ${duplicates.mkString(", ")}")
    }
    keys
  }

  /**
   * Read target columns and unique key definitions. The target must already exist because the
   * upsert path can not safely infer a key or locking contract from an arbitrary DataFrame.
   */
  private def inspectTarget(options: Map[String, String]): TargetTable = {
    JdbcTables.loadDriver(options)
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    val table = JdbcTables.required(options, TableKey)
    try {
      JdbcTables.withConnection(url, user, password) { connection =>
        this.readTarget(connection, table)
      }
    } catch {
      case error: PuddingException => throw error
      case NonFatal(error) =>
        throw new PuddingException(
          "JDBC streaming outputMode=update requires an existing, readable target table: " +
            error.getClass.getSimpleName,
          error)
    }
  }

  private def readTarget(connection: Connection, table: String): TargetTable = {
    val quoted = JdbcTables.quoteTable(connection, table)
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(s"SELECT * FROM $quoted WHERE 1=0")
      try {
        val metadata = result.getMetaData
        if (metadata.getColumnCount == 0) {
          throw new PuddingException("JDBC streaming target table has no columns")
        }
        val columns = (1 to metadata.getColumnCount).map(metadata.getColumnName)
        val catalog = this.optional(metadata.getCatalogName(1))
        val schema = this.optional(metadata.getSchemaName(1))
        val tableName = this.optional(metadata.getTableName(1))
          .getOrElse(table.split("\\.", -1).last.trim)
        val uniqueKeys = this.uniqueKeys(connection, catalog, schema, tableName)
        TargetTable(table, quoted, columns, uniqueKeys)
      } finally {
        result.close()
      }
    } finally {
      statement.close()
    }
  }

  private def uniqueKeys(
      connection: Connection,
      catalog: Option[String],
      schema: Option[String],
      table: String): Seq[Seq[String]] = {
    val keys = mutable.ArrayBuffer[Seq[String]]()
    val primary = mutable.ArrayBuffer[(Int, String)]()
    val primaryKeys = connection.getMetaData.getPrimaryKeys(
      catalog.orNull, schema.orNull, table)
    try {
      while (primaryKeys.next()) {
        primary += ((primaryKeys.getInt("KEY_SEQ"), primaryKeys.getString("COLUMN_NAME")))
      }
    } finally {
      primaryKeys.close()
    }
    if (primary.nonEmpty) {
      keys += primary.sortBy(_._1).map(_._2).toSeq
    }

    val indexes = mutable.LinkedHashMap[String, mutable.ArrayBuffer[String]]()
    val indexInfo = connection.getMetaData.getIndexInfo(
      catalog.orNull, schema.orNull, table, true, false)
    try {
      while (indexInfo.next()) {
        val name = indexInfo.getString("INDEX_NAME")
        val column = indexInfo.getString("COLUMN_NAME")
        if (name != null && column != null && !indexInfo.getBoolean("NON_UNIQUE")) {
          indexes.getOrElseUpdate(name, mutable.ArrayBuffer[String]()) += column
        }
      }
    } finally {
      indexInfo.close()
    }
    keys ++= indexes.values
    keys.map(_.toSeq).distinct
  }

  /**
   * Resolve configured keys to the target's actual column spelling and require a primary key or
   * unique index whose column set matches exactly. MySQL's `ON DUPLICATE KEY UPDATE` silently
   * follows any unique index, so this validation is required for predictable upsert semantics.
   */
  private def validateTargetKeys(
      target: TargetTable,
      keys: Seq[String]): Seq[String] = {
    val canonical = keys.map { key =>
      target.columns.find(_.equalsIgnoreCase(key)).getOrElse {
        throw new PuddingException(
          s"$RecordKeyFieldsKey contains a column that is not in the target table: $key")
      }
    }
    val wanted = canonical.map(_.toLowerCase(Locale.ROOT)).toSet
    val matched = target.uniqueKeys.exists { unique =>
      unique.map(_.toLowerCase(Locale.ROOT)).toSet == wanted
    }
    if (!matched) {
      val available = target.uniqueKeys.map(_.mkString("(", ", ", ")")).mkString(", ")
      throw new PuddingException(
        s"JDBC streaming outputMode=update requires $RecordKeyFieldsKey to match a primary " +
          s"key or unique index on the target table; configured keys: " +
          s"${canonical.mkString(", ")}; target unique keys: " +
          (if (available.isEmpty) "none" else available))
    }
    canonical
  }

  /** The streaming schema must provide every target column; extra internal columns are dropped. */
  private def validateBatchSchema(dataFrame: DataFrame, target: TargetTable): Unit = {
    val missing = target.columns.filterNot { targetColumn =>
      dataFrame.columns.exists(_.equalsIgnoreCase(targetColumn))
    }
    if (missing.nonEmpty) {
      throw new PuddingException(
        "JDBC streaming outputMode=update rows must contain every target column; " +
          s"missing: ${missing.mkString(", ")}")
    }
  }

  private def upsertBatch(
      batch: DataFrame,
      options: Map[String, String],
      keys: Seq[String],
      target: TargetTable): Unit = {
    val changes = this.align(batch, target)
    if (!this.hasRows(changes)) {
      return
    }
    this.validateKeys(changes, keys)
    val staging = JdbcTables.stagingTable(target.table, JdbcTables.StreamingPrefix)
    this.createStaging(options, target, staging)
    try {
      changes.write
        .format("jdbc")
        .options(options + (TableKey -> staging))
        .mode("append")
        .save()
      this.mergeStaging(options, target, staging, keys)
    } finally {
      this.dropStaging(options, staging)
    }
  }

  private def align(batch: DataFrame, target: TargetTable): DataFrame = {
    val cleaned = StreamingCdc.dropInternal(batch)
    val columns = target.columns.map { targetColumn =>
      columnsFor(cleaned, targetColumn) match {
        case Some(batchColumn) => col(batchColumn).as(targetColumn)
        case None =>
          throw new PuddingException(
            s"JDBC streaming upsert rows are missing target column: $targetColumn")
      }
    }
    cleaned.select(columns: _*)
  }

  private def columnsFor(dataFrame: DataFrame, name: String): Option[String] = {
    dataFrame.columns.find(_.equalsIgnoreCase(name))
  }

  private def validateKeys(changes: DataFrame, keys: Seq[String]): Unit = {
    val nullKey = keys.map(key => col(key).isNull).reduce(_ || _)
    if (changes.filter(nullKey).limit(1).count() > 0) {
      throw new PuddingException(
        s"JDBC streaming upsert rows contain a null key; $RecordKeyFieldsKey: " +
          keys.mkString(", "))
    }
    val duplicate = changes
      .groupBy(keys.map(key => col(key)): _*)
      .count()
      .filter(col("count") > 1)
      .limit(1)
      .count() > 0
    if (duplicate) {
      throw new PuddingException(
        "JDBC streaming upsert batch contains duplicate keys; " +
          s"$RecordKeyFieldsKey must uniquely identify rows: ${keys.mkString(", ")}")
    }
  }

  private def hasRows(dataFrame: DataFrame): Boolean =
    dataFrame.limit(1).count() > 0

  private def createStaging(
      options: Map[String, String],
      target: TargetTable,
      staging: String): Unit = {
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    JdbcTables.withConnection(url, user, password) { connection =>
      JdbcTables.execute(
        connection,
        s"CREATE TABLE ${JdbcTables.quoteTable(connection, staging)} AS " +
          s"SELECT * FROM ${target.quoted} WHERE 1=0")
    }
  }

  private def mergeStaging(
      options: Map[String, String],
      target: TargetTable,
      staging: String,
      keys: Seq[String]): Unit = {
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    JdbcTables.withConnection(url, user, password) { connection =>
      this.executeTransaction(
        connection,
        this.mergeStatements(connection, target, staging, keys, this.dialect(url)))
    }
  }

  private def mergeStatements(
      connection: Connection,
      target: TargetTable,
      staging: String,
      keys: Seq[String],
      dialect: Dialect): Seq[String] = {
    val targetName = JdbcTables.quoteTable(connection, target.table)
    val stagingName = JdbcTables.quoteTable(connection, staging)
    val columns = target.columns
      .map(column => JdbcTables.quoteIdentifier(connection, column))
    val keySet = keys.map(_.toLowerCase(Locale.ROOT)).toSet
    val keyColumns = target.columns
      .filter(column => keySet.contains(column.toLowerCase(Locale.ROOT)))
      .map(column => JdbcTables.quoteIdentifier(connection, column))
    val updateColumns = target.columns
      .filterNot(column => keySet.contains(column.toLowerCase(Locale.ROOT)))
      .map(column => JdbcTables.quoteIdentifier(connection, column))
    val assigned = if (updateColumns.nonEmpty) updateColumns else keyColumns.take(1)
    val on = keyColumns.map(column => s"t.$column = s.$column").mkString(" AND ")
    dialect match {
      case PostgreSqlDialect | H2Dialect =>
        Seq(
          s"MERGE INTO $targetName AS t USING $stagingName AS s ON ($on) " +
            s"WHEN MATCHED THEN UPDATE SET " +
            assigned.map(column => s"$column = s.$column").mkString(", ") +
            " WHEN NOT MATCHED THEN INSERT (" + columns.mkString(", ") + ") VALUES (" +
            columns.map(column => s"s.$column").mkString(", ") + ")")
      case MySqlDialect =>
        // ON DUPLICATE KEY UPDATE follows every unique index, so an unrelated unique
        // constraint could update the wrong row. Update and insert explicitly by key
        // instead; both statements share one transaction.
        Seq(
          s"UPDATE $targetName AS t JOIN $stagingName AS s ON ($on) SET " +
            assigned.map(column => s"t.$column = s.$column").mkString(", "),
          s"INSERT INTO $targetName (${columns.mkString(", ")}) SELECT " +
            columns.map(column => s"s.$column").mkString(", ") +
            s" FROM $stagingName AS s WHERE NOT EXISTS (" +
            s"SELECT 1 FROM $targetName AS t WHERE $on)")
    }
  }

  private def dropStaging(options: Map[String, String], staging: String): Unit = {
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    try {
      JdbcTables.withConnection(url, user, password) { connection =>
        JdbcTables.execute(
          connection,
          s"DROP TABLE IF EXISTS ${JdbcTables.quoteTable(connection, staging)}")
      }
    } catch {
      case NonFatal(_) => ()
    }
  }

  private def appendBatch(
      batch: DataFrame,
      options: Map[String, String]): Unit = {
    val changes = StreamingCdc.dropInternal(batch)
    if (this.hasRows(changes)) {
      changes.write
        .format("jdbc")
        .options(options)
        .mode("append")
        .save()
    }
  }

  private sealed trait Dialect

  private case object PostgreSqlDialect extends Dialect

  private case object MySqlDialect extends Dialect

  private case object H2Dialect extends Dialect

  private def requireDialect(options: Map[String, String]): Unit = {
    this.dialect(JdbcTables.required(options, UrlKey))
    ()
  }

  private def dialect(url: String): Dialect = {
    val normalized = url.toLowerCase(Locale.ROOT)
    if (normalized.startsWith("jdbc:postgresql:")) {
      PostgreSqlDialect
    } else if (normalized.startsWith("jdbc:mysql:") ||
      normalized.startsWith("jdbc:mariadb:")) {
      MySqlDialect
    } else if (normalized.startsWith("jdbc:h2:")) {
      H2Dialect
    } else {
      throw new PuddingException(
        "JDBC streaming upsert supports postgresql, mysql, mariadb and h2; " +
          "the configured JDBC dialect is not supported")
    }
  }

  private def optional(value: String): Option[String] =
    Option(value).map(_.trim).filter(_.nonEmpty)

  private def booleanOption(
      cfg: Map[String, Any],
      key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case _ => throw new PuddingException(s"$key must be a boolean")
        }
      case _ => throw new PuddingException(s"$key must be a boolean")
    }
  }

  private def stringSeq(value: Option[Any], key: String): Seq[String] = {
    value.filter(_ != null) match {
      case Some(text: String) =>
        text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      case Some(seq: Seq[_]) =>
        seq.map(item => this.requiredText(item, key))
      case Some(array: Array[_]) =>
        array.map(item => this.requiredText(item, key)).toSeq
      case Some(list: java.util.List[_]) =>
        list.asScala.map(item => this.requiredText(item, key)).toSeq
      case Some(_) =>
        throw new PuddingException(s"$key must be a string or a list of strings")
      case None => Seq.empty
    }
  }

  private def requiredText(value: Any, key: String): String = {
    if (value == null) {
      throw new PuddingException(s"$key can not contain a null value")
    }
    val text = value.toString.trim
    if (text.isEmpty) {
      throw new PuddingException(s"$key can not contain an empty value")
    }
    text
  }

  private case class TargetTable(
      table: String,
      quoted: String,
      columns: Seq[String],
      uniqueKeys: Seq[Seq[String]])
}
