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

package org.pudding.source

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.fs.{FileIO, Path => PaimonPath}
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.{DataTable, FileStoreTableFactory, Table}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, lit, lower, when}

import org.pudding.core.PuddingException
import org.pudding.core.catalog.CatalogTableName
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSource, IncrementalRead, IncrementalRequest, IncrementalSemantics, IncrementalSource, SourceContext}

/**
 * Reads Apache Paimon tables.
 *
 * Catalog mode reads a table from a Paimon catalog and supports batch and streaming reads plus
 * Paimon scan options (snapshot/tag/branch, startup mode, filtering and parallelism). Path mode
 * reads an existing Paimon table directly.
 */
class PaimonSource extends DataSource with IncrementalSource {

  private val TABLE_KEY = "table"

  private val PATH_KEY = "path"

  private val OPTIONS_KEY = "options"

  private val STREAM_READ_KEY = "streamRead"

  private val IS_STREAMING_KEY = "isStreaming"

  private val FILTER_EXPR_KEY = "filterExpr"

  private val NUM_PARTITIONS_KEY = "numPartitions"

  private val SCAN_MODE_KEY = "scanMode"

  private val SNAPSHOT_ID_KEY = "snapshotId"

  private val TAG_NAME_KEY = "tagName"

  private val BRANCH_KEY = "branch"

  private val TIMESTAMP_MILLIS_KEY = "timestampMillis"

  override def identifier: String = "paimon-source"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportsLakeIncremental: Boolean = true

  override def supportedCatalogTypes: Set[String] = Set("paimon")

  override def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append, IncrementalSemantics.Cdc)

  override def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "paimon") {
        throw new PuddingException(
          s"paimon-source requires a paimon catalog, but got: ${catalog.`type`}")
      }
    }
    val table = this.loadPaimonTable(sparkSession, context, cfg)
    val primaryKeys = table.primaryKeys().asScala.toSeq
    if (request.semantics == IncrementalSemantics.Append && primaryKeys.nonEmpty) {
      throw new PuddingException(
        "paimon append incremental semantics require an append-only table; " +
          s"the table has primary keys: ${primaryKeys.mkString(", ")}")
    }
    if (request.semantics == IncrementalSemantics.Cdc) {
      if (primaryKeys.isEmpty) {
        throw new PuddingException(
          "paimon cdc incremental semantics require a primary-key table")
      }
      val configuredKeys = request.keyFields.toSet
      if (configuredKeys != primaryKeys.toSet) {
        throw new PuddingException(
          s"paimon cdc keyFields ${request.keyFields.mkString(", ")} must match the table " +
            s"primary keys ${primaryKeys.mkString(", ")}")
      }
    }

    val end = request.end.map { value =>
      try value.toLong catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"paimon snapshot cursor must be a number: $value")
      }
    }.getOrElse {
      val latest = table.latestSnapshot()
      if (!latest.isPresent) {
        throw new PuddingException("paimon table has no snapshot")
      }
      latest.get().id()
    }
    val start = request.start.map { value =>
      try value.toLong catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"paimon snapshot cursor must be a number: $value")
      }
    }.orElse {
      request.initial match {
        case "latest" => Some(end)
        case "full" => None
        case _ =>
          throw new PuddingException(
            "paimon incremental read requires incremental.start, initial=latest or initial=full")
      }
    }
    val fullRead = start.isEmpty
    if (start.exists(_ > end)) {
      throw new PuddingException(s"paimon incremental start ${start.get} is after end $end")
    }

    val tableName = this.optionalString(cfg, TABLE_KEY)
    val tablePath = this.optionalString(cfg, PATH_KEY)
    val fullTable = tableName.map { name =>
      context.catalog
        .map(catalog => CatalogTableName.qualify(catalog, name))
        .getOrElse(name)
    }

    val userOptions = this.getStringOptions(cfg, OPTIONS_KEY)
    val options = userOptions ++ (if (fullRead) {
      Map(
        "scan.mode" -> "from-snapshot",
        "scan.snapshot-id" -> end.toString)
    } else {
      val cdc = request.semantics == IncrementalSemantics.Cdc
      var incremental = Map(
        "scan.mode" -> "incremental",
        "incremental-between" -> s"${start.get},$end",
        "incremental-between-scan-mode" ->
          (if (cdc) this.changelogScanMode(table) else "delta"))
      if (cdc) {
        incremental += "read.changelog" -> "true"
      }
      incremental
    })

    val cdcIncremental = request.semantics == IncrementalSemantics.Cdc && !fullRead
    val raw = if (cdcIncremental) {
      // read.changelog is implemented by Paimon's path-based SparkSource, which wraps the table
      // in the audit-log system table to expose rowkind. Catalog table scans ignore that option.
      val location = table.asInstanceOf[DataTable].location().toString
      sparkSession.read.format("paimon").options(options).load(location)
    } else {
      tablePath match {
        case Some(path) => sparkSession.read.format("paimon").options(options).load(path)
        case None => sparkSession.read.format("paimon").options(options).table(fullTable.get)
      }
    }
    val normalized = request.semantics match {
      case IncrementalSemantics.Cdc if fullRead =>
        raw.withColumn(request.opColumn, lit("I"))
      case IncrementalSemantics.Cdc =>
        this.normalizeChangelog(raw, request.opColumn)
      case _ => raw
    }
    val filtered = cfg.get(FILTER_EXPR_KEY).map(expression =>
      normalized.filter(expression.toString)).getOrElse(normalized)
    val result = this.numPartitions(cfg) match {
      case Some(partitions) => filtered.repartition(partitions)
      case None => filtered
    }

    IncrementalRead(
      dataFrame = result,
      semantics = request.semantics,
      keyFields = request.keyFields,
      opColumn = request.opColumn,
      fromCursor = start.map(_.toString),
      toCursor = end.toString,
      cursorType = "snapshot_id")
  }

  private def loadPaimonTable(
      sparkSession: SparkSession,
      context: SourceContext,
      cfg: Map[String, Any]): Table = {
    this.optionalString(cfg, PATH_KEY) match {
      case Some(location) =>
        val path = new PaimonPath(location)
        val catalogContext = CatalogContext.create(
          new Options(), sparkSession.sessionState.newHadoopConf())
        val fileIO = FileIO.get(path, catalogContext)
        FileStoreTableFactory.create(fileIO, path)
      case None =>
        val name = this.optionalString(cfg, TABLE_KEY).getOrElse {
          throw new PuddingException(s"paimon-source requires $TABLE_KEY or $PATH_KEY!")
        }
        val fullTable = context.catalog
          .map(catalog => CatalogTableName.qualify(catalog, name))
          .getOrElse(name)
        val configuredCatalogName =
          context.catalog.map(_.sparkCatalogName).getOrElse("spark_catalog")
        val parts = fullTable.split("\\.").map(_.trim).filter(_.nonEmpty)
        // A qualified table starts with its Spark catalog name, which is not part of the
        // identifier passed to TableCatalog.loadTable. A two-part name is database.table.
        val catalogName = if (parts.length >= 3) parts.head else configuredCatalogName
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
    }
  }

  /**
   * Use changelog files when the table produces them; otherwise derive the changelog from the
   * difference between the two snapshots so cdc still works for tables without a producer.
   */
  private def changelogScanMode(table: Table): String = {
    val producer = table.options().getOrDefault("changelog-producer", "none")
    if (producer.equalsIgnoreCase("none")) "diff" else "changelog"
  }

  private def normalizeChangelog(dataFrame: DataFrame, opColumn: String): DataFrame = {
    // `read.changelog` exposes the audit-log system table with a `rowkind` string column;
    // increment-between changelog scans can expose `_row_kind_` instead.
    val kindNames = Set("rowkind", "_row_kind_", "_change_type")
    val kindColumn = dataFrame.columns
      .find(column => kindNames.contains(column.toLowerCase(Locale.ROOT)))
      .getOrElse {
        throw new PuddingException(
          "paimon changelog result does not contain a row kind column; columns: " +
            dataFrame.columns.mkString(", "))
      }
    // The connector can expose _row_kind_ as its string form or as Paimon's byte value
    // (INSERT=0, UPDATE_BEFORE=1, UPDATE_AFTER=2, DELETE=3).
    val kind = lower(col(kindColumn).cast("string"))
    val normalized = dataFrame
      .filter(!kind.isin("-u", "update_before", "1"))
      .withColumn(opColumn,
        when(kind.isin("+i", "i", "insert", "0"), lit("I"))
          .when(kind.isin("+u", "u", "update", "update_after", "2"), lit("U"))
          .when(kind.isin("-d", "d", "delete", "3"), lit("D"))
          .otherwise(lit("?")))
    val metadata = normalized.columns.filter(column =>
      column.equalsIgnoreCase(kindColumn) || column.equalsIgnoreCase("_bucket_"))
    normalized.drop(metadata: _*)
  }

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    context.catalog.foreach { catalog =>
      if (catalog.`type` != "paimon") {
        throw new PuddingException(
          s"paimon-source requires a paimon catalog, but got: ${catalog.`type`}")
      }
    }

    val table = this.optionalString(cfg, TABLE_KEY)
    val path = this.optionalString(cfg, PATH_KEY)
    if (table.isEmpty && path.isEmpty) {
      throw new PuddingException(
        s"paimon-source requires $TABLE_KEY or $PATH_KEY!")
    }

    val userOptions = this.getStringOptions(cfg, OPTIONS_KEY)
    val options = userOptions ++ this.scanOptions(cfg)
    val configuredStreaming = this.booleanOption(cfg, STREAM_READ_KEY)
      .orElse(this.booleanOption(cfg, IS_STREAMING_KEY))
      .getOrElse(false)
    val streaming = this.effectiveStreaming(configuredStreaming, context.executionMode)

    val source = path match {
      case Some(location) =>
        if (streaming) {
          sparkSession.readStream.format("paimon").options(options).load(location)
        } else {
          sparkSession.read.format("paimon").options(options).load(location)
        }
      case None =>
        val fullTable = context.catalog
          .map(catalog => CatalogTableName.qualify(catalog, table.get))
          .getOrElse(table.get)
        if (streaming) {
          sparkSession.readStream.format("paimon").options(options).table(fullTable)
        } else {
          sparkSession.read.format("paimon").options(options).table(fullTable)
        }
    }

    val filtered = cfg.get(FILTER_EXPR_KEY).map { expression =>
      source.filter(expression.toString)
    }.getOrElse(source)
    this.numPartitions(cfg) match {
      case Some(partitions) => filtered.repartition(partitions)
      case None => filtered
    }
  }

  private def scanOptions(cfg: Map[String, Any]): Map[String, String] = {
    var options = Map.empty[String, String]
    this.optionalString(cfg, SCAN_MODE_KEY).foreach(value => options += "scan.mode" -> value)
    cfg.get(SNAPSHOT_ID_KEY).filter(_ != null).foreach(value =>
      options += "scan.snapshot-id" -> value.toString)
    this.optionalString(cfg, TAG_NAME_KEY).foreach(value => options += "scan.tag-name" -> value)
    this.optionalString(cfg, BRANCH_KEY).foreach(value => options += "scan.branch" -> value)
    cfg.get(TIMESTAMP_MILLIS_KEY).filter(_ != null).foreach(value =>
      options += "scan.timestamp-millis" -> value.toString)
    options
  }

  private def numPartitions(cfg: Map[String, Any]): Option[Int] = {
    cfg.get(NUM_PARTITIONS_KEY).filter(_ != null).map { value =>
      val partitions = try {
        value.toString.trim.toInt
      } catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"$NUM_PARTITIONS_KEY must be an integer, but got: $value")
      }
      if (partitions <= 0) {
        throw new PuddingException(
          s"$NUM_PARTITIONS_KEY must be greater than zero, but got: $partitions")
      }
      partitions
    }
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
