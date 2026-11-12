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

package org.pudding.maintenance.spark

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}

import org.pudding.maintenance.api._

/**
 * Executes table-maintenance requests through Spark SQL native procedures.
 *
 * The service does not create a SparkSession. The CLI and HTTP control plane create one with
 * the format-specific extension and catalog settings before calling this service.
 */
class SparkTableMaintenanceService(session: SparkSession) extends TableMaintenanceService {

  override def capabilities(format: String): Seq[MaintenanceActionSpec] =
    MaintenanceActions.specs(format)

  override def execute(request: MaintenanceRequest): MaintenanceResult = {
    val normalized = MaintenanceValidation.validate(request)
    val spec = MaintenanceActions.spec(normalized.format, normalized.action)
    val target = MaintenanceValidation.target(normalized)

    if (normalized.action == MaintenanceActions.Status) {
      return this.status(normalized, target)
    }

    val statements = normalized.format match {
      case MaintenanceActions.Iceberg => IcebergSql.action(normalized)
      case MaintenanceActions.Paimon => PaimonSql.action(normalized)
      case MaintenanceActions.Hudi => HudiSql.action(normalized)
      case other =>
        throw new MaintenanceUsageException(s"unsupported format: $other")
    }

    val preview = normalized.dryRun && !spec.nativeDryRun
    val result = if (preview) {
      MaintenanceResult(
        format = normalized.format,
        action = normalized.action,
        target = target,
        dryRun = true,
        executed = false,
        statements = statements.map(_.sql),
        message = "dry-run: maintenance statements were not executed")
    } else {
      val rows = this.run(statements.head)
      MaintenanceResult(
        format = normalized.format,
        action = normalized.action,
        target = target,
        dryRun = normalized.dryRun,
        executed = !normalized.dryRun,
        statements = statements.map(_.sql),
        summary = this.firstRowSummary(rows),
        rows = rows.map(this.rowMap),
        message = if (normalized.dryRun) "dry-run completed" else "maintenance completed")
    }
    result
  }

  private def status(request: MaintenanceRequest, target: String): MaintenanceResult = {
    val queries = request.format match {
      case MaintenanceActions.Iceberg => IcebergSql.status(request)
      case MaintenanceActions.Paimon => PaimonSql.status(request)
      case MaintenanceActions.Hudi => HudiSql.status(request)
      case other =>
        throw new MaintenanceUsageException(s"unsupported format: $other")
    }
    val summary = mutable.LinkedHashMap[String, String]()
    val rows = Seq.newBuilder[Map[String, String]]
    val errors = mutable.ListBuffer[String]()
    queries.foreach { query =>
      try {
        val result = this.run(query)
        result.foreach { row =>
          val map = this.rowMap(row)
          resultsWithQuery(query.name, map).foreach { case (key, value) =>
            summary += key -> value
          }
          rows += (map + ("query" -> query.name))
        }
      } catch {
        case NonFatal(error) if this.notApplicable(query, error) =>
          summary += s"${query.name}.applicable" -> "false"
        case NonFatal(error) =>
          errors += s"${query.name}: ${this.message(error)}"
      }
    }
    if (request.format == MaintenanceActions.Hudi) {
      request.path.foreach { path =>
        try {
          this.pathSummary(path).foreach { case (key, value) => summary += key -> value }
        } catch {
          case NonFatal(error) => errors += s"path: ${this.message(error)}"
        }
      }
    }
    if (errors.nonEmpty) {
      summary += "errors" -> errors.mkString("; ")
    }
    val collectedRows = rows.result()
    if (collectedRows.isEmpty && errors.nonEmpty) {
      throw new MaintenanceException("status failed: " + errors.mkString("; "))
    }
    MaintenanceResult(
      format = request.format,
      action = request.action,
      target = target,
      dryRun = request.dryRun,
      executed = true,
      statements = queries.map(_.sql),
      summary = summary.toMap,
      rows = collectedRows,
      message = "status collected")
  }

  private def resultsWithQuery(
      query: String,
      values: Map[String, String]): Map[String, String] = {
    values.map { case (key, value) => s"$query.$key" -> value }
  }

  private def run(statement: NamedSql): Seq[Row] = {
    try {
      this.session.sql(statement.sql).collect().toSeq
    } catch {
      case NonFatal(error) =>
        throw new MaintenanceException(
          s"maintenance statement '${statement.name}' failed: ${this.message(error)}",
          cause = error)
    }
  }

  private def firstRowSummary(rows: Seq[Row]): Map[String, String] = {
    rows.headOption.map(this.rowMap).getOrElse(Map.empty)
  }

  private def rowMap(row: Row): Map[String, String] = {
    row.schema.fieldNames.zipWithIndex.map { case (name, index) =>
      name -> Option(row.get(index)).map(_.toString).getOrElse("")
    }.toMap
  }

  private def pathSummary(path: String): Map[String, String] = {
    val hadoopPath = new Path(path)
    val fileSystem = hadoopPath.getFileSystem(this.session.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(hadoopPath)) {
      Map("path.exists" -> "false")
    } else {
      var files = 0L
      var bytes = 0L
      val statuses = fileSystem.listFiles(hadoopPath, true)
      while (statuses.hasNext) {
        val status = statuses.next()
        if (!status.getPath.getName.startsWith(".")) {
          files += 1L
          bytes += status.getLen
        }
      }
      Map(
        "path.exists" -> "true",
        "path.file_count" -> files.toString,
        "path.size_bytes" -> bytes.toString)
    }
  }

  private def notApplicable(query: NamedSql, error: Throwable): Boolean = {
    // Hudi's show_compaction procedure asserts for copy-on-write tables instead of returning
    // an empty result. That is expected status information, not a maintenance failure.
    query.name == "compaction" &&
      this.message(error).contains("Non Merge On Read")
  }

  private def message(error: Throwable): String = {
    Option(error.getMessage).map(_.trim).filter(_.nonEmpty)
      .getOrElse(error.getClass.getSimpleName)
  }
}
