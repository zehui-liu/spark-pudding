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

package org.pudding.maintenance.spark

import org.pudding.maintenance.api.{MaintenanceActions, MaintenanceRequest, MaintenanceUsageException, MaintenanceValidation}

/**
 * Paimon 1.3 procedure SQL. Procedures live under `<catalog>.sys`; action options are passed
 * as Paimon's comma-separated `key=value` string rather than a Spark map.
 */
private[spark] object PaimonSql {

  private val CompactArguments = Set(
    "partitions", "compact-strategy", "compact_strategy", "order-strategy", "order_strategy",
    "order-by", "order_by", "partition-idle-time", "partition_idle_time")

  private val ExpireArguments = Set("max-deletes", "max_deletes")

  private val InternalOptions = Set("small-file-bytes", "mode", "parallelism", "location")

  def status(request: MaintenanceRequest): Seq[NamedSql] = {
    val snapshots = MaintenanceSql.systemTable(request, "$" + "snapshots")
    val files = MaintenanceSql.systemTable(request, "$" + "files")
    val partitions = MaintenanceSql.systemTable(request, "$" + "partitions")
    val smallFileBytes = MaintenanceSql.smallFileBytes(request)
    Seq(
      NamedSql(
        "snapshots",
        s"SELECT count(*) AS snapshots, max(commit_time) AS last_commit_time " +
          s"FROM $snapshots"),
      NamedSql(
        "files",
        s"SELECT count(*) AS data_files, coalesce(sum(file_size_in_bytes), 0) AS size_bytes " +
          s"FROM $files"),
      NamedSql(
        "small-files",
        s"SELECT count(*) AS small_files FROM $files " +
          s"WHERE file_size_in_bytes < $smallFileBytes"),
      NamedSql(
        "partitions",
        s"SELECT count(*) AS partitions FROM $partitions"))
  }

  def action(request: MaintenanceRequest): Seq[NamedSql] = {
    request.action match {
      case MaintenanceActions.Compact => this.compact(request)
      case MaintenanceActions.CompactManifests => this.compactManifests(request)
      case MaintenanceActions.ExpireSnapshots => this.expireSnapshots(request)
      case MaintenanceActions.RemoveOrphanFiles => this.removeOrphanFiles(request)
      case other =>
        throw new MaintenanceUsageException(s"paimon action is not implemented: $other")
    }
  }

  private def compact(request: MaintenanceRequest): Seq[NamedSql] = {
    val strategy = this.argument(request, "compact-strategy", "compact_strategy")
    strategy.foreach { value =>
      if (value != "full" && value != "minor") {
        throw new MaintenanceUsageException(
          s"paimon compact-strategy must be full or minor, got: $value")
      }
    }
    val arguments = Seq(
      Some(s"table => ${MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))}"),
      MaintenanceSql.whereArg(request.where),
      request.options.get("partitions")
        .map(value => s"partitions => ${MaintenanceSql.literal(value)}"),
      strategy.map(value => s"compact_strategy => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "order-strategy", "order_strategy")
        .map(value => s"order_strategy => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "order-by", "order_by")
        .map(value => s"order_by => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "partition-idle-time", "partition_idle_time")
        .map(value => s"partition_idle_time => ${MaintenanceSql.literal(value)}"),
      this.dynamicOptions(
        request.options -- CompactArguments -- InternalOptions)).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.sys.compact(${arguments.mkString(", ")})")
  }

  private def compactManifests(request: MaintenanceRequest): Seq[NamedSql] = {
    val table = MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.sys.compact_manifests(table => $table)")
  }

  private def expireSnapshots(request: MaintenanceRequest): Seq[NamedSql] = {
    val maxDeletes = this.argument(request, "max-deletes", "max_deletes")
      .map(value => s"max_deletes => ${MaintenanceSql.positiveInt(value, "max-deletes")}")
    val arguments = Seq(
      Some(s"table => ${MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))}"),
      MaintenanceSql.timeStringArg(request),
      request.retainMax.map(value => s"retain_max => $value"),
      request.retainMin.map(value => s"retain_min => $value"),
      maxDeletes,
      this.dynamicOptions(request.options -- ExpireArguments -- InternalOptions)).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.sys.expire_snapshots(${arguments.mkString(", ")})")
  }

  private def removeOrphanFiles(request: MaintenanceRequest): Seq[NamedSql] = {
    val arguments = Seq(
      Some(s"table => ${MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))}"),
      MaintenanceSql.timeStringArg(request),
      Some(s"dry_run => ${request.dryRun}"),
      request.options.get("mode").map(value => s"mode => ${MaintenanceSql.literal(value)}"),
      request.options.get("parallelism")
        .map(value => s"parallelism => ${MaintenanceSql.positiveInt(value, "parallelism")}")).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.sys.remove_orphan_files(${arguments.mkString(", ")})")
  }

  private def argument(
      request: MaintenanceRequest,
      key: String,
      aliases: String*): Option[String] = {
    val names = (key +: aliases).distinct
    names.collectFirst {
      case name if request.options.get(name).exists(_.trim.nonEmpty) =>
        request.options(name).trim
    }
  }

  private def dynamicOptions(options: Map[String, String]): Option[String] = {
    val explicit = options.get("options").map(_.trim).filter(_.nonEmpty)
    val custom = options - "options"
    val invalid = custom.filter { case (key, value) =>
      key.contains(",") || key.contains("=") || value.contains(",")
    }.keys.toSeq.sorted
    if (invalid.nonEmpty) {
      throw new MaintenanceUsageException(
        "Paimon options can not contain ',' in values or ','/'=' in keys: " +
          invalid.mkString(", "))
    }
    val rest = custom.toSeq.sortBy(_._1).map {
      case (key, value) => s"$key=$value"
    }
    val parts = explicit.toSeq ++ rest
    if (parts.isEmpty) None
    else Some(s"options => ${MaintenanceSql.literal(parts.mkString(","))}")
  }
}
