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

import org.pudding.maintenance.api.{MaintenanceActions, MaintenanceRequest, MaintenanceUsageException, MaintenanceValidation, MaintenanceValues}

/**
 * Iceberg 1.5 procedure SQL. The table argument is relative to `CALL <catalog>.system...`;
 * an explicitly supplied catalog prefix is removed first.
 */
private[spark] object IcebergSql {

  def status(request: MaintenanceRequest): Seq[NamedSql] = {
    val base = MaintenanceSql.tableBase(request)
    val smallFileBytes = MaintenanceSql.smallFileBytes(request)
    Seq(
      NamedSql(
        "snapshots",
        s"SELECT count(*) AS snapshots, max(committed_at) AS last_committed_at " +
          s"FROM $base.snapshots"),
      NamedSql(
        "files",
        s"SELECT count(*) AS data_files, coalesce(sum(file_size_in_bytes), 0) AS size_bytes " +
          s"FROM $base.files"),
      NamedSql(
        "small-files",
        s"SELECT count(*) AS small_files FROM $base.files " +
          s"WHERE file_size_in_bytes < $smallFileBytes"),
      NamedSql(
        "manifests",
        s"SELECT count(*) AS manifests FROM $base.manifests"))
  }

  def action(request: MaintenanceRequest): Seq[NamedSql] = {
    request.action match {
      case MaintenanceActions.RewriteDataFiles => this.rewriteDataFiles(request)
      case MaintenanceActions.RewriteManifests => this.rewriteManifests(request)
      case MaintenanceActions.ExpireSnapshots => this.expireSnapshots(request)
      case MaintenanceActions.RemoveOrphanFiles => this.removeOrphanFiles(request)
      case other =>
        throw new MaintenanceUsageException(s"iceberg action is not implemented: $other")
    }
  }

  private def rewriteDataFiles(request: MaintenanceRequest): Seq[NamedSql] = {
    val options = (request.options -- Set("small-file-bytes", "location", "max-concurrent-deletes")) ++
      request.targetFileSize.map(value =>
        "target-file-size-bytes" -> MaintenanceValues.size(value).toString) ++
      request.minInputFiles.map(value => "min-input-files" -> value.toString)
    val arguments = Seq(
      Some(s"table => ${MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))}"),
      MaintenanceSql.whereArg(request.where),
      MaintenanceSql.strategyArg(request.strategy),
      MaintenanceSql.optionMap(options)).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.system.rewrite_data_files(${arguments.mkString(", ")})")
  }

  private def rewriteManifests(request: MaintenanceRequest): Seq[NamedSql] = {
    val table = MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.system.rewrite_manifests(table => $table)")
  }

  private def expireSnapshots(request: MaintenanceRequest): Seq[NamedSql] = {
    val arguments = Seq(
      Some(s"table => ${MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))}"),
      MaintenanceSql.timeLiteral(request),
      request.retainLast.map(value => s"retain_last => $value"),
      request.options.get("max-concurrent-deletes")
        .map(value =>
          s"max_concurrent_deletes => ${MaintenanceSql.positiveInt(value, "max-concurrent-deletes")}"))
      .flatten
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.system.expire_snapshots(${arguments.mkString(", ")})")
  }

  private def removeOrphanFiles(request: MaintenanceRequest): Seq[NamedSql] = {
    val arguments = Seq(
      Some(s"table => ${MaintenanceSql.literal(MaintenanceValidation.tableWithoutCatalog(request))}"),
      MaintenanceSql.timeLiteral(request),
      Some(s"dry_run => ${request.dryRun}"),
      request.options.get("location").map(value => s"location => ${MaintenanceSql.literal(value)}"),
      request.options.get("max-concurrent-deletes")
        .map(value =>
          s"max_concurrent_deletes => ${MaintenanceSql.positiveInt(value, "max-concurrent-deletes")}"))
      .flatten
    MaintenanceSql.one(
      request.action,
      s"CALL ${MaintenanceSql.catalog(request)}.system.remove_orphan_files(${arguments.mkString(", ")})")
  }
}
