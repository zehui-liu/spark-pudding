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

import java.util.Locale

import org.pudding.maintenance.api.{MaintenanceActions, MaintenanceRequest, MaintenanceUsageException, MaintenanceValues}

/**
 * Hudi 0.14 procedure SQL. `run_clean` resolves a registered catalog table; compaction,
 * clustering and archive can operate directly on a path.
 */
private[spark] object HudiSql {

  private val CleanArguments = Set(
    "retain-commits", "retain_commits", "hours-retained", "hours_retained",
    "file-versions-retained", "file_versions_retained", "clean-policy", "clean_policy",
    "trigger-strategy", "trigger_strategy", "trigger-max-commits", "trigger_max_commits",
    "skip-locking", "skip_locking", "schedule-in-line", "schedule_in_line")

  private val CompactArguments = Set("op", "instants")

  private val ClusterArguments = Set(
    "op", "predicate", "order", "order-strategy", "order_strategy", "selected-partitions",
    "selected_partitions", "show-involved-partition", "show_involved_partition")

  def status(request: MaintenanceRequest): Seq[NamedSql] = {
    val names = Seq.newBuilder[NamedSql]
    request.table.foreach { _ =>
      names += NamedSql(
        "commits",
        s"CALL show_commits(${this.tableArg(request)}, limit => 10)")
    }
    val target = this.tableOrPathArg(request)
    names += NamedSql("compaction", s"CALL show_compaction($target, limit => 20)")
    names += NamedSql("clustering", s"CALL show_clustering($target, limit => 20)")
    names.result()
  }

  def action(request: MaintenanceRequest): Seq[NamedSql] = {
    request.action match {
      case MaintenanceActions.Clean => this.clean(request)
      case MaintenanceActions.Compact => this.compact(request)
      case MaintenanceActions.Cluster => this.cluster(request)
      case MaintenanceActions.Archive => this.archive(request)
      case other =>
        throw new MaintenanceUsageException(s"hudi action is not implemented: $other")
    }
  }

  private def clean(request: MaintenanceRequest): Seq[NamedSql] = {
    val arguments = Seq(
      Some(this.tableArg(request)),
      this.argument(request, "retain-commits", "retain_commits")
        .map(value => s"retain_commits => ${MaintenanceSql.positiveInt(value, "retain-commits")}")
        .orElse(request.retainLast.map(value => s"retain_commits => $value")),
      this.argument(request, "hours-retained", "hours_retained")
        .map(value => s"hours_retained => ${MaintenanceSql.positiveInt(value, "hours-retained")}")
        .orElse(request.olderThan.map(value =>
          s"hours_retained => ${this.positiveHours(value)}")),
      this.argument(request, "file-versions-retained", "file_versions_retained")
        .map(value => s"file_versions_retained => ${MaintenanceSql.positiveInt(value, "file-versions-retained")}"),
      this.cleanPolicy(request)
        .map(value => s"clean_policy => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "trigger-strategy", "trigger_strategy")
        .map(value => s"trigger_strategy => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "trigger-max-commits", "trigger_max_commits")
        .map(value => s"trigger_max_commits => ${MaintenanceSql.positiveInt(value, "trigger-max-commits")}"),
      this.booleanArgument(request, "skip-locking", "skip_locking")
        .map(value => s"skip_locking => $value"),
      this.booleanArgument(request, "schedule-in-line", "schedule_in_line")
        .map(value => s"schedule_in_line => $value"),
      MaintenanceSql.hudiOptions(request.options -- CleanArguments)
        .map(value => s"options => $value")).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL run_clean(${arguments.mkString(", ")})")
  }

  private def compact(request: MaintenanceRequest): Seq[NamedSql] = {
    val arguments = Seq(
      Some(s"op => ${MaintenanceSql.literal(this.op(request))}"),
      Some(this.tableOrPathArg(request)),
      request.options.get("instants")
        .map(value => s"instants => ${MaintenanceSql.literal(value)}"),
      MaintenanceSql.hudiOptions(request.options -- CompactArguments)
        .map(value => s"options => $value")).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL run_compaction(${arguments.mkString(", ")})")
  }

  private def cluster(request: MaintenanceRequest): Seq[NamedSql] = {
    val predicate = request.where
      .orElse(this.argument(request, "predicate", "predicate"))
    val arguments = Seq(
      Some(s"op => ${MaintenanceSql.literal(this.op(request))}"),
      Some(this.tableOrPathArg(request)),
      predicate.map(value => s"predicate => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "order", "order")
        .map(value => s"order => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "order-strategy", "order_strategy")
        .map(value => s"order_strategy => ${MaintenanceSql.literal(value)}"),
      this.argument(request, "selected-partitions", "selected_partitions")
        .map(value => s"selected_partitions => ${MaintenanceSql.literal(value)}"),
      this.booleanArgument(request, "show-involved-partition", "show_involved_partition")
        .map(value => s"show_involved_partition => $value"),
      MaintenanceSql.hudiOptions(request.options -- ClusterArguments)
        .map(value => s"options => $value")).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL run_clustering(${arguments.mkString(", ")})")
  }

  private def archive(request: MaintenanceRequest): Seq[NamedSql] = {
    val arguments = Seq(
      Some(this.tableOrPathArg(request)),
      this.argument(request, "min-commits", "min_commits")
        .map(value => s"min_commits => ${MaintenanceSql.positiveInt(value, "min-commits")}")
        .orElse(Some("min_commits => 20")),
      this.argument(request, "max-commits", "max_commits")
        .map(value => s"max_commits => ${MaintenanceSql.positiveInt(value, "max-commits")}")
        .orElse(Some("max_commits => 30")),
      this.argument(request, "retain-commits", "retain_commits")
        .map(value => s"retain_commits => ${MaintenanceSql.positiveInt(value, "retain-commits")}")
        .orElse(Some("retain_commits => 10")),
      this.booleanArgument(request, "enable-metadata", "enable_metadata")
        .map(value => s"enable_metadata => $value")
        .orElse(Some("enable_metadata => true"))).flatten
    MaintenanceSql.one(
      request.action,
      s"CALL archive_commits(${arguments.mkString(", ")})")
  }

  /**
   * Pick the clean policy that matches the supplied retention setting. Hudi ignores
   * `hours_retained` under the default commit-based policy, so `--older-than` must select
   * KEEP_LATEST_BY_HOURS.
   */
  private def cleanPolicy(request: MaintenanceRequest): Option[String] = {
    this.argument(request, "clean-policy", "clean_policy").orElse {
      if (this.argument(request, "file-versions-retained", "file_versions_retained").isDefined) {
        Some("KEEP_LATEST_FILE_VERSIONS")
      } else if (request.olderThan.isDefined ||
        this.argument(request, "hours-retained", "hours_retained").isDefined) {
        Some("KEEP_LATEST_BY_HOURS")
      } else {
        None
      }
    }
  }

  private def tableArg(request: MaintenanceRequest): String = {
    val table = request.table.getOrElse(
      throw new MaintenanceUsageException("hudi action requires --table"))
    s"table => ${MaintenanceSql.literal(table)}"
  }

  private def tableOrPathArg(request: MaintenanceRequest): String = {
    request.table match {
      case Some(table) => s"table => ${MaintenanceSql.literal(table)}"
      case None => request.path match {
        case Some(path) => s"path => ${MaintenanceSql.literal(path)}"
        case None =>
          throw new MaintenanceUsageException("hudi action requires --table or --path")
      }
    }
  }

  private def op(request: MaintenanceRequest): String = {
    val value = request.options.getOrElse("op", "scheduleAndExecute")
      .trim.toLowerCase(Locale.ROOT)
    value match {
      case "schedule" | "execute" | "scheduleandexecute" | "unschedule" =>
        request.options.getOrElse("op", "scheduleAndExecute").trim
      case other =>
        throw new MaintenanceUsageException(
          s"hudi op must be schedule, execute, scheduleAndExecute or unschedule; got: $other")
    }
  }

  private def positiveHours(value: String): Long = {
    val hours = MaintenanceValues.duration(value).toHours
    if (hours <= 0L) {
      throw new MaintenanceUsageException("hudi clean --older-than must be at least 1h")
    }
    hours
  }

  private def booleanArgument(
      request: MaintenanceRequest,
      key: String,
      alias: String): Option[Boolean] = {
    request.options.get(key).orElse(request.options.get(alias)).map { value =>
      value.trim.toLowerCase(Locale.ROOT) match {
        case "true" => true
        case "false" => false
        case other =>
          throw new MaintenanceUsageException(s"$key must be true or false, got: $other")
      }
    }
  }

  private def argument(
      request: MaintenanceRequest,
      key: String,
      alias: String): Option[String] = {
    request.options.get(key).orElse(request.options.get(alias))
      .map(_.trim).filter(_.nonEmpty)
  }
}
