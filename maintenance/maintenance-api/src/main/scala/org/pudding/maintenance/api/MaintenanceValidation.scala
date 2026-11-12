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

package org.pudding.maintenance.api

import java.time.Duration

/**
 * Request validation that is independent of Spark. The CLI and HTTP control plane reuse it
 * before submitting a maintenance task.
 */
object MaintenanceValidation {

  private val CatalogPattern = "^[A-Za-z_][A-Za-z0-9_-]*$".r

  private val TablePartPattern = "^[A-Za-z0-9_-]+$".r

  private val MinimumOrphanAge = Duration.ofHours(24)

  def validate(request: MaintenanceRequest): MaintenanceRequest = {
    if (request == null) {
      throw new MaintenanceUsageException("maintenance request can not be null")
    }
    val normalized = request.normalized
    val spec = MaintenanceActions.spec(normalized.format, normalized.action)

    normalized.catalog.foreach(this.validateCatalog)
    normalized.table.foreach(this.validateTable)

    if (normalized.format == MaintenanceActions.Hudi) {
      if (normalized.table.isEmpty && normalized.path.isEmpty) {
        throw new MaintenanceUsageException(
          "hudi maintenance requires --table or --path")
      }
      if (normalized.action == MaintenanceActions.Clean && normalized.table.isEmpty) {
        throw new MaintenanceUsageException(
          "hudi clean requires --table; path-only Hudi tables must be registered in a catalog " +
            "before run_clean can use them")
      }
    } else {
      if (normalized.catalog.isEmpty) {
        throw new MaintenanceUsageException(
          s"${normalized.format} maintenance requires --catalog")
      }
      if (normalized.table.isEmpty) {
        throw new MaintenanceUsageException(
          s"${normalized.format} maintenance requires --table")
      }
    }

    this.positive(normalized.retainLast, "retain-last")
    this.positive(normalized.retainMax, "retain-max")
    this.positive(normalized.retainMin, "retain-min")
    this.positive(normalized.minInputFiles, "min-input-files")
    normalized.targetFileSize.foreach(MaintenanceValues.size)

    if (normalized.retainMax.isDefined || normalized.retainMin.isDefined) {
      val supported = normalized.format == MaintenanceActions.Paimon &&
        normalized.action == MaintenanceActions.ExpireSnapshots
      if (!supported) {
        throw new MaintenanceUsageException(
          "--retain-max and --retain-min are supported only by Paimon expire-snapshots")
      }
    }
    if (normalized.retainLast.isDefined) {
      val supported =
        (normalized.format == MaintenanceActions.Iceberg &&
          normalized.action == MaintenanceActions.ExpireSnapshots) ||
          (normalized.format == MaintenanceActions.Hudi &&
            normalized.action == MaintenanceActions.Clean)
      if (!supported) {
        throw new MaintenanceUsageException(
          "--retain-last is supported only by Iceberg expire-snapshots and Hudi clean")
      }
    }

    normalized.action match {
      case MaintenanceActions.ExpireSnapshots =>
        if (normalized.format == MaintenanceActions.Iceberg &&
          normalized.olderThan.isEmpty && normalized.retainLast.isEmpty) {
          throw new MaintenanceUsageException(
            "iceberg expire-snapshots requires --older-than and/or --retain-last")
        }
        if (normalized.format == MaintenanceActions.Paimon &&
          normalized.olderThan.isEmpty && normalized.retainMax.isEmpty) {
          throw new MaintenanceUsageException(
            "paimon expire-snapshots requires --older-than and/or --retain-max")
        }
        if (normalized.format == MaintenanceActions.Paimon) {
          (normalized.retainMax, normalized.retainMin) match {
            case (Some(max), Some(min)) if max < min =>
              throw new MaintenanceUsageException(
                "paimon --retain-max must be greater than or equal to --retain-min")
            // Paimon 1.3.2 defaults retain_min to 10. Reject a lower retain_max unless the
            // caller explicitly supplies a compatible retain_min.
            case (Some(max), None) if max < 10 =>
              throw new MaintenanceUsageException(
                "paimon retain-max below the default retain-min (10); add --retain-min <= " +
                  "--retain-max")
            case _ => ()
          }
        }
        normalized.olderThan.foreach(MaintenanceValues.duration)

      case MaintenanceActions.RemoveOrphanFiles =>
        val age = normalized.olderThan.map(MaintenanceValues.duration).getOrElse {
          throw new MaintenanceUsageException(
            "remove-orphan-files requires --older-than")
        }
        if (age.compareTo(MinimumOrphanAge) < 0) {
          throw new MaintenanceUsageException(
            "remove-orphan-files requires --older-than of at least 24h")
        }

      case MaintenanceActions.Clean if normalized.format == MaintenanceActions.Hudi =>
        val optionModes = Seq(
          "retain-commits", "retain_commits",
          "hours-retained", "hours_retained",
          "file-versions-retained", "file_versions_retained")
          .count(normalized.options.contains)
        val retentionModes = optionModes +
          (if (normalized.olderThan.isDefined) 1 else 0) +
          (if (normalized.retainLast.isDefined) 1 else 0)
        if (retentionModes > 1) {
          throw new MaintenanceUsageException(
            "hudi clean accepts only one retention mode: --older-than, --retain-last or " +
              "one explicit retain-commits/hours-retained/file-versions-retained option")
        }

      case _ => ()
    }

    if (spec.destructive && !normalized.dryRun && !normalized.confirmed) {
      throw new MaintenanceUsageException(
        s"action '${normalized.action}' can change or delete data; add --confirm or --dry-run")
    }
    normalized
  }

  def target(request: MaintenanceRequest): String = {
    request.table
      .map { table =>
        request.catalog match {
          case Some(catalog) if !table.startsWith(catalog + ".") => s"$catalog.$table"
          case _ => table
        }
      }
      .orElse(request.path)
      .getOrElse("<unknown>")
  }

  /**
   * Table identifier without the catalog prefix, for procedure arguments that already receive
   * the catalog through `CALL <catalog>.system...`.
   */
  def tableWithoutCatalog(request: MaintenanceRequest): String = {
    val table = request.table.getOrElse(
      throw new MaintenanceUsageException("maintenance request has no table"))
    request.catalog match {
      case Some(catalog) if table.startsWith(catalog + ".") =>
        table.substring(catalog.length + 1)
      case _ => table
    }
  }

  def tableParts(request: MaintenanceRequest): Seq[String] = {
    this.tableWithoutCatalog(request).split("\\.", -1).toSeq
  }

  private def validateCatalog(catalog: String): Unit = {
    if (CatalogPattern.findFirstIn(catalog).isEmpty) {
      throw new MaintenanceUsageException(
        s"invalid catalog name '$catalog'; use letters, digits, underscore or hyphen")
    }
  }

  private def validateTable(table: String): Unit = {
    val parts = table.split("\\.", -1)
    if (parts.isEmpty || parts.exists(part => TablePartPattern.findFirstIn(part).isEmpty)) {
      throw new MaintenanceUsageException(
        s"invalid table identifier '$table'; expected dotted identifier parts with letters, " +
          "digits, underscore or hyphen")
    }
  }

  private def positive(value: Option[Int], key: String): Unit = {
    value.foreach { item =>
      if (item <= 0) {
        throw new MaintenanceUsageException(s"$key must be greater than zero")
      }
    }
  }
}
