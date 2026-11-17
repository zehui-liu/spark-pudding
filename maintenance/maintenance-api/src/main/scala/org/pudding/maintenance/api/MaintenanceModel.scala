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

package org.pudding.maintenance.api

/**
 * Retention settings shared by the snapshot-expiry and clean actions.
 */
case class MaintenanceRetention(
    olderThan: Option[String] = None,
    retainLast: Option[Int] = None,
    retainMax: Option[Int] = None,
    retainMin: Option[Int] = None) {

  def normalized: MaintenanceRetention = this.copy(
    olderThan = MaintenanceValues.trimmed(this.olderThan))
}

/**
 * Rewrite/layout settings for Iceberg and Paimon compaction actions.
 */
case class MaintenanceRewrite(
    targetFileSize: Option[String] = None,
    minInputFiles: Option[Int] = None,
    where: Option[String] = None,
    strategy: Option[String] = None) {

  def normalized: MaintenanceRewrite = this.copy(
    targetFileSize = MaintenanceValues.trimmed(this.targetFileSize),
    where = MaintenanceValues.trimmed(this.where),
    strategy = MaintenanceValues.trimmed(this.strategy))
}

/**
 * One table-maintenance request.
 *
 * `catalog` is the Spark catalog name for Iceberg or Paimon. Hudi can use either a registered
 * `table` or a `path`. `options` contains native action options; the CLI exposes them with
 * repeated `--option key=value` flags.
 */
case class MaintenanceRequest(
    format: String,
    action: String,
    catalog: Option[String] = None,
    table: Option[String] = None,
    path: Option[String] = None,
    retention: MaintenanceRetention = MaintenanceRetention(),
    rewrite: MaintenanceRewrite = MaintenanceRewrite(),
    options: Map[String, String] = Map.empty,
    dryRun: Boolean = false,
    confirmed: Boolean = false) {

  def olderThan: Option[String] = this.retention.olderThan

  def retainLast: Option[Int] = this.retention.retainLast

  def retainMax: Option[Int] = this.retention.retainMax

  def retainMin: Option[Int] = this.retention.retainMin

  def targetFileSize: Option[String] = this.rewrite.targetFileSize

  def minInputFiles: Option[Int] = this.rewrite.minInputFiles

  def where: Option[String] = this.rewrite.where

  def strategy: Option[String] = this.rewrite.strategy

  def normalized: MaintenanceRequest = this.copy(
    format = MaintenanceValues.normalize(this.format),
    action = MaintenanceValues.normalize(this.action),
    catalog = MaintenanceValues.trimmed(this.catalog),
    table = MaintenanceValues.trimmed(this.table),
    path = MaintenanceValues.trimmed(this.path),
    retention = this.retention.normalized,
    rewrite = this.rewrite.normalized)
}

/**
 * Summary of one maintenance action.
 *
 * `summary` holds scalar values from the first returned row, while `rows` preserves the complete
 * (bounded) procedure output. `executed=false` means the action was a dry-run or a plan preview.
 */
case class MaintenanceResult(
    format: String,
    action: String,
    target: String,
    dryRun: Boolean,
    executed: Boolean,
    statements: Seq[String],
    summary: Map[String, String] = Map.empty,
    rows: Seq[Map[String, String]] = Seq.empty,
    message: String = "")

/**
 * Stable description of a maintenance action for help output and API discovery.
 */
case class MaintenanceActionSpec(
    name: String,
    description: String,
    destructive: Boolean,
    nativeDryRun: Boolean)

/**
 * A maintenance failure. `exitCode` is 2 for usage/validation errors and 1 for execution errors.
 */
class MaintenanceException(
    message: String,
    val exitCode: Int = 1,
    cause: Throwable = null)
  extends RuntimeException(message, cause)

final class MaintenanceUsageException(message: String, cause: Throwable = null)
  extends MaintenanceException(message, 2, cause)

/**
 * Programmatic table-maintenance service. The CLI and HTTP control plane are thin wrappers
 * around this interface.
 */
trait TableMaintenanceService extends AutoCloseable {

  def capabilities(format: String): Seq[MaintenanceActionSpec]

  def execute(request: MaintenanceRequest): MaintenanceResult

  override def close(): Unit = ()
}
