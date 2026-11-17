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
 * Supported formats and actions. The action names map directly to native maintenance semantics
 * but use one spelling across all formats.
 */
object MaintenanceActions {

  val Iceberg = "iceberg"

  val Paimon = "paimon"

  val Hudi = "hudi"

  val Status = "status"

  val RewriteDataFiles = "rewrite-data-files"

  val RewriteManifests = "rewrite-manifests"

  val ExpireSnapshots = "expire-snapshots"

  val RemoveOrphanFiles = "remove-orphan-files"

  val Compact = "compact"

  val CompactManifests = "compact-manifests"

  val Clean = "clean"

  val Cluster = "cluster"

  val Archive = "archive"

  private val IcebergActions = Seq(
    MaintenanceActionSpec(
      Status, "show snapshot, data file and small-file metrics", destructive = false,
      nativeDryRun = true),
    MaintenanceActionSpec(
      RewriteDataFiles, "rewrite small data files using Iceberg rewrite_data_files",
      destructive = false, nativeDryRun = false),
    MaintenanceActionSpec(
      RewriteManifests, "compact manifest files", destructive = false, nativeDryRun = false),
    MaintenanceActionSpec(
      ExpireSnapshots, "expire old snapshots", destructive = true, nativeDryRun = false),
    MaintenanceActionSpec(
      RemoveOrphanFiles, "remove files not referenced by the table", destructive = true,
      nativeDryRun = true))

  private val PaimonActions = Seq(
    MaintenanceActionSpec(
      Status, "show snapshot, data file and small-file metrics", destructive = false,
      nativeDryRun = true),
    MaintenanceActionSpec(
      Compact, "run Paimon compaction", destructive = false, nativeDryRun = false),
    MaintenanceActionSpec(
      CompactManifests, "compact Paimon manifests", destructive = false, nativeDryRun = false),
    MaintenanceActionSpec(
      ExpireSnapshots, "expire old Paimon snapshots", destructive = true, nativeDryRun = false),
    MaintenanceActionSpec(
      RemoveOrphanFiles, "remove files not referenced by the table", destructive = true,
      nativeDryRun = true))

  private val HudiActions = Seq(
    MaintenanceActionSpec(
      Status, "show commits, pending compaction/clustering and path size", destructive = false,
      nativeDryRun = true),
    MaintenanceActionSpec(
      Clean, "run Hudi clean", destructive = true, nativeDryRun = false),
    MaintenanceActionSpec(
      Compact, "run or schedule Hudi compaction", destructive = false, nativeDryRun = false),
    MaintenanceActionSpec(
      Cluster, "run or schedule Hudi clustering", destructive = false, nativeDryRun = false),
    MaintenanceActionSpec(
      Archive, "archive Hudi timeline instants", destructive = false, nativeDryRun = false))

  private val ByFormat = Map(
    Iceberg -> IcebergActions,
    Paimon -> PaimonActions,
    Hudi -> HudiActions)

  val formats: Seq[String] = Seq(Iceberg, Paimon, Hudi)

  def specs(format: String): Seq[MaintenanceActionSpec] = {
    ByFormat.getOrElse(
      MaintenanceValues.normalize(format),
      throw new MaintenanceUsageException(
        s"unsupported format '${MaintenanceValues.normalize(format)}'; supported: " +
          formats.mkString(", ")))
  }

  def spec(format: String, action: String): MaintenanceActionSpec = {
    val normalized = MaintenanceValues.normalize(action)
    this.specs(format).find(_.name == normalized).getOrElse {
      throw new MaintenanceUsageException(
        s"unsupported action '$normalized' for format '${MaintenanceValues.normalize(format)}'; " +
          s"supported: ${this.specs(format).map(_.name).mkString(", ")}")
    }
  }
}
