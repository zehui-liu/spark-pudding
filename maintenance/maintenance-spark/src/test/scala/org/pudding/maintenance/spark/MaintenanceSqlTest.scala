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

import org.pudding.maintenance.api.{MaintenanceRequest, MaintenanceRetention, MaintenanceRewrite, MaintenanceUsageException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceSqlTest extends AnyFunSuite with Matchers {

  private val iceberg = MaintenanceRequest(
    format = "iceberg",
    action = "rewrite-data-files",
    catalog = Some("lake"),
    table = Some("db.orders"),
    rewrite = MaintenanceRewrite(
      targetFileSize = Some("128MB"),
      minInputFiles = Some(5),
      where = Some("dt = '2026-01-01'"),
      strategy = Some("binpack")))

  test("escape backslashes and quotes in SQL literals") {
    MaintenanceSql.literal("a\\'b") shouldBe "'a\\\\''b'"
  }

  test("build Iceberg rewrite_data_files SQL") {
    val sql = IcebergSql.action(iceberg).head.sql

    sql shouldBe
      "CALL `lake`.system.rewrite_data_files(table => 'db.orders', " +
        "where => 'dt = ''2026-01-01''', strategy => 'binpack', " +
        "options => map('min-input-files', '5', 'target-file-size-bytes', '134217728'))"
  }

  test("build Iceberg expiry and orphan SQL") {
    val expire = iceberg.copy(
      action = "expire-snapshots",
      rewrite = MaintenanceRewrite(),
      retention = MaintenanceRetention(olderThan = Some("7d"), retainLast = Some(3)))
      .normalized
    val expireSql = IcebergSql.action(expire).head.sql
    expireSql should include ("CALL `lake`.system.expire_snapshots(table => 'db.orders'")
    expireSql should include ("older_than => TIMESTAMP '")
    expireSql should include ("retain_last => 3")

    val orphan = expire.copy(
      action = "remove-orphan-files",
      retention = MaintenanceRetention(olderThan = Some("2d")),
      dryRun = true)
    val orphanSql = IcebergSql.action(orphan).head.sql
    orphanSql should include ("dry_run => true")
    orphanSql should include ("older_than => TIMESTAMP '")
  }

  test("build Paimon compact SQL with named arguments and dynamic options") {
    val request = MaintenanceRequest(
      format = "paimon",
      action = "compact",
      catalog = Some("paimon"),
      table = Some("db.orders"),
      rewrite = MaintenanceRewrite(where = Some("dt > 10")),
      options = Map(
        "partitions" -> "dt=1;dt=2",
        "compact-strategy" -> "full",
        "sink.parallelism" -> "4")).normalized
    val sql = PaimonSql.action(request).head.sql

    sql should include ("CALL `paimon`.sys.compact(table => 'db.orders'")
    sql should include ("where => 'dt > 10'")
    sql should include ("partitions => 'dt=1;dt=2'")
    sql should include ("compact_strategy => 'full'")
    sql should include ("options => 'sink.parallelism=4'")
  }

  test("build Paimon expiry and orphan SQL") {
    val expire = MaintenanceRequest(
      format = "paimon",
      action = "expire-snapshots",
      catalog = Some("paimon"),
      table = Some("db.orders"),
      retention = MaintenanceRetention(
        olderThan = Some("7d"), retainMax = Some(10), retainMin = Some(2)),
      options = Map("max-deletes" -> "20")).normalized
    val sql = PaimonSql.action(expire).head.sql
    sql should include ("CALL `paimon`.sys.expire_snapshots(table => 'db.orders'")
    sql should include ("older_than => '")
    sql should include ("retain_max => 10")
    sql should include ("retain_min => 2")
    sql should include ("max_deletes => 20")

    val orphan = expire.copy(
      action = "remove-orphan-files",
      retention = MaintenanceRetention(olderThan = Some("2d")),
      dryRun = true,
      options = Map("mode" -> "local", "parallelism" -> "4")).normalized
    val orphanSql = PaimonSql.action(orphan).head.sql
    orphanSql should include ("dry_run => true")
    orphanSql should include ("mode => 'local'")
    orphanSql should include ("parallelism => 4")
  }

  test("build Hudi clean, compaction, clustering and archive SQL") {
    val clean = MaintenanceRequest(
      "hudi", "clean", table = Some("db.orders"),
      retention = MaintenanceRetention(retainLast = Some(10))).normalized
    HudiSql.action(clean).head.sql should include (
      "CALL run_clean(table => 'db.orders', retain_commits => 10)")

    val cleanByHours = clean.copy(
      retention = MaintenanceRetention(olderThan = Some("7d")),
      confirmed = true)
    val cleanByHoursSql = HudiSql.action(cleanByHours).head.sql
    cleanByHoursSql should include ("hours_retained => 168")
    cleanByHoursSql should include ("clean_policy => 'KEEP_LATEST_BY_HOURS'")

    val compact = MaintenanceRequest(
      "hudi", "compact", path = Some("hdfs:///warehouse/orders"),
      options = Map("op" -> "schedule", "hoodie.compact.inline" -> "false")).normalized
    val compactSql = HudiSql.action(compact).head.sql
    compactSql should include ("CALL run_compaction(op => 'schedule', path => 'hdfs:///warehouse/orders'")
    compactSql should include ("options => 'hoodie.compact.inline=false'")

    val cluster = MaintenanceRequest(
      "hudi", "cluster", path = Some("/warehouse/orders"),
      rewrite = MaintenanceRewrite(where = Some("dt = '2026-01-01'")),
      options = Map("op" -> "execute")).normalized
    val clusterSql = HudiSql.action(cluster).head.sql
    clusterSql should include ("predicate => 'dt = ''2026-01-01'''")
    clusterSql should include ("op => 'execute'")

    val archive = MaintenanceRequest(
      "hudi", "archive", path = Some("/warehouse/orders")).normalized
    val archiveSql = HudiSql.action(archive).head.sql
    archiveSql should include ("min_commits => 20")
    archiveSql should include ("max_commits => 30")
    archiveSql should include ("retain_commits => 10")
    archiveSql should include ("enable_metadata => true")
  }

  test("build status SQL for all formats") {
    val icebergStatus = IcebergSql.status(
      MaintenanceRequest("iceberg", "status", Some("lake"), Some("db.orders")))
    icebergStatus.map(_.name) shouldBe Seq("snapshots", "files", "small-files", "manifests")

    val paimonStatus = PaimonSql.status(
      MaintenanceRequest("paimon", "status", Some("paimon"), Some("db.orders")))
    paimonStatus.map(_.name) shouldBe Seq("snapshots", "files", "small-files", "partitions")
    paimonStatus.map(_.sql).mkString(" ") should include ("$" + "snapshots")

    val hudiStatus = HudiSql.status(
      MaintenanceRequest("hudi", "status", table = Some("db.orders")))
    hudiStatus.map(_.name) shouldBe Seq("commits", "compaction", "clustering")
  }

  test("reject unsafe Hudi option strings") {
    val request = MaintenanceRequest(
      "hudi", "compact", path = Some("/warehouse/orders"),
      options = Map("a" -> "b,c")).normalized
    intercept[MaintenanceUsageException] {
      HudiSql.action(request)
    }.getMessage should include ("can not contain")
  }
}
