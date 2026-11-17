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

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.pudding.maintenance.api.{MaintenanceRequest, MaintenanceRetention, MaintenanceRewrite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IcebergMaintenanceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var warehouse: Path = _

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    this.warehouse = Files.createTempDirectory("pudding-iceberg-maintenance")
    this.spark = SparkSession.builder()
      .appName("test for iceberg table maintenance")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lake.type", "hadoop")
      .config("spark.sql.catalog.lake.warehouse", this.warehouse.toUri.toString)
      .getOrCreate()
    this.spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
    this.spark.sql(
      "CREATE TABLE lake.db.orders (id BIGINT, amount DOUBLE, dt STRING) USING iceberg")
    this.spark.sql(
      "INSERT INTO lake.db.orders VALUES (1, 10.0, '2026-01-01'), (2, 20.0, '2026-01-01')")
  }

  override def afterAll(): Unit = {
    if (this.spark != null) {
      this.spark.stop()
    }
    if (this.warehouse != null) {
      this.deleteRecursively(this.warehouse)
    }
  }

  private def request(action: String): MaintenanceRequest = {
    MaintenanceRequest("iceberg", action, catalog = Some("lake"), table = Some("db.orders"))
  }

  test("collect Iceberg table status") {
    val result = new SparkTableMaintenanceService(this.spark).execute(
      this.request("status"))

    result.executed shouldBe true
    result.summary("files.data_files").toLong should be > 0L
    result.summary("snapshots.snapshots").toLong should be > 0L
    result.rows.map(_("query")).toSet shouldBe
      Set("snapshots", "files", "small-files", "manifests")
  }

  test("run Iceberg rewrite and expiry procedures") {
    val service = new SparkTableMaintenanceService(this.spark)

    val rewrite = service.execute(this.request("rewrite-data-files").copy(
      rewrite = MaintenanceRewrite(targetFileSize = Some("1MB"), minInputFiles = Some(1))))
    rewrite.executed shouldBe true
    rewrite.statements.head should include ("rewrite_data_files")

    val expire = service.execute(this.request("expire-snapshots").copy(
      retention = MaintenanceRetention(olderThan = Some("2d"), retainLast = Some(1)),
      confirmed = true))
    expire.executed shouldBe true
    expire.statements.head should include ("expire_snapshots")
  }

  test("dry-run Iceberg orphan file cleanup") {
    val result = new SparkTableMaintenanceService(this.spark).execute(
      this.request("remove-orphan-files").copy(
        retention = MaintenanceRetention(olderThan = Some("2d")),
        dryRun = true))

    result.dryRun shouldBe true
    result.executed shouldBe false
    result.statements.head should include ("dry_run => true")
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val children = Files.list(path)
      try children.iterator().asScala.foreach(this.deleteRecursively)
      finally children.close()
    }
    Files.deleteIfExists(path)
  }
}
