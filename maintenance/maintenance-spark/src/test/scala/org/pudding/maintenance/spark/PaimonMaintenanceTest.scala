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

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.pudding.maintenance.api.{MaintenanceRequest, MaintenanceRetention}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PaimonMaintenanceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var warehouse: Path = _

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    this.warehouse = Files.createTempDirectory("pudding-paimon-maintenance")
    this.spark = SparkSession.builder()
      .appName("test for paimon table maintenance")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", this.warehouse.toUri.toString)
      .getOrCreate()
    this.spark.sql("CREATE DATABASE IF NOT EXISTS paimon.db")
    this.spark.sql(
      "CREATE TABLE paimon.db.orders (id BIGINT, amount DOUBLE, dt STRING) USING paimon")
    this.spark.sql(
      "INSERT INTO paimon.db.orders VALUES (1, 10.0, '2026-01-01'), (2, 20.0, '2026-01-01')")
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
    MaintenanceRequest("paimon", action, catalog = Some("paimon"), table = Some("db.orders"))
  }

  test("collect Paimon table status") {
    val result = new SparkTableMaintenanceService(this.spark).execute(
      this.request("status"))

    result.executed shouldBe true
    result.summary("files.data_files").toLong should be > 0L
    result.summary("snapshots.snapshots").toLong should be > 0L
    result.rows.map(_("query")).toSet shouldBe
      Set("snapshots", "files", "small-files", "partitions")
  }

  test("run Paimon compaction and snapshot expiry") {
    val service = new SparkTableMaintenanceService(this.spark)

    val compact = service.execute(this.request("compact").copy(
      options = Map("compact-strategy" -> "full")))
    compact.executed shouldBe true
    compact.statements.head should include ("compact_strategy => 'full'")

    val expire = service.execute(this.request("expire-snapshots").copy(
      retention = MaintenanceRetention(
        olderThan = Some("2d"), retainMax = Some(1), retainMin = Some(1)),
      confirmed = true))
    expire.executed shouldBe true
    expire.statements.head should include ("retain_max => 1")
  }

  test("dry-run Paimon orphan file cleanup") {
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
