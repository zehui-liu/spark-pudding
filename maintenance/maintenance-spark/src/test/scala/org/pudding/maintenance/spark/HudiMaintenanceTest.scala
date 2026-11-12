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

class HudiMaintenanceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var workspace: Path = _

  private var spark: SparkSession = _

  private var tablePath: Path = _

  override def beforeAll(): Unit = {
    this.workspace = Files.createTempDirectory("pudding-hudi-maintenance")
    this.tablePath = this.workspace.resolve("orders")
    this.spark = SparkSession.builder()
      .appName("test for hudi table maintenance")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(
        "spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()
    this.spark.sql(
      s"CREATE TABLE default.hudi_orders (id BIGINT, name STRING, dt STRING) USING hudi " +
        s"TBLPROPERTIES (primaryKey = 'id', type = 'cow', " +
        s"'hoodie.metadata.enable' = 'false') " +
        s"LOCATION '${this.tablePath.toUri}'")
    this.spark.sql(
      "INSERT INTO default.hudi_orders VALUES (1, 'a', '2026-01-01'), (2, 'b', '2026-01-01')")
  }

  override def afterAll(): Unit = {
    if (this.spark != null) {
      this.spark.stop()
    }
    if (this.workspace != null) {
      this.deleteRecursively(this.workspace)
    }
  }

  test("collect Hudi status and run clean, cluster and archive") {
    val service = new SparkTableMaintenanceService(this.spark)
    val status = service.execute(MaintenanceRequest(
      "hudi",
      "status",
      table = Some("default.hudi_orders"),
      path = Some(this.tablePath.toUri.toString)))

    status.executed shouldBe true
    status.summary("path.exists") shouldBe "true"
    status.summary.get("path.file_count").exists(_.toLong > 0L) shouldBe true
    status.rows.map(_("query")).toSet should contain ("commits")
    status.summary.get("errors") shouldBe None

    val clean = service.execute(MaintenanceRequest(
      "hudi",
      "clean",
      table = Some("default.hudi_orders"),
      retention = MaintenanceRetention(retainLast = Some(1)),
      confirmed = true))
    clean.executed shouldBe true
    clean.statements.head should include ("run_clean")

    val cleanByHours = service.execute(MaintenanceRequest(
      "hudi",
      "clean",
      table = Some("default.hudi_orders"),
      retention = MaintenanceRetention(olderThan = Some("7d")),
      confirmed = true))
    cleanByHours.executed shouldBe true
    cleanByHours.statements.head should include ("KEEP_LATEST_BY_HOURS")

    val cluster = service.execute(MaintenanceRequest(
      "hudi",
      "cluster",
      table = Some("default.hudi_orders"),
      options = Map("op" -> "scheduleAndExecute")))
    cluster.executed shouldBe true
    cluster.statements.head should include ("run_clustering")

    val archive = service.execute(MaintenanceRequest(
      "hudi",
      "archive",
      path = Some(this.tablePath.toUri.toString),
      options = Map(
        "min-commits" -> "1",
        "max-commits" -> "2",
        "retain-commits" -> "1",
        "enable-metadata" -> "false")))
    archive.executed shouldBe true
    archive.statements.head should include ("archive_commits")
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
