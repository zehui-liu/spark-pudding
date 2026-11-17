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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceValidationTest extends AnyFunSuite with Matchers {

  test("require a catalog and table for iceberg and paimon") {
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(MaintenanceRequest("iceberg", "status"))
    }.getMessage should include ("--catalog")

    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(
        MaintenanceRequest("paimon", "status", catalog = Some("paimon")))
    }.getMessage should include ("--table")
  }

  test("require explicit confirmation for destructive actions") {
    val request = MaintenanceRequest(
      "iceberg", "expire-snapshots", catalog = Some("lake"), table = Some("db.orders"),
      retention = MaintenanceRetention(retainLast = Some(5)))
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(request)
    }.getMessage should include ("--confirm")

    MaintenanceValidation.validate(request.copy(confirmed = true)).confirmed shouldBe true
    MaintenanceValidation.validate(request.copy(dryRun = true)).dryRun shouldBe true
  }

  test("validate Paimon retain bounds") {
    val request = MaintenanceRequest(
      "paimon", "expire-snapshots", catalog = Some("paimon"), table = Some("db.orders"),
      retention = MaintenanceRetention(retainMax = Some(1)), confirmed = true)
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(request)
    }.getMessage should include ("default retain-min")

    val invalid = request.copy(
      retention = MaintenanceRetention(retainMax = Some(2), retainMin = Some(5)))
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(invalid)
    }.getMessage should include ("greater than or equal")

    MaintenanceValidation.validate(
      request.copy(retention = MaintenanceRetention(retainMax = Some(2), retainMin = Some(2))))
      .confirmed shouldBe true
  }

  test("reject retention flags that the selected action does not implement") {
    val iceberg = MaintenanceRequest(
      "iceberg", "expire-snapshots", catalog = Some("lake"), table = Some("db.orders"),
      retention = MaintenanceRetention(retainMax = Some(5)), confirmed = true)
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(iceberg)
    }.getMessage should include ("retain-max")

    val paimon = MaintenanceRequest(
      "paimon", "expire-snapshots", catalog = Some("paimon"), table = Some("db.orders"),
      retention = MaintenanceRetention(olderThan = Some("7d"), retainLast = Some(5)),
      confirmed = true)
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(paimon)
    }.getMessage should include ("retain-last")

    val hudi = MaintenanceRequest(
      "hudi", "clean", table = Some("db.orders"),
      retention = MaintenanceRetention(retainMax = Some(5)), confirmed = true)
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(hudi)
    }.getMessage should include ("retain-max")
  }

  test("enforce a 24 hour minimum age for orphan file cleanup") {
    val request = MaintenanceRequest(
      "paimon", "remove-orphan-files", catalog = Some("paimon"), table = Some("db.orders"),
      retention = MaintenanceRetention(olderThan = Some("1h")), dryRun = true)
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(request)
    }.getMessage should include ("at least 24h")

    MaintenanceValidation.validate(
      request.copy(retention = request.retention.copy(olderThan = Some("2d"))))
      .dryRun shouldBe true
  }

  test("Hudi clean requires a registered table") {
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(
        MaintenanceRequest("hudi", "clean", path = Some("hdfs:///warehouse/orders")))
    }.getMessage should include ("requires --table")

    val valid = MaintenanceValidation.validate(
      MaintenanceRequest("hudi", "clean", table = Some("db.orders"),
        retention = MaintenanceRetention(retainLast = Some(10)), confirmed = true))
    valid.table shouldBe Some("db.orders")

    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(valid.copy(
        retention = MaintenanceRetention(olderThan = Some("7d"), retainLast = Some(10))))
    }.getMessage should include ("only one retention mode")
  }

  test("strip the catalog prefix from a procedure table argument") {
    val request = MaintenanceRequest(
      "iceberg", "status", catalog = Some("lake"), table = Some("lake.db.orders"))
    MaintenanceValidation.tableWithoutCatalog(request) shouldBe "db.orders"
    MaintenanceValidation.tableParts(request) shouldBe Seq("db", "orders")
    MaintenanceValidation.target(request) shouldBe "lake.db.orders"
  }

  test("reject invalid identifiers") {
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(
        MaintenanceRequest("iceberg", "status", catalog = Some("lake"), table = Some("db..orders")))
    }
    intercept[MaintenanceUsageException] {
      MaintenanceValidation.validate(
        MaintenanceRequest("iceberg", "status", catalog = Some("lake"), table = Some("db.orders")))
      MaintenanceValidation.validate(
        MaintenanceRequest("iceberg", "status", catalog = Some("bad catalog"), table = Some("db.orders")))
    }
  }
}
