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

package org.pudding.maintenance.cli

import org.pudding.maintenance.api.MaintenanceUsageException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CliArgumentsTest extends AnyFunSuite with Matchers {

  test("parse common, repeatable and inline options") {
    val parsed = CliArguments.parse(Array(
      "iceberg",
      "rewrite-data-files",
      "--catalog", "lake",
      "--table=db.orders",
      "--dry-run",
      "--option", "target-file-size-bytes=134217728",
      "--option=sink.parallelism=4",
      "--json"))

    parsed.format shouldBe "iceberg"
    parsed.action shouldBe "rewrite-data-files"
    parsed.target.catalog shouldBe Some("lake")
    parsed.target.table shouldBe Some("db.orders")
    parsed.flags.dryRun shouldBe true
    parsed.flags.json shouldBe true
    parsed.options shouldBe Map(
      "target-file-size-bytes" -> "134217728",
      "sink.parallelism" -> "4")
  }

  test("parse retention and Hudi target options") {
    val parsed = CliArguments.parse(Array(
      "hudi", "clean", "--table", "db.orders", "--retain-last", "10", "--confirm"))

    parsed.target.table shouldBe Some("db.orders")
    parsed.retention.retainLast shouldBe Some(10)
    parsed.flags.confirmed shouldBe true
  }

  test("help is accepted anywhere") {
    CliArguments.parse(Array("--help")).flags.help shouldBe true
    CliArguments.parse(Array("iceberg", "status", "--help")).flags.help shouldBe true
  }

  test("reject malformed options") {
    intercept[MaintenanceUsageException] {
      CliArguments.parse(Array("iceberg", "status", "--unknown"))
    }.getMessage should include ("unknown option")

    intercept[MaintenanceUsageException] {
      CliArguments.parse(Array("iceberg", "status", "--catalog"))
    }.getMessage should include ("requires a value")

    intercept[MaintenanceUsageException] {
      CliArguments.parse(Array(
        "iceberg", "status", "--option", "a=b", "--option", "a=c"))
    }.getMessage should include ("duplicate --option")

    intercept[MaintenanceUsageException] {
      CliArguments.parse(Array("iceberg", "status", "--option", "missing-equals"))
    }.getMessage should include ("key=value")

    intercept[MaintenanceUsageException] {
      CliArguments.parse(Array("iceberg", "status", "--retain-last", "x"))
    }.getMessage should include ("integer")
  }
}
