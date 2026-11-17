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

package org.pudding.maintenance.cli

import org.pudding.maintenance.api.MaintenanceResult
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceJsonTest extends AnyFunSuite with Matchers {

  test("render a maintenance result as JSON") {
    val json = MaintenanceJson.render(MaintenanceResult(
      format = "iceberg",
      action = "status",
      target = "lake.db.orders",
      dryRun = false,
      executed = true,
      statements = Seq("SELECT 1"),
      summary = Map("files.data_files" -> "3", "a\"b" -> "line\nnext"),
      rows = Seq(Map("query" -> "files", "data_files" -> "3")),
      message = "ok"))

    json should include ("\"format\":\"iceberg\"")
    json should include ("\"executed\":true")
    json should include ("\"statements\":[\"SELECT 1\"]")
    json should include ("\"a\\\"b\":\"line\\nnext\"")
    json should include ("\"rows\":[{\"data_files\":\"3\",\"query\":\"files\"}]")
  }

  test("render errors as JSON") {
    MaintenanceJson.error("bad \"table\"") shouldBe "{\"error\":\"bad \\\"table\\\"\"}"
  }
}
