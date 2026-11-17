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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceCliTest extends AnyFunSuite with Matchers {

  private final case class Output(code: Int, out: String, err: String)

  private def run(args: String*): Output = {
    val out = new ByteArrayOutputStream()
    val err = new ByteArrayOutputStream()
    val code = MaintenanceCli.run(
      args.toArray,
      new PrintStream(out, true, StandardCharsets.UTF_8.name()),
      new PrintStream(err, true, StandardCharsets.UTF_8.name()))
    Output(
      code,
      out.toString(StandardCharsets.UTF_8.name()),
      err.toString(StandardCharsets.UTF_8.name()))
  }

  test("print help without starting Spark") {
    val result = run("--help")
    result.code shouldBe 0
    result.out should include ("rewrite-data-files")
    result.out should include ("--confirm")
  }

  test("return the usage exit code for invalid requests") {
    val result = run("iceberg", "status")
    result.code shouldBe 2
    result.err should include ("--catalog")

    val unknown = run("iceberg", "status", "--not-an-option")
    unknown.code shouldBe 2
    unknown.err should include ("unknown option")

    val json = run("iceberg", "status", "--json")
    json.code shouldBe 2
    json.out should include ("\"error\"")
  }
}
