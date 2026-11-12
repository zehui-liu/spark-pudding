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

package org.pudding.maintenance.api

import java.time.{Duration, Instant}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceValuesTest extends AnyFunSuite with Matchers {

  test("parse maintenance durations and byte sizes") {
    MaintenanceValues.duration("7d") shouldBe Duration.ofDays(7)
    MaintenanceValues.duration("90m") shouldBe Duration.ofMinutes(90)
    MaintenanceValues.size("128MB") shouldBe 128L * 1024L * 1024L
    MaintenanceValues.size("1G") shouldBe 1024L * 1024L * 1024L
    MaintenanceValues.size("4096") shouldBe 4096L
  }

  test("format older-than as a UTC timestamp") {
    val now = Instant.parse("2026-01-15T12:00:00Z")
    MaintenanceValues.olderThanTimestamp("7d", now) shouldBe
      "2026-01-08 12:00:00.000"
  }

  test("reject durations and sizes that overflow the storage range") {
    intercept[MaintenanceUsageException] {
      MaintenanceValues.duration("9223372036854775807d")
    }.getMessage should include ("out of range")

    intercept[MaintenanceUsageException] {
      MaintenanceValues.size("9223372036854775807TB")
    }.getMessage should include ("out of range")
  }

  test("reject invalid durations and sizes") {
    intercept[MaintenanceUsageException] {
      MaintenanceValues.duration("7")
    }
    intercept[MaintenanceUsageException] {
      MaintenanceValues.duration("0d")
    }
    intercept[MaintenanceUsageException] {
      MaintenanceValues.size("128XB")
    }
  }

  test("normalize formats and actions") {
    MaintenanceValues.normalize(" Iceberg ") shouldBe "iceberg"
    MaintenanceValues.trimmed(Some("  ")) shouldBe None
  }
}
