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

package org.pudding.core

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for safe job configuration selection.
 */
class JobConfigSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val jobText = "job:\n  appName: test\n"

  private var jobFile: Path = _

  override def beforeAll(): Unit = {
    jobFile = Files.createTempFile("pudding-job-config-", ".conf")
    Files.write(jobFile, jobText.getBytes(StandardCharsets.UTF_8))
  }

  override def afterAll(): Unit = {
    if (jobFile != null) {
      Files.deleteIfExists(jobFile)
    }
  }

  test("uses inline job text") {
    JobConfigSource.resolve(Some(jobText), None, None, Map.empty) shouldBe jobText
  }

  test("reads job text from an environment variable") {
    JobConfigSource.resolve(
      None,
      Some("PUDDING_JOB_CONFIG"),
      None,
      Map("PUDDING_JOB_CONFIG" -> jobText)) shouldBe jobText
  }

  test("reads job text from a file") {
    JobConfigSource.resolve(None, None, Some(jobFile.toString), Map.empty) shouldBe jobText
  }

  test("requires exactly one configuration source") {
    val missing = intercept[PuddingException] {
      JobConfigSource.resolve(None, None, None, Map.empty)
    }
    missing.getMessage should include ("one of --jobText, --jobEnv or --jobFile is required")

    val both = intercept[PuddingException] {
      JobConfigSource.resolve(Some(jobText), Some("PUDDING_JOB_CONFIG"), None,
        Map("PUDDING_JOB_CONFIG" -> jobText))
    }
    both.getMessage should include ("exactly one")
    both.getMessage should not include (jobText)

    val textAndFile = intercept[PuddingException] {
      JobConfigSource.resolve(Some(jobText), None, Some(jobFile.toString), Map.empty)
    }
    textAndFile.getMessage should include ("exactly one")

    val envAndFile = intercept[PuddingException] {
      JobConfigSource.resolve(
        None,
        Some("PUDDING_JOB_CONFIG"),
        Some(jobFile.toString),
        Map("PUDDING_JOB_CONFIG" -> jobText))
    }
    envAndFile.getMessage should include ("exactly one")
  }

  test("rejects missing and empty job files") {
    val missingFile = jobFile.getParent.resolve("pudding-job-config-missing.conf")
    Files.deleteIfExists(missingFile)
    val missing = intercept[PuddingException] {
      JobConfigSource.resolve(None, None, Some(missingFile.toString), Map.empty)
    }
    missing.getMessage should include ("does not exist")

    val emptyFile = Files.createTempFile("pudding-job-config-empty-", ".conf")
    try {
      val empty = intercept[PuddingException] {
        JobConfigSource.resolve(None, None, Some(emptyFile.toString), Map.empty)
      }
      empty.getMessage should include ("is empty")
    } finally {
      Files.deleteIfExists(emptyFile)
    }
  }

  test("rejects a missing or empty environment variable") {
    val missing = intercept[PuddingException] {
      JobConfigSource.resolve(None, Some("PUDDING_JOB_CONFIG"), None, Map.empty)
    }
    missing.getMessage should include ("is not set: PUDDING_JOB_CONFIG")

    val empty = intercept[PuddingException] {
      JobConfigSource.resolve(None, Some("PUDDING_JOB_CONFIG"), None,
        Map("PUDDING_JOB_CONFIG" -> "  \n"))
    }
    empty.getMessage should include ("is empty: PUDDING_JOB_CONFIG")
  }

  test("rejects an invalid environment variable name") {
    val error = intercept[PuddingException] {
      JobConfigSource.resolve(
        None, Some("invalid-name"), None, Map("invalid-name" -> jobText))
    }
    error.getMessage should include ("invalid job environment variable name")
  }

  test("blank inline text is treated as missing") {
    val error = intercept[PuddingException] {
      JobConfigSource.resolve(Some("  \n"), None, None, Map.empty)
    }
    error.getMessage should include ("one of --jobText, --jobEnv or --jobFile is required")
  }
}
