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

package org.pudding.client.submit

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for job configuration type normalization and file loading.
 */
class JobConfigurationTest extends AnyFunSuite with Matchers {

  test("normalize text configuration types") {
    JobConfiguration.fromText("yaml", "job:\n  mode: batch\n").getType shouldBe "yaml"
    JobConfiguration.fromText("JSON", "{}").getType shouldBe "json"
    JobConfiguration.fromText("hocon", "job { mode = \"etl\" }").getType shouldBe "hocon"
    JobConfiguration.fromText("conf", "job { mode = \"etl\" }").getType shouldBe "hocon"
    JobConfiguration.fromText(".yaml", "job:\n  mode: batch\n").getType shouldBe "yaml"
  }

  test("infer the type from supported file extensions") {
    Seq("yaml" -> "yaml", "json" -> "json", "hocon" -> "hocon", "conf" -> "hocon").foreach {
      case (extension, expected) =>
        val file = Files.createTempFile("pudding-config-", s".$extension")
        try {
          Files.write(file, "job { mode = \"etl\" }\n".getBytes(StandardCharsets.UTF_8))
          JobConfiguration.fromFile(file).getType shouldBe expected
        } finally {
          Files.deleteIfExists(file)
        }
    }
  }

  test("reject unsupported extensions, missing files and empty content") {
    intercept[SubmissionException] {
      JobConfiguration.fromText("xml", "<job/>")
    }
    intercept[SubmissionException] {
      JobConfiguration.fromText("yaml", "   ")
    }
    intercept[SubmissionException] {
      JobConfiguration.fromFile(Files.createTempDirectory("missing-job").resolve("job.yaml"))
    }

    val noExtension = Files.createTempFile("pudding-config-", "")
    try {
      intercept[SubmissionException] {
        JobConfiguration.fromFile(noExtension)
      }
    } finally {
      Files.deleteIfExists(noExtension)
    }
  }
}
