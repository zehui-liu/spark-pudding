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

package org.pudding.core.connection

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for secret reference resolution. Resolution stays in core because the job engine and
 * every provider use it.
 */
class SecretResolverTest extends AnyFunSuite with Matchers {

  test("resolve environment and file secret references") {
    val home = sys.env("HOME")
    SecretResolver.resolve("${secret:env/HOME}") shouldBe home
    SecretResolver.resolve("prefix-${secret:env/HOME}-suffix") shouldBe s"prefix-$home-suffix"
    SecretResolver.resolve("plain value") shouldBe "plain value"

    val secretFile = Files.createTempFile("pudding-secret", ".properties")
    try {
      Files.write(
        secretFile,
        "password=s3cret\nusername=pudding\n".getBytes(StandardCharsets.UTF_8))
      SecretResolver.resolve(s"${'$'}{secret:file:${secretFile.toString}#password}") shouldBe "s3cret"
      SecretResolver.resolveMap(Map("user" -> "${secret:env/HOME}"))("user") shouldBe home
      SecretResolver.resolveConfig(
        Some(Map("nested" -> Map("token" -> "${secret:env/HOME}"))))
        .get("nested").asInstanceOf[Map[String, Any]]("token") shouldBe home
    } finally {
      Files.deleteIfExists(secretFile)
    }
  }

  test("reject a missing secret") {
    val error = intercept[PuddingException] {
      SecretResolver.resolve("${secret:env/PUDDING_TEST_SECRET_THAT_DOES_NOT_EXIST}")
    }
    error.getMessage should include ("secret environment variable is not set")
  }
}
