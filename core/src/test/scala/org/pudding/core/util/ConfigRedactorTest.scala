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

package org.pudding.core.util

import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JValue, NoTypeHints}
import org.pudding.core.PuddingException
import org.pudding.core.configure.{JobConf, JobMode, SourceConf}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Covers credential detection, redacted rendering and recursive map redaction.
 */
class ConfigRedactorTest extends AnyFunSuite with Matchers {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  test("redact sensitive keys at any depth") {
    val config = Map[String, Any](
      "password" -> "top-secret",
      "table" -> "users",
      "source" -> Map(
        "user" -> "root",
        "options" -> Map(
          "PASSWD" -> "nested-secret",
          "header" -> "true"
        )
      ),
      "headers" -> Map(
        "Authorization" -> "Bearer header-secret",
        "Content-Type" -> "application/json"
      ),
      "instances" -> Seq(
        Map("apiToken" -> "list-secret"),
        Map("name" -> "visible")
      )
    )

    val json = ConfigRedactor.redactToJson(config)

    json should not include ("top-secret")
    json should not include ("nested-secret")
    json should not include ("header-secret")
    json should not include ("list-secret")
    json should include ("users")
    json should include ("root")
    json should include ("application/json")
    json should include ("visible")
    json should include (ConfigRedactor.REDACTED)
    JsonMethods.parse(json)
  }

  test("canonical redacted JSON is stable across map order and secret values") {
    val first = Map[String, Any](
      "table" -> "users",
      "password" -> "first-secret",
      "options" -> Map("b" -> 2, "a" -> 1))
    val second = Map[String, Any](
      "options" -> Map("a" -> 1, "b" -> 2),
      "password" -> "second-secret",
      "table" -> "users")

    val firstJson = ConfigRedactor.canonicalRedactedJson(first)
    val secondJson = ConfigRedactor.canonicalRedactedJson(second)

    firstJson shouldBe secondJson
    firstJson should not include ("first-secret")
    firstJson should not include ("second-secret")
    firstJson should include ("users")
  }

  test("redact common sensitive key spellings") {
    val sensitiveKeys = Seq(
      "password",
      "passwd",
      "passphrase",
      "pwd",
      "pass",
      "secret",
      "secretKey",
      "access-key",
      "api_token",
      "privateKey",
      "credentials",
      "Authorization",
      "Cookie",
      "account_key",
      "basic.auth.user.info",
      "sasl.jaas.config"
    )
    val config = sensitiveKeys.zipWithIndex.map {
      case (key, index) => key -> s"value-$index"
    }.toMap + ("keyName" -> "visible-key-name")

    val json = ConfigRedactor.redactToJson(config)

    sensitiveKeys.indices.foreach { index =>
      json should not include (s"value-$index")
    }
    json should include ("visible-key-name")
  }

  test("redact credentials embedded in string values") {
    val config = Map[String, Any](
      "url" ->
        "jdbc:mysql://alice:url-secret@localhost:3306/db?user=alice&password=query-secret&useSSL=false",
      "loginConfig" ->
        """PlainLoginModule required username="alice" password="jaas-secret";""",
      "header" -> "Authorization: Bearer bearer-secret"
    )

    val json = ConfigRedactor.redactToJson(config)

    json should not include ("url-secret")
    json should not include ("query-secret")
    json should not include ("jaas-secret")
    json should not include ("bearer-secret")
    json should include ("alice")
    json should include ("useSSL=false")
    json should include ("PlainLoginModule")
    json should include ("Authorization")
  }

  test("redact credentials in individual log lines") {
    val cases = Seq(
      "password=line-secret rest" -> "line-secret",
      "spark.hadoop.fs.s3a.secret.key -> arrow-secret" -> "arrow-secret",
      "secretKey: camel-secret" -> "camel-secret",
      "access.key=access-secret" -> "access-secret",
      """{"api_key": "json-secret", "table": "users"}""" -> "json-secret",
      "jdbc:mysql://bob:url-line-secret@localhost/db" -> "url-line-secret"
    )

    cases.foreach { case (line, secret) =>
      val redacted = ConfigRedactor.redactLine(line)
      redacted should not include (secret)
      redacted should include (ConfigRedactor.REDACTED)
    }

    val ordinary = "INFO loaded 12 rows from file:///tmp/input.csv"
    ConfigRedactor.redactLine(ordinary) shouldBe ordinary
    ConfigRedactor.redactLine(null) shouldBe null
  }

  test("redact a matched prefix containing a replacement token") {
    val line = "jdbc:mysql://ali$ce:url-secret@localhost/db"

    val redacted = ConfigRedactor.redactLine(line)

    redacted should not include ("url-secret")
    redacted should include ("ali$ce")
    redacted should include (ConfigRedactor.REDACTED)
  }

  test("fail closed when canonical redaction can not serialize a value") {
    val throwingFormats = DefaultFormats + new CustomSerializer[Unserializable](_ =>
      (PartialFunction.empty[JValue, Unserializable],
        { case _: Unserializable =>
          throw new IllegalStateException("serialization failed")
        }))

    val error = intercept[PuddingException] {
      ConfigRedactor.canonicalRedactedJson(Unserializable("value"), throwingFormats)
    }

    error.getMessage should include ("stable fingerprint")
  }

  test("keep non-sensitive configuration readable") {
    val config = Map[String, Any](
      "url" -> "jdbc:mysql://localhost:3306/pudding?useSSL=false",
      "table" -> "users",
      "user" -> "root",
      "description" -> "passwordless authentication is disabled",
      "keyName" -> "order_id"
    )

    val json = ConfigRedactor.redactToJson(config)

    json should include ("jdbc:mysql://localhost:3306/pudding?useSSL=false")
    json should include ("passwordless authentication is disabled")
    json should include ("order_id")
    json should not include (ConfigRedactor.REDACTED)
  }

  test("redact configuration in case classes") {
    val source = SourceConf(
      `type` = "mysql-source",
      describe = Some("mysql source"),
      id = "source-1",
      config = Some(Map(
        "url" -> "jdbc:mysql://localhost:3306/pudding",
        "table" -> "users",
        "user" -> "root",
        "password" -> "source-secret"
      ))
    )
    val job = JobConf(
      sparkHome = "/opt/spark",
      sparkPuddingHome = None,
      appName = "redaction test",
      master = "local[2]",
      deployMode = "client",
      describe = Some("redaction test"),
      config = Some(Map(
        "spark.master" -> "local[2]",
        "spark.hadoop.fs.s3a.secret.key" -> "s3-secret"
      )),
      mode = JobMode.Batch
    )

    val sourceJson = ConfigRedactor.redactToJson(source)
    val jobJson = ConfigRedactor.redactToJson(job)

    sourceJson should not include ("source-secret")
    sourceJson should include ("mysql-source")
    sourceJson should include ("source-1")
    sourceJson should include ("users")
    sourceJson should include ("root")

    jobJson should not include ("s3-secret")
    jobJson should include ("local[2]")
  }

  private case class Unserializable(value: String)
}
