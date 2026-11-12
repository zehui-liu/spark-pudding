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

package org.pudding.maintenance.server

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.json4s.JString
import org.json4s.jackson.JsonMethods.parse
import org.pudding.maintenance.api._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceHttpServerTest extends AnyFunSuite with Matchers {

  private val token = "test-token"

  private def response(
      method: String,
      url: String,
      body: Option[String] = None,
      idempotencyKey: Option[String] = None,
      authorization: Option[String] = Some(s"Bearer $token")): (Int, String) = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    authorization.foreach(value =>
      connection.setRequestProperty("Authorization", value))
    idempotencyKey.foreach(value =>
      connection.setRequestProperty("Idempotency-Key", value))
    if (body.isDefined) {
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json")
      val output = connection.getOutputStream
      try output.write(body.get.getBytes(StandardCharsets.UTF_8))
      finally output.close()
    }
    val status = connection.getResponseCode
    val stream = if (status >= 400) connection.getErrorStream else connection.getInputStream
    val text = this.read(stream)
    connection.disconnect()
    status -> text
  }

  private def read(input: InputStream): String = {
    if (input == null) {
      ""
    } else {
      val output = new ByteArrayOutputStream()
      val buffer = new Array[Byte](4096)
      var count = input.read(buffer)
      while (count >= 0) {
        output.write(buffer, 0, count)
        count = input.read(buffer)
      }
      input.close()
      new String(output.toByteArray, StandardCharsets.UTF_8)
    }
  }

  private def waitFor(url: String, timeoutMs: Long = 5000): String = {
    val deadline = System.currentTimeMillis() + timeoutMs
    var body = ""
    while (System.currentTimeMillis() < deadline) {
      val (status, text) = this.response("GET", url)
      body = text
      if (status == 200 && (text.contains("\"status\":\"succeeded\"") ||
        text.contains("\"succeeded\""))) {
        return text
      }
      Thread.sleep(20L)
    }
    body
  }

  test("serve health, capabilities, submission, status and auth") {
    val workspace = Files.createTempDirectory("maintenance-http")
    val config = MaintenanceServerConfig(
      host = "127.0.0.1",
      port = 0,
      stateFile = workspace.resolve("tasks.json"),
      auditFile = workspace.resolve("audit.jsonl"),
      authToken = Some(token)).validate()
    val manager = new MaintenanceTaskManager(
      new MaintenanceTaskStore(config.stateFile),
      new MaintenanceAuditLog(config.auditFile),
      new MaintenanceExecutor {
        override def execute(request: MaintenanceRequest): MaintenanceResult =
          MaintenanceResult(
            format = request.format,
            action = request.action,
            target = "lake.db.orders",
            dryRun = false,
            executed = true,
            statements = Seq("CALL lake.system.rewrite_data_files(...)"))
      })
    val server = new MaintenanceHttpServer(config, manager)
    try {
      server.start()
      val base = server.address

      val (healthStatus, health) = this.response("GET", s"$base/healthz", authorization = None)
      healthStatus shouldBe 200
      health should include ("UP")

      val (unauthorizedStatus, _) = this.response(
        "GET", s"$base/v1/capabilities", authorization = None)
      unauthorizedStatus shouldBe 401

      val (capabilityStatus, capabilities) = this.response("GET", s"$base/v1/capabilities")
      capabilityStatus shouldBe 200
      capabilities should include ("iceberg")

      val payload =
        """{"format":"iceberg","action":"status","catalog":"lake","table":"db.orders"}"""
      val (submitStatus, submitted) = this.response(
        "POST", s"$base/v1/maintenance", Some(payload), Some("idem-1"))
      submitStatus shouldBe 202
      val id = (parse(submitted) \ "id") match {
        case JString(value) => value
        case _ => fail("task id missing from response")
      }
      val completed = waitFor(s"$base/v1/maintenance/$id")
      completed should include ("succeeded")

      val (repeatStatus, repeated) = this.response(
        "POST", s"$base/v1/maintenance", Some(payload), Some("idem-1"))
      repeatStatus shouldBe 202
      (parse(repeated) \ "id") shouldBe JString(id)

      val (invalidStatus, _) = this.response(
        "POST", s"$base/v1/maintenance", Some("""{"format":"iceberg"}"""))
      invalidStatus shouldBe 400
    } finally {
      server.close()
      manager.close()
    }
  }
}
