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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class YarnClusterRunningApplicationTest extends AnyFunSuite with Matchers {

  private val appId = "application_1_2"

  private val output =
    "26/11/11 00:00:00 INFO Client: " +
      s"Submitting application $appId to ResourceManager\n"

  test("track a YARN cluster application until it succeeds") {
    withServer { calls =>
      val state = if (calls.getAndIncrement() == 0) "RUNNING" else "FINISHED"
      val finalStatus = if (state == "RUNNING") "UNDEFINED" else "SUCCEEDED"
      statusBody(state, finalStatus)
    } { url =>
      val process = new FakeProcess(0, output)
      val application = YarnClusterRunningApplication.start(process, "yarn", url, 20L)
      process.finish()

      val result = application.await()

      result.getState shouldBe ApplicationState.FINISHED
      result.getApplicationId.get shouldBe appId
      result.getError.isPresent shouldBe false
    }
  }

  test("report a failed YARN final status") {
    withServer { calls =>
      if (calls.getAndIncrement() == 0) {
        statusBody("RUNNING", "UNDEFINED")
      } else {
        statusBody("FINISHED", "FAILED", "driver exited with code 1")
      }
    } { url =>
      val process = new FakeProcess(0, output)
      val application = YarnClusterRunningApplication.start(process, "yarn", url, 20L)
      process.finish()

      val result = application.await()

      result.getState shouldBe ApplicationState.FAILED
      result.getError.get.getMessage should include ("driver exited with code 1")
    }
  }

  test("fail when spark-submit exits non-zero") {
    val process = new FakeProcess(1, output)
    val application = YarnClusterRunningApplication.start(
      process, "yarn", "http://127.0.0.1:1", 20L)
    process.finish()

    val result = application.await()

    result.getState shouldBe ApplicationState.FAILED
    result.getError.get.getMessage should include ("exited with code 1")
  }

  test("report LOST when the application never appears in the ResourceManager") {
    withServer(_ => "{\"app\":null}", missingStatus = true) { url =>
      val process = new FakeProcess(0, output)
      val application = YarnClusterRunningApplication.start(process, "yarn", url, 10L)
      process.finish()

      val result = application.await()

      result.getState shouldBe ApplicationState.LOST
      result.getError.get.getMessage should include ("does not report application")
    }
  }

  test("cancel a YARN cluster application through the ResourceManager") {
    withServer { _ =>
      statusBody("RUNNING", "UNDEFINED")
    } { url =>
      val process = new FakeProcess(0, output)
      val application = YarnClusterRunningApplication.start(process, "yarn", url, 20L)
      process.finish()
      waitForApplicationId(application)
      application.cancel()
      val result = application.await()

      result.getState shouldBe ApplicationState.KILLED
    }
  }

  private def waitForApplicationId(application: RunningApplication): Unit = {
    val deadline = System.currentTimeMillis() + 2000L
    while (!application.getApplicationId.isPresent &&
      System.currentTimeMillis() < deadline) {
      Thread.sleep(10L)
    }
    application.getApplicationId.isPresent shouldBe true
  }

  private def statusBody(
      state: String,
      finalStatus: String,
      diagnostics: String = ""): String = {
    s"""{"app":{"state":"$state","finalStatus":"$finalStatus","diagnostics":"$diagnostics"}}"""
  }

  private def withServer(
      body: AtomicInteger => String,
      missingStatus: Boolean = false)(
      testBody: String => Unit): Unit = {
    val calls = new AtomicInteger()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      s"/ws/v1/cluster/apps/$appId/state",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          exchange.sendResponseHeaders(200, -1)
          exchange.close()
        }
      })
    server.createContext(
      s"/ws/v1/cluster/apps/$appId",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          if (missingStatus) {
            exchange.sendResponseHeaders(404, -1)
          } else {
            val payload = body(calls).getBytes(StandardCharsets.UTF_8)
            exchange.getResponseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, payload.length)
            val stream = exchange.getResponseBody
            try stream.write(payload)
            finally stream.close()
          }
          exchange.close()
        }
      })
    server.start()
    try {
      testBody(s"http://127.0.0.1:${server.getAddress.getPort}")
    } finally {
      server.stop(0)
    }
  }

  private class FakeProcess(exitCode: Int, output: String) extends Process {

    private val done = new CountDownLatch(1)

    private val stdout = new ByteArrayInputStream(output.getBytes(StandardCharsets.UTF_8))

    @volatile
    private var running = true

    override def getOutputStream: OutputStream = new ByteArrayOutputStream()

    override def getInputStream: InputStream = stdout

    override def getErrorStream: InputStream = new ByteArrayInputStream(new Array[Byte](0))

    override def waitFor(): Int = {
      done.await()
      exitCode
    }

    override def exitValue(): Int = {
      if (running) {
        throw new IllegalThreadStateException("process has not exited")
      }
      exitCode
    }

    override def destroy(): Unit = finish()

    def finish(): Unit = {
      running = false
      done.countDown()
    }
  }
}
