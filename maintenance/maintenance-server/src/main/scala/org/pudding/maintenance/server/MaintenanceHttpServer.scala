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

import java.io.ByteArrayOutputStream
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale
import java.util.concurrent.{ExecutorService, Executors}

import scala.util.control.NonFatal

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.internal.Logging

import org.pudding.maintenance.api.{MaintenanceException, MaintenanceUsageException}

/**
 * Dependency-free HTTP adapter for the maintenance task manager.
 *
 * Endpoints:
 *  - GET  /healthz
 *  - GET  /v1/capabilities
 *  - GET  /v1/maintenance?status=&limit=
 *  - POST /v1/maintenance
 *  - GET  /v1/maintenance/{id}
 *  - POST /v1/maintenance/{id}/cancel
 */
private[server] class MaintenanceHttpServer(
    config: MaintenanceServerConfig,
    manager: MaintenanceTaskManager) extends AutoCloseable with Logging {

  private val server = HttpServer.create(new InetSocketAddress(config.host, config.port), 0)

  private val handlerExecutor: ExecutorService = Executors.newFixedThreadPool(4)

  server.setExecutor(handlerExecutor)

  def start(): Unit = {
    server.createContext("/", new Handler)
    server.start()
  }

  def port: Int = server.getAddress.getPort

  def address: String = s"http://${config.host}:${this.port}"

  override def close(): Unit = {
    server.stop(0)
    handlerExecutor.shutdownNow()
  }

  private class Handler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        this.route(exchange)
      } catch {
        case error: RequestTooLarge =>
          MaintenanceHttpServer.this.sendJson(
            exchange, 413, MaintenanceHttpJson.error(error.getMessage))
        case error: MaintenanceUsageException =>
          MaintenanceHttpServer.this.sendJson(
            exchange, 400, MaintenanceHttpJson.error(error.getMessage))
        case error: MaintenanceException =>
          MaintenanceHttpServer.this.sendJson(
            exchange, 400, MaintenanceHttpJson.error(error.getMessage))
        case NonFatal(error) =>
          logWarning(
            s"maintenance HTTP request failed: ${error.getClass.getSimpleName}")
          MaintenanceHttpServer.this.sendJson(
            exchange, 500, MaintenanceHttpJson.error("internal server error"))
      } finally {
        exchange.close()
      }
    }

    private def route(exchange: HttpExchange): Unit = {
      val method = exchange.getRequestMethod.toUpperCase(Locale.ROOT)
      val path = Option(exchange.getRequestURI.getPath).getOrElse("/").stripSuffix("/") match {
        case "" => "/"
        case value => value
      }

      if (path == "/healthz") {
        if (method != "GET") {
          MaintenanceHttpServer.this.sendJson(
            exchange, 405, MaintenanceHttpJson.error("method not allowed"))
        } else {
          MaintenanceHttpServer.this.sendJson(
            exchange, 200, MaintenanceHttpJson.health(manager.counts))
        }
      } else if (!MaintenanceHttpServer.this.authorized(exchange)) {
        exchange.getResponseHeaders.set("WWW-Authenticate", "Bearer")
        MaintenanceHttpServer.this.sendJson(
          exchange, 401, MaintenanceHttpJson.error("unauthorized"))
      } else {
        this.routeAuthorized(exchange, method, path)
      }
    }

    private def routeAuthorized(
        exchange: HttpExchange,
        method: String,
        path: String): Unit = {
      if (path == "/v1/capabilities") {
        if (method != "GET") {
          MaintenanceHttpServer.this.sendJson(
            exchange, 405, MaintenanceHttpJson.error("method not allowed"))
        } else {
          MaintenanceHttpServer.this.sendJson(
            exchange, 200, MaintenanceHttpJson.capabilities)
        }
      } else if (path == "/v1/maintenance") {
        if (method == "GET") {
          val status = MaintenanceHttpServer.this.query(exchange, "status")
          val limit = MaintenanceHttpServer.this.query(exchange, "limit")
            .map { value =>
              try value.toInt
              catch {
                case _: NumberFormatException =>
                  throw new MaintenanceUsageException("limit must be an integer")
              }
            }.getOrElse(100)
          MaintenanceHttpServer.this.sendJson(
            exchange, 200, MaintenanceHttpJson.tasks(manager.list(status, limit)))
        } else if (method == "POST") {
          val body = MaintenanceHttpServer.this.readBody(exchange, config.maxRequestBytes)
          val request = MaintenanceHttpJson.parseRequest(body)
          val idempotencyKey =
            Option(exchange.getRequestHeaders.getFirst("Idempotency-Key"))
          val task = manager.submit(request, idempotencyKey)
          MaintenanceHttpServer.this.sendJson(
            exchange, 202, MaintenanceHttpJson.task(task))
        } else {
          MaintenanceHttpServer.this.sendJson(
            exchange, 405, MaintenanceHttpJson.error("method not allowed"))
        }
      } else if (path.startsWith("/v1/maintenance/")) {
        val suffix = path.substring("/v1/maintenance/".length)
        val cancel = suffix.endsWith("/cancel")
        val id = if (cancel) suffix.stripSuffix("/cancel").stripSuffix("/") else suffix
        if (id.isEmpty || id.contains("/")) {
          MaintenanceHttpServer.this.sendJson(
            exchange, 404, MaintenanceHttpJson.error("task not found"))
        } else if (cancel) {
          if (method != "POST") {
            MaintenanceHttpServer.this.sendJson(
              exchange, 405, MaintenanceHttpJson.error("method not allowed"))
          } else {
            manager.cancel(id) match {
              case Right(task) =>
                MaintenanceHttpServer.this.sendJson(
                  exchange, 202, MaintenanceHttpJson.task(task))
              case Left(message) if message == "task not found" =>
                MaintenanceHttpServer.this.sendJson(
                  exchange, 404, MaintenanceHttpJson.error(message))
              case Left(message) =>
                MaintenanceHttpServer.this.sendJson(
                  exchange, 409, MaintenanceHttpJson.error(message))
            }
          }
        } else if (method == "GET") {
          manager.get(id) match {
            case Some(task) =>
              MaintenanceHttpServer.this.sendJson(
                exchange, 200, MaintenanceHttpJson.task(task))
            case None =>
              MaintenanceHttpServer.this.sendJson(
                exchange, 404, MaintenanceHttpJson.error("task not found"))
          }
        } else {
          MaintenanceHttpServer.this.sendJson(
            exchange, 405, MaintenanceHttpJson.error("method not allowed"))
        }
      } else {
        MaintenanceHttpServer.this.sendJson(
          exchange, 404, MaintenanceHttpJson.error("not found"))
      }
    }
  }

  private def authorized(exchange: HttpExchange): Boolean = {
    config.authToken match {
      case None => true
      case Some(expected) =>
        val provided = Option(exchange.getRequestHeaders.getFirst("Authorization"))
          .map(_.trim)
          .filter(_.toLowerCase(Locale.ROOT).startsWith("bearer "))
          .map(_.substring("Bearer ".length).trim)
        provided.exists(value =>
          MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8)))
    }
  }

  private def query(exchange: HttpExchange, key: String): Option[String] = {
    Option(exchange.getRequestURI.getRawQuery).toSeq
      .flatMap(_.split("&").toSeq)
      .flatMap { entry =>
        entry.split("=", 2).toSeq match {
          case Seq(name, value) if name == key =>
            Some(URLDecoder.decode(value, StandardCharsets.UTF_8.name()))
          case _ => None
        }
      }
      .headOption
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  private def readBody(exchange: HttpExchange, limit: Int): String = {
    val input = exchange.getRequestBody
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    var total = 0
    var read = input.read(buffer)
    while (read >= 0) {
      total += read
      if (total > limit) {
        throw new RequestTooLarge("request body exceeds the configured limit")
      }
      output.write(buffer, 0, read)
      read = input.read(buffer)
    }
    new String(output.toByteArray, StandardCharsets.UTF_8)
  }

  private def sendJson(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    exchange.getResponseHeaders.set("Cache-Control", "no-store")
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    val output = exchange.getResponseBody
    try output.write(bytes)
    finally output.close()
  }

  private class RequestTooLarge(message: String) extends RuntimeException(message)
}
