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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.CountDownLatch

import org.pudding.maintenance.api.MaintenanceUsageException

/**
 * Standalone HTTP maintenance service.
 *
 * Start it through `pudding maintenance-server` (or spark-submit with the maintenance server
 * jar and format runtime jars) from a controlled network. The bearer token is read from
 * `PUDDING_MAINTENANCE_TOKEN` or `--auth-token-file`; it is never accepted on the command line.
 */
object MaintenanceServer {

  def main(args: Array[String]): Unit = {
    try {
      val config = this.parse(args)
      val store = new MaintenanceTaskStore(config.stateFile)
      val audit = new MaintenanceAuditLog(config.auditFile)
      val executor = new SparkMaintenanceExecutor(config)
      val manager = new MaintenanceTaskManager(store, audit, executor, config.maxTasks)
      val server = new MaintenanceHttpServer(config, manager)
      val latch = new CountDownLatch(1)
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(new Thread(
        new Runnable {
          override def run(): Unit = {
            server.close()
            manager.close()
            latch.countDown()
          }
        },
        "pudding-maintenance-shutdown"))
      // scalastyle:on runtimeaddshutdownhook
      server.start()
      // scalastyle:off println
      println(s"maintenance server listening on ${server.address}")
      // scalastyle:on println
      latch.await()
    } catch {
      case error: MaintenanceUsageException =>
        // scalastyle:off println
        System.err.println(s"maintenance-server: ${error.getMessage}")
        // scalastyle:on println
        System.exit(2)
      case error: Exception =>
        // scalastyle:off println
        System.err.println(
          s"maintenance-server: ${Option(error.getMessage).getOrElse(error.getClass.getSimpleName)}")
        // scalastyle:on println
        System.exit(1)
    }
  }

  private[server] def parse(args: Array[String], env: Map[String, String] = sys.env)
      : MaintenanceServerConfig = {
    var host = "127.0.0.1"
    var port = 8080
    var stateFile = "maintenance-tasks.json"
    var auditFile = "maintenance-audit.jsonl"
    var authTokenFile: Option[String] = None
    var allowAnonymous = false
    var confFile: Option[String] = None
    var master: Option[String] = None
    var hadoopConf: Option[String] = None
    var maxRequestBytes = 1024 * 1024
    var maxTasks = 1000
    var index = 0

    def value(): String = {
      index += 1
      if (index >= args.length) {
        throw new MaintenanceUsageException(s"${args(index - 1)} requires a value")
      }
      args(index)
    }

    while (index < args.length) {
      args(index) match {
        case "-h" | "--help" =>
          printUsage()
          System.exit(0)
        case "--host" => host = value()
        case "--port" => port = parseInt(value(), "--port")
        case "--state-file" => stateFile = value()
        case "--audit-file" => auditFile = value()
        case "--auth-token-file" => authTokenFile = Some(value())
        case "--allow-anonymous" => allowAnonymous = true
        case "--conf-file" => confFile = Some(value())
        case "--master" => master = Some(value())
        case "--hadoop-conf" => hadoopConf = Some(value())
        case "--max-request-bytes" => maxRequestBytes = parseInt(value(), "--max-request-bytes")
        case "--max-tasks" => maxTasks = parseInt(value(), "--max-tasks")
        case other => throw new MaintenanceUsageException(s"unknown option: $other")
      }
      index += 1
    }

    val token = authTokenFile.map(readToken).orElse(env.get("PUDDING_MAINTENANCE_TOKEN")
      .map(_.trim).filter(_.nonEmpty))
    MaintenanceServerConfig(
      host = host,
      port = port,
      stateFile = Paths.get(stateFile).toAbsolutePath.normalize(),
      auditFile = Paths.get(auditFile).toAbsolutePath.normalize(),
      authToken = token,
      allowAnonymous = allowAnonymous,
      confFile = confFile,
      master = master,
      hadoopConf = hadoopConf,
      maxRequestBytes = maxRequestBytes,
      maxTasks = maxTasks).validate()
  }

  private def parseInt(value: String, option: String): Int = {
    try value.toInt
    catch {
      case _: NumberFormatException =>
        throw new MaintenanceUsageException(s"$option must be an integer")
    }
  }

  private def readToken(path: String): String = {
    val file = Paths.get(path)
    if (!Files.isRegularFile(file)) {
      throw new MaintenanceUsageException(s"--auth-token-file is not a file: $path")
    }
    val token = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim
    if (token.isEmpty) {
      throw new MaintenanceUsageException("--auth-token-file is empty")
    }
    token
  }

  private def printUsage(): Unit = {
    // scalastyle:off println
    println(
      """Usage: pudding maintenance-server [options]
        |
        |Options:
        |  --host <host>                Bind host (default 127.0.0.1)
        |  --port <port>                Bind port (default 8080; 0 picks a free port)
        |  --state-file <file>          Durable task state JSON
        |  --audit-file <file>          Append-only audit JSONL
        |  --auth-token-file <file>     Bearer token file; or set PUDDING_MAINTENANCE_TOKEN
        |  --allow-anonymous            Allow requests without a token (local use only)
        |  --conf-file <file>           Spark properties containing only spark.* keys
        |  --master <url>               Spark master for maintenance sessions
        |  --hadoop-conf <dir>          XML Hadoop configuration directory
        |  --max-request-bytes <n>      Request body limit (default 1048576)
        |  --max-tasks <n>              Retained task limit (default 1000)
        |  -h, --help""".stripMargin)
    // scalastyle:on println
  }
}
