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

import java.nio.file.{Path, Paths}

import org.pudding.maintenance.api.MaintenanceUsageException

case class MaintenanceServerConfig(
    host: String = "127.0.0.1",
    port: Int = 8080,
    stateFile: Path = Paths.get("maintenance-tasks.json"),
    auditFile: Path = Paths.get("maintenance-audit.jsonl"),
    authToken: Option[String] = None,
    allowAnonymous: Boolean = false,
    confFile: Option[String] = None,
    master: Option[String] = None,
    hadoopConf: Option[String] = None,
    maxRequestBytes: Int = 1024 * 1024,
    maxTasks: Int = 1000) {

  def validate(): MaintenanceServerConfig = {
    if (host == null || host.trim.isEmpty) {
      throw new MaintenanceUsageException("server host can not be empty")
    }
    if (port < 0 || port > 65535) {
      throw new MaintenanceUsageException("server port must be between 0 and 65535")
    }
    if (stateFile == null || auditFile == null) {
      throw new MaintenanceUsageException("state-file and audit-file are required")
    }
    if (maxRequestBytes <= 0) {
      throw new MaintenanceUsageException("max-request-bytes must be positive")
    }
    if (maxTasks <= 0) {
      throw new MaintenanceUsageException("max-tasks must be positive")
    }
    if (authToken.forall(_.trim.isEmpty) && !allowAnonymous) {
      throw new MaintenanceUsageException(
        "an auth token is required; set PUDDING_MAINTENANCE_TOKEN, use --auth-token-file, " +
          "or explicitly pass --allow-anonymous for local use")
    }
    this.copy(
      host = host.trim,
      authToken = authToken.map(_.trim).filter(_.nonEmpty))
  }

  override def toString: String =
    s"MaintenanceServerConfig(host=$host, port=$port, stateFile=$stateFile, " +
      s"auditFile=$auditFile, authToken=${if (authToken.isDefined) "<set>" else "<none>"}, " +
      s"allowAnonymous=$allowAnonymous)"
}
