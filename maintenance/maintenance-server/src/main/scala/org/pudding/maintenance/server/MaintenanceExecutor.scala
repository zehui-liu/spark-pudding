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

package org.pudding.maintenance.server

import org.pudding.maintenance.api._
import org.pudding.maintenance.spark.{SparkMaintenanceSession, SparkTableMaintenanceService}

/**
 * Executes one validated maintenance request.
 */
private[server] trait MaintenanceExecutor extends AutoCloseable {

  def execute(request: MaintenanceRequest): MaintenanceResult

  override def close(): Unit = ()
}

/**
 * Creates an isolated Spark maintenance session for each task. The session is stopped after the
 * task so a failed table action does not leave a Spark application behind.
 */
private[server] class SparkMaintenanceExecutor(
    config: MaintenanceServerConfig) extends MaintenanceExecutor {

  override def execute(request: MaintenanceRequest): MaintenanceResult = {
    val normalized = MaintenanceValidation.validate(request)
    val session = SparkMaintenanceSession.create(
      normalized.format,
      s"pudding-maintenance-${normalized.format}-${normalized.action}",
      config.confFile,
      config.master,
      config.hadoopConf)
    try {
      new SparkTableMaintenanceService(session).execute(normalized)
    } finally {
      session.stop()
    }
  }
}
