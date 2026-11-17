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

import org.pudding.maintenance.api.{MaintenanceRequest, MaintenanceResult}

object MaintenanceTaskStatus {
  val Queued = "queued"
  val Running = "running"
  val Succeeded = "succeeded"
  val Failed = "failed"
  val Cancelled = "cancelled"

  val All: Set[String] = Set(Queued, Running, Succeeded, Failed, Cancelled)
}

/**
 * Durable view of one submitted maintenance task. Timestamps are ISO-8601 UTC strings so the
 * state file stays readable and does not need a custom JSON codec.
 */
case class MaintenanceTask(
    id: String,
    idempotencyKey: Option[String],
    request: MaintenanceRequest,
    status: String,
    createdAt: String,
    startedAt: Option[String] = None,
    finishedAt: Option[String] = None,
    result: Option[MaintenanceResult] = None,
    error: Option[String] = None)
