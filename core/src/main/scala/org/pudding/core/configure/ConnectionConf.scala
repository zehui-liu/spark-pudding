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

package org.pudding.core.configure

/**
 * A reusable connection definition. It describes how to reach a system, not what to read or
 * write. Sources and sinks reference a connection by name and keep their resource locator
 * (table, topic, index, collection) in their own config.
 *
 * @param name       unique connection name inside one job
 * @param `type`     connection provider identifier, such as jdbc, kafka or mongodb
 * @param describe   optional description
 * @param endpoint   address of the system; a comma-separated list when the client supports it
 * @param auth       credential fields; values may use `${secret:env/NAME}` references
 * @param properties provider-specific connection properties
 */
case class ConnectionConf(
  name: String,
  `type`: String,
  describe: Option[String] = None,
  endpoint: Option[String] = None,
  auth: Map[String, String] = Map.empty,
  properties: Map[String, String] = Map.empty
)
