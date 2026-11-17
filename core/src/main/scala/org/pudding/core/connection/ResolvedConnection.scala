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

package org.pudding.core.connection

/**
 * A connection with secret references resolved. `options` contains the native Spark options for
 * this connection type and may hold credentials; it must never be logged.
 *
 * @param name       connection name
 * @param `type`     connection provider identifier
 * @param endpoint   resolved endpoint
 * @param auth       resolved credential fields
 * @param properties resolved provider properties
 * @param options    Spark options produced by the connection provider
 */
case class ResolvedConnection(
  name: String,
  `type`: String,
  endpoint: Option[String],
  auth: Map[String, String],
  properties: Map[String, String],
  options: Map[String, String])
