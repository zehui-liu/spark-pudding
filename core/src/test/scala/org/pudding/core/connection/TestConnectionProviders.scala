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

import org.pudding.core.configure.ConnectionConf

/**
 * Test-only providers keep core tests independent from the provider modules. The real built-in
 * providers live in `connection-providers`.
 */
class TestJdbcConnectionProvider extends ConnectionProvider {

  override def identifier: String = "jdbc"

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    var options = Map[String, String]()
    conf.endpoint.foreach(endpoint => options += "url" -> endpoint)
    conf.auth.get("username").foreach(user => options += "user" -> user)
    conf.auth.get("password").foreach(password => options += "password" -> password)
    ResolvedConnection(
      conf.name,
      identifier,
      conf.endpoint,
      conf.auth,
      conf.properties,
      options)
  }
}

class TestKafkaConnectionProvider extends ConnectionProvider {

  override def identifier: String = "kafka"

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    ResolvedConnection(conf.name, identifier, conf.endpoint, conf.auth, conf.properties, Map.empty)
  }
}
