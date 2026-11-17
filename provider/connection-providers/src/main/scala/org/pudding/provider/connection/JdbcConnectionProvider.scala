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

package org.pudding.provider.connection

import org.pudding.core.connection.{ConnectionProvider, ResolvedConnection}
import org.pudding.core.configure.ConnectionConf

/**
 * JDBC connection shared by the jdbc and mysql connectors. The table name is a resource locator
 * and remains in the source or sink config.
 */
class JdbcConnectionProvider extends ConnectionProvider {

  override def identifier: String = "jdbc"

  protected def defaultDriver: Option[String] = None

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    import ConnectionProviderSupport._

    val resolvedProperties = properties(conf)
    val resolvedEndpoint = endpointOrFail(conf, "url", "jdbc-url")
    val resolvedAuth = auth(conf)

    val user = text(resolvedAuth, "user", "username")
      .orElse(property(resolvedProperties, "user", "jdbc-user"))
    val password = resolvedAuth.get("password")
      .orElse(property(resolvedProperties, "password", "jdbc-password"))
    val driver = property(resolvedProperties, "driver", "jdbc-driver").orElse(defaultDriver)

    val reserved = Set(
      "url", "jdbc-url", "user", "username", "jdbc-user", "password", "jdbc-password",
      "driver", "jdbc-driver")
    var options = resolvedProperties.filterNot { case (key, _) => reserved.contains(key) }
    options += "url" -> resolvedEndpoint
    user.foreach(value => options += "user" -> value)
    password.foreach(value => options += "password" -> value)
    driver.foreach(value => options += "driver" -> value)

    ResolvedConnection(
      conf.name, identifier, Some(resolvedEndpoint), resolvedAuth, resolvedProperties, options)
  }
}

/**
 * MySQL connection. It is the same as jdbc but supplies the MySQL driver by default.
 */
class MysqlConnectionProvider extends JdbcConnectionProvider {

  override def identifier: String = "mysql"

  override protected def defaultDriver: Option[String] = Some("com.mysql.cj.jdbc.Driver")
}
