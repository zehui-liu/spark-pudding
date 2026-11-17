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

package org.pudding.provider.catalog

import scala.collection.mutable

import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection, SecretResolver}

/**
 * Shared helpers for built-in catalog providers.
 */
private[catalog] object CatalogProviderSupport {

  def properties(conf: CatalogConf): Map[String, String] = {
    SecretResolver.resolveMap(conf.properties)
  }

  def connection(
      conf: CatalogConf,
      connections: ConnectionRegistry,
      expectedTypes: Set[String]): Option[ResolvedConnection] = {
    conf.connection.map { name =>
      val resolved = connections.get(name).getOrElse {
        throw new PuddingException(
          s"catalog '${conf.name}' references unknown connection '$name'")
      }
      if (!expectedTypes.contains(resolved.`type`)) {
        throw new PuddingException(
          s"catalog '${conf.name}' requires connection types " +
            s"${expectedTypes.toSeq.sorted.mkString(", ")}, but '$name' is '${resolved.`type`}'")
      }
      resolved
    }
  }

  def put(
      target: mutable.Map[String, String],
      properties: Map[String, String],
      aliases: Map[String, String] = Map.empty,
      skip: Set[String] = Set.empty): Unit = {
    properties.foreach { case (key, value) =>
      if (!skip.contains(key)) {
        target += aliases.getOrElse(key, key) -> value
      }
    }
  }

  def putMetastoreUri(target: mutable.Map[String, String], uri: String): Unit = {
    target += "hive.metastore.uris" -> uri
    target += "spark.hadoop.hive.metastore.uris" -> uri
  }

  def credentials(
      target: mutable.Map[String, String],
      connection: ResolvedConnection,
      userKey: String,
      passwordKey: String,
      driverKey: String): Unit = {
    connection.auth.get("user")
      .orElse(connection.auth.get("username"))
      .orElse(connection.properties.get("user"))
      .orElse(connection.properties.get("jdbc-user"))
      .orElse(connection.options.get("user"))
      .foreach(value => target += userKey -> value)
    connection.auth.get("password")
      .orElse(connection.properties.get("password"))
      .orElse(connection.properties.get("jdbc-password"))
      .orElse(connection.options.get("password"))
      .foreach(value => target += passwordKey -> value)
    connection.properties.get("driver")
      .orElse(connection.properties.get("jdbc-driver"))
      .orElse(connection.options.get("driver"))
      .foreach(value => target += driverKey -> value)
  }
}
