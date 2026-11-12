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

package org.pudding.provider.catalog

import scala.collection.mutable

import org.pudding.core.catalog.{CatalogProvider, ResolvedCatalog}
import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection}

/**
 * Spark JDBC catalog. It exposes database tables through Spark SQL; the jdbc source and sink still
 * use the same connection for DataFrame reads and writes.
 */
class JdbcCatalogProvider extends CatalogProvider {

  override def identifier: String = "jdbc"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    val connection = CatalogProviderSupport.connection(
      conf, connections, Set("jdbc", "mysql"))
    JdbcCatalogProvider.build(conf.name, CatalogProviderSupport.properties(conf), connection)
  }
}

private[catalog] object JdbcCatalogProvider {

  private val CatalogClass =
    "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"

  private val Aliases = Map(
    "jdbc-url" -> "url",
    "jdbc-user" -> "user",
    "jdbc-password" -> "password",
    "jdbc-driver" -> "driver")

  private val Reserved = Set("catalogName", "sparkCatalogName")

  def build(
      name: String,
      properties: Map[String, String],
      connection: Option[ResolvedConnection]): ResolvedCatalog = {

    val sparkCatalogName = properties.get("catalogName")
      .orElse(properties.get("sparkCatalogName"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(name)
    val settings = mutable.Map[String, String]()

    connection.foreach { resolved =>
      resolved.endpoint.foreach(url => settings += "url" -> url)
      CatalogProviderSupport.credentials(
        settings, resolved, "user", "password", "driver")
      CatalogProviderSupport.put(
        settings,
        resolved.properties,
        Aliases,
        Set("url", "jdbc-url", "user", "username", "jdbc-user", "password", "jdbc-password",
          "driver", "jdbc-driver"))
    }

    CatalogProviderSupport.put(settings, properties, Aliases, Reserved)

    if (!settings.get("url").exists(_.trim.nonEmpty)) {
      throw new PuddingException(s"jdbc catalog '$name' requires url")
    }

    val sparkConf = mutable.Map[String, String](
      s"spark.sql.catalog.$sparkCatalogName" -> CatalogClass)
    settings.foreach { case (key, value) =>
      sparkConf += s"spark.sql.catalog.$sparkCatalogName.$key" -> value
    }

    ResolvedCatalog(
      name = name,
      `type` = "jdbc",
      sparkCatalogName = sparkCatalogName,
      sessionCatalog = false,
      sparkConf = sparkConf.toMap,
      properties = properties)
  }
}
