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

import org.pudding.core.catalog.{CatalogProvider, ResolvedCatalog}
import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection}

/**
 * Iceberg catalog. The catalog is installed as a named Spark catalog, or as the session catalog
 * when its Spark name is spark_catalog.
 */
class IcebergCatalogProvider extends CatalogProvider {

  override def identifier: String = "iceberg"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    val connection = CatalogProviderSupport.connection(
      conf, connections, Set("hive-metastore", "jdbc", "mysql"))
    IcebergCatalogProvider.build(conf.name, CatalogProviderSupport.properties(conf), connection)
  }
}

private[catalog] object IcebergCatalogProvider {

  private val NamedCatalogClass = "org.apache.iceberg.spark.SparkCatalog"

  private val SessionCatalogClass = "org.apache.iceberg.spark.SparkSessionCatalog"

  private val SessionCatalogName = "spark_catalog"

  private val Extension = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  private val Aliases = Map(
    "catalog-backend" -> "type",
    "catalogType" -> "type",
    "jdbc-user" -> "jdbc.user",
    "jdbc-password" -> "jdbc.password",
    "jdbc-driver" -> "jdbc.driver",
    "jdbc-url" -> "jdbc-url")

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
      resolved.`type` match {
        case "hive-metastore" =>
          settings += "type" -> "hive"
          resolved.endpoint.foreach(uri => settings += "uri" -> uri)
          CatalogProviderSupport.put(settings, resolved.properties, Aliases)
        case "jdbc" | "mysql" =>
          settings += "type" -> "jdbc"
          resolved.endpoint.foreach(uri => settings += "uri" -> uri)
          CatalogProviderSupport.credentials(
            settings, resolved, "jdbc.user", "jdbc.password", "jdbc.driver")
          CatalogProviderSupport.put(
            settings,
            resolved.properties,
            Aliases,
            Set("user", "username", "password", "driver", "url", "jdbc-url"))
        case other =>
          throw new PuddingException(
            s"iceberg catalog '$name' does not support connection type: $other")
      }
    }

    CatalogProviderSupport.put(settings, properties, Aliases, Reserved)
    this.normalizeSettings(settings)

    val sessionCatalog = sparkCatalogName == SessionCatalogName
    if (sessionCatalog && !settings.contains("type")) {
      throw new PuddingException(
        s"iceberg catalog '$name' uses spark_catalog and requires property type " +
          "(for example hive or hadoop)")
    }

    val catalogClass = if (sessionCatalog) SessionCatalogClass else NamedCatalogClass
    val sparkConf = mutable.Map[String, String](
      s"spark.sql.catalog.$sparkCatalogName" -> catalogClass,
      "spark.sql.extensions" -> Extension)
    settings.foreach { case (key, value) =>
      sparkConf += s"spark.sql.catalog.$sparkCatalogName.$key" -> value
    }

    ResolvedCatalog(
      name = name,
      `type` = "iceberg",
      sparkCatalogName = sparkCatalogName,
      sessionCatalog = sessionCatalog,
      sparkConf = sparkConf.toMap,
      properties = properties)
  }

  private def normalizeSettings(settings: mutable.Map[String, String]): Unit = {
    if (!settings.contains("io-impl")) {
      settings.get("warehouse").filter(_.startsWith("s3://")).foreach { _ =>
        settings += "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO"
      }
    }
    if (settings.get("type").contains("hive")) {
      settings.get("metastore.uris")
        .orElse(settings.get("hive.metastore.uris"))
        .foreach(uri => settings += "uri" -> uri)
      settings -= "metastore.uris"
      settings -= "hive.metastore.uris"
    }
    if (settings.get("type").contains("jdbc")) {
      settings.get("jdbc-url").foreach(url => settings += "uri" -> url)
      settings -= "jdbc-url"
    }
  }
}
