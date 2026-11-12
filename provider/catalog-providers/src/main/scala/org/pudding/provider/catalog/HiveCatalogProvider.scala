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

import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection}
import org.pudding.core.catalog.{CatalogProvider, ResolvedCatalog}

/**
 * Hive catalog backed by Spark's session catalog. Spark supports one Hive session catalog per
 * SparkSession; multiple hive catalog definitions are rejected by CatalogManager.
 */
class HiveCatalogProvider extends CatalogProvider {

  override def identifier: String = "hive"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    val connection = CatalogProviderSupport.connection(
      conf, connections, Set("hive-metastore"))
    HiveCatalogProvider.build(conf.name, CatalogProviderSupport.properties(conf), connection)
  }
}

private[catalog] object HiveCatalogProvider {

  def build(
      name: String,
      properties: Map[String, String],
      connection: Option[ResolvedConnection]): ResolvedCatalog = {

    val settings = mutable.Map[String, String](
      "spark.sql.catalogImplementation" -> "hive")

    connection.foreach { resolved =>
      resolved.endpoint.foreach { uri =>
        CatalogProviderSupport.putMetastoreUri(settings, uri)
      }
      this.applyProperties(settings, resolved.properties)
    }
    this.applyProperties(settings, properties)

    ResolvedCatalog(
      name = name,
      `type` = "hive",
      sparkCatalogName = "spark_catalog",
      sessionCatalog = true,
      sparkConf = settings.toMap,
      properties = properties)
  }

  private def applyProperties(
      settings: mutable.Map[String, String],
      properties: Map[String, String]): Unit = {
    properties.foreach {
      case ("warehouse", value) =>
        settings += "spark.sql.warehouse.dir" -> value
      case ("metastore.uris" | "hive.metastore.uris", value) =>
        CatalogProviderSupport.putMetastoreUri(settings, value)
      case (key, value) if key.startsWith("spark.") || key.startsWith("hive.") =>
        settings += key -> value
      case (key, value) =>
        settings += s"hive.$key" -> value
    }
  }
}
