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

import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection}
import org.pudding.core.catalog.{CatalogProvider, ResolvedCatalog}

/**
 * Hudi catalog backed by Hudi's HoodieCatalog and Spark session catalog. Hudi SQL support also
 * requires HoodieSparkSessionExtension, so both settings are installed together.
 */
class HudiCatalogProvider extends CatalogProvider {

  override def identifier: String = "hudi"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    val connection = CatalogProviderSupport.connection(
      conf, connections, Set("hive-metastore"))
    HudiCatalogProvider.build(conf.name, CatalogProviderSupport.properties(conf), connection)
  }
}

private[catalog] object HudiCatalogProvider {

  private val CatalogClass = "org.apache.spark.sql.hudi.catalog.HoodieCatalog"

  private val Extension = "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"

  private val KryoSerializer = "org.apache.spark.serializer.KryoSerializer"

  private val KryoRegistrar = "org.apache.spark.HoodieSparkKryoRegistrar"

  private val SessionCatalogName = "spark_catalog"

  def build(
      name: String,
      properties: Map[String, String],
      connection: Option[ResolvedConnection]): ResolvedCatalog = {

    val settings = mutable.Map[String, String](
      "spark.sql.catalog.spark_catalog" -> CatalogClass,
      "spark.sql.extensions" -> Extension)

    val hiveEnabled = !properties.get("enableHiveSupport")
      .exists(value => value.trim.equalsIgnoreCase("false"))
    if (hiveEnabled) {
      settings += "spark.sql.catalogImplementation" -> "hive"
    }

    val serializerEnabled = !properties.get("setSerializer")
      .exists(value => value.trim.equalsIgnoreCase("false"))
    if (serializerEnabled) {
      settings += "spark.serializer" ->
        properties.getOrElse("serializer", KryoSerializer)
    }

    val registrarEnabled = !properties.get("setKryoRegistrator")
      .exists(value => value.trim.equalsIgnoreCase("false"))
    if (registrarEnabled) {
      settings += "spark.kryo.registrator" ->
        properties.getOrElse("kryoRegistrator", KryoRegistrar)
    }

    properties.get("warehouse").foreach { warehouse =>
      settings += "spark.sql.warehouse.dir" -> warehouse
    }
    // Gravitino Hudi catalogs expose the metastore URI as `uri`.
    properties.get("uri")
      .filter(_ => !properties.contains("metastore.uris") &&
        !properties.contains("hive.metastore.uris"))
      .foreach(uri => CatalogProviderSupport.putMetastoreUri(settings, uri))

    connection.foreach { resolved =>
      resolved.endpoint.foreach(uri => CatalogProviderSupport.putMetastoreUri(settings, uri))
      this.applySessionProperties(settings, resolved.properties)
    }
    this.applySessionProperties(settings, properties)

    ResolvedCatalog(
      name = name,
      `type` = "hudi",
      sparkCatalogName = SessionCatalogName,
      sessionCatalog = true,
      sparkConf = settings.toMap,
      properties = properties)
  }

  /**
   * Hudi table properties are not Spark session settings. Only settings that can be applied to
   * the session are forwarded; table-level options belong to the source or sink.
   */
  private def applySessionProperties(
      settings: mutable.Map[String, String],
      properties: Map[String, String]): Unit = {
    properties.foreach {
      case ("metastore.uris" | "hive.metastore.uris", value) =>
        CatalogProviderSupport.putMetastoreUri(settings, value)
      case (key, value) if key.startsWith("spark.") || key.startsWith("hive.") =>
        settings += key -> value
      case _ =>
    }
  }
}
