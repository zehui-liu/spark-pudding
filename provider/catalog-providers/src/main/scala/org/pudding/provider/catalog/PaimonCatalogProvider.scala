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

import java.util.Locale

import scala.collection.mutable

import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.{ConnectionRegistry, ResolvedConnection}
import org.pudding.core.catalog.{CatalogProvider, ResolvedCatalog}

/**
 * Apache Paimon catalog. A named catalog uses SparkCatalog; `sessionCatalog: true` (or
 * `mode: generic`) installs SparkGenericCatalog as the Spark session catalog so Paimon and
 * non-Paimon tables can coexist.
 */
class PaimonCatalogProvider extends CatalogProvider {

  override def identifier: String = "paimon"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    val connection = CatalogProviderSupport.connection(
      conf, connections, Set("hive-metastore", "jdbc", "mysql"))
    PaimonCatalogProvider.build(conf.name, CatalogProviderSupport.properties(conf), connection)
  }
}

private[catalog] object PaimonCatalogProvider {

  private val NamedCatalogClass = "org.apache.paimon.spark.SparkCatalog"

  private val GenericCatalogClass = "org.apache.paimon.spark.SparkGenericCatalog"

  private val Extension = "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"

  private val SessionCatalogName = "spark_catalog"

  private val Reserved = Set(
    "catalogName",
    "sparkCatalogName",
    "sessionCatalog",
    "mode")

  def build(
      name: String,
      properties: Map[String, String],
      connection: Option[ResolvedConnection]): ResolvedCatalog = {

    val generic = this.resolveGeneric(properties)
    val sparkCatalogName = if (generic) {
      SessionCatalogName
    } else {
      properties.get("sparkCatalogName")
        .orElse(properties.get("catalogName"))
        .map(_.trim)
        .filter(_.nonEmpty)
        .getOrElse(name)
    }

    val options = mutable.LinkedHashMap[String, String]()
    connection.foreach { resolved =>
      resolved.`type` match {
        case "hive-metastore" =>
          options += "metastore" -> "hive"
          resolved.endpoint.foreach(uri => options += "uri" -> uri)
          this.applyOptions(options, resolved.properties)
        case "jdbc" | "mysql" =>
          options += "metastore" -> "jdbc"
          resolved.endpoint.foreach(uri => options += "uri" -> uri)
          resolved.auth.get("user").orElse(resolved.auth.get("username"))
            .foreach(user => options += "jdbc.user" -> user)
          resolved.auth.get("password").foreach(password => options += "jdbc.password" -> password)
          resolved.auth.get("token").foreach(token => options += "token" -> token)
          this.applyOptions(options, resolved.properties)
        case other =>
          throw new PuddingException(
            s"paimon catalog '$name' does not support connection type: $other")
      }
    }
    this.applyOptions(options, properties)

    val warehouse = options.get("warehouse").map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"paimon catalog '$name' requires property warehouse")
    }
    options += "warehouse" -> warehouse

    val catalogClass = if (generic) GenericCatalogClass else NamedCatalogClass
    val sparkConf = mutable.Map[String, String](
      s"spark.sql.catalog.$sparkCatalogName" -> catalogClass,
      "spark.sql.extensions" -> Extension)
    options.foreach { case (key, value) =>
      sparkConf += s"spark.sql.catalog.$sparkCatalogName.$key" -> value
    }

    ResolvedCatalog(
      name = name,
      `type` = "paimon",
      sparkCatalogName = sparkCatalogName,
      sessionCatalog = generic,
      sparkConf = sparkConf.toMap,
      properties = properties)
  }

  private def applyOptions(
      options: mutable.Map[String, String],
      properties: Map[String, String]): Unit = {
    properties.foreach { case (key, value) =>
      if (!Reserved.contains(key)) {
        options += key -> value
      }
    }
  }

  private def resolveGeneric(properties: Map[String, String]): Boolean = {
    val sessionCatalog = properties.get("sessionCatalog")
      .exists(value => value.trim.equalsIgnoreCase("true"))
    val mode = properties.get("mode").map(_.trim.toLowerCase(Locale.ROOT))
    mode.foreach {
      case "named" | "catalog" =>
        if (sessionCatalog) {
          throw new PuddingException(
            "paimon catalog can not use sessionCatalog=true with mode=named")
        }
      case "generic" | "spark_catalog" | "sparkcatalog" =>
      case other =>
        throw new PuddingException(
          s"paimon catalog mode must be named or generic, but got: $other")
    }
    sessionCatalog || mode.exists(value =>
      value == "generic" || value == "spark_catalog" || value == "sparkcatalog")
  }
}
