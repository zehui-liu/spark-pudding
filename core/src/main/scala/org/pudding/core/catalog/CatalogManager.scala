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

package org.pudding.core.catalog

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkConf

import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.ConnectionRegistry
import org.pudding.core.util.ConfigRedactor

/**
 * Resolves all catalogs and installs their settings into the Spark configuration. Installation
 * must happen before SparkSession.builder.getOrCreate() because Spark reads catalog settings only
 * during session initialization.
 */
object CatalogManager {

  private val ExtensionKey = "spark.sql.extensions"

  def resolve(
      confs: Seq[CatalogConf],
      connections: ConnectionRegistry): Seq[ResolvedCatalog] = {
    val nonNull = Option(confs).getOrElse(Seq.empty).filter(_ != null)
    val resolved = nonNull.map { conf =>
      try {
        CatalogProvider.forType(conf.`type`).resolve(conf, connections)
      } catch {
        case NonFatal(error) =>
          val detail = ConfigRedactor.redactLine(
            Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName))
          throw new PuddingException(
            s"failed to resolve catalog '${conf.name}' (type '${conf.`type`}'): $detail",
            error)
      }
    }
    this.validate(resolved)
    resolved
  }

  /**
   * Apply catalog settings, merging the extension list instead of letting a later catalog
   * overwrite an extension registered by the job or by another catalog.
   */
  def install(catalogs: Seq[ResolvedCatalog], sparkConf: SparkConf): Unit = {
    val extensions = mutable.LinkedHashSet[String]()
    this.extensions(sparkConf.get(ExtensionKey, "")).foreach(extensions += _)
    Option(catalogs).getOrElse(Seq.empty).foreach { catalog =>
      catalog.sparkConf.foreach {
        case (ExtensionKey, value) => this.extensions(value).foreach(extensions += _)
        case (key, value) => sparkConf.set(key, value)
      }
    }
    if (extensions.nonEmpty) {
      sparkConf.set(ExtensionKey, extensions.mkString(","))
    }
  }

  private def validate(catalogs: Seq[ResolvedCatalog]): Unit = {
    val duplicatedNames = catalogs.map(_.name).groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }.toSeq.sorted
    if (duplicatedNames.nonEmpty) {
      throw new PuddingException(s"duplicated catalog name: ${duplicatedNames.mkString(", ")}")
    }

    val sessionCatalogs = catalogs.filter(_.sessionCatalog)
    if (sessionCatalogs.size > 1) {
      throw new PuddingException(
        "Spark supports only one session catalog per session, configured: " +
          sessionCatalogs.map(_.name).sorted.mkString(", "))
    }

    // Include session catalogs: a named catalog that also claims spark_catalog would otherwise
    // silently override the Hive/Hudi session catalog.
    val sparkNames = catalogs.map(_.sparkCatalogName)
    val duplicatedSparkNames = sparkNames.groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }.toSeq.sorted
    if (duplicatedSparkNames.nonEmpty) {
      throw new PuddingException(
        s"duplicated Spark catalog name: ${duplicatedSparkNames.mkString(", ")}")
    }
  }

  private def extensions(value: String): Seq[String] = {
    Option(value).getOrElse("").split(",").map(_.trim).filter(_.nonEmpty).toSeq
  }
}
