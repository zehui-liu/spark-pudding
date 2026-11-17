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

package org.pudding.core.spark

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import org.pudding.core.PuddingException
import org.pudding.core.catalog.{CatalogManager, ResolvedCatalog}
import org.pudding.core.configure.JobConf
import org.pudding.core.connection.SecretResolver

/**
 * Builds the driver SparkConf from explicit job, catalog and runtime settings.
 *
 * Deployment keys are owned by the execution target and are never taken from `job.config`.
 * Spark extensions are merged because several catalogs may contribute one. Every other key may
 * come from only one value across the three sources; a conflicting value fails before the
 * SparkSession is created instead of being silently overwritten by application order.
 */
object SparkConfigResolver extends Logging {

  val MasterKey = "spark.master"

  val DeployModeKey = "spark.submit.deployMode"

  val AppNameKey = "spark.app.name"

  val ExtensionsKey = "spark.sql.extensions"

  private val ReservedKeys = Set(MasterKey, DeployModeKey, AppNameKey)

  private val MergeKeys = Set(ExtensionsKey)

  private val JobSource = "job.config"

  private val RuntimeSource = "runtime SparkConf"

  def resolve(
      jobConf: JobConf,
      catalogs: Seq[ResolvedCatalog],
      runtime: SparkConf): SparkConf = {
    val jobEntries = jobConf.config.getOrElse(Map.empty)
    val ignored = jobEntries.keySet.intersect(ReservedKeys).toSeq.sorted
    if (ignored.nonEmpty) {
      logWarning(
        "ignoring target-owned Spark keys in job.config: " + ignored.mkString(", ") +
          "; use job.master and job.deployMode for deployment settings")
    }

    val explicit = mutable.Map[String, mutable.LinkedHashMap[String, String]]()
    def add(source: String, entries: Map[String, String]): Unit = {
      entries.foreach { case (key, value) =>
        if (!ReservedKeys.contains(key) && !MergeKeys.contains(key)) {
          explicit.getOrElseUpdate(key, mutable.LinkedHashMap()).update(source, value)
        }
      }
    }

    add(JobSource, jobEntries.filterNot { case (key, _) => ReservedKeys.contains(key) })
    Option(catalogs).getOrElse(Seq.empty).foreach { catalog =>
      add(s"catalog '${catalog.name}'", catalog.sparkConf)
    }
    // The submit client forwards job.config entries (including unresolved secret references) as
    // spark-submit --conf arguments. Resolve them here as well, on the driver, so an identical
    // setting is recognized as the same value instead of a job/runtime conflict.
    val runtimeEntries = Option(runtime)
      .map(_.getAll.toMap.map { case (key, value) =>
        key -> SecretResolver.resolve(value)
      })
      .getOrElse(Map.empty)
    add(RuntimeSource, runtimeEntries)

    val conflicts = explicit.collect {
      case (key, values) if values.values.toSet.size > 1 =>
        key -> values.keys.toSeq
    }.toSeq.sortBy(_._1)
    if (conflicts.nonEmpty) {
      val detail = conflicts.map { case (key, sources) =>
        s"'$key' (${sources.mkString(", ")})"
      }.mkString(", ")
      throw new PuddingException(
        "conflicting Spark configuration; set each key in only one source: " + detail)
    }

    val resolved = new SparkConf()
    runtimeEntries.foreach { case (key, value) => resolved.set(key, value) }
    jobEntries.foreach { case (key, value) =>
      if (!ReservedKeys.contains(key)) {
        resolved.set(key, value)
      }
    }
    val extensions = this.extensions(runtime, jobEntries, catalogs)
    // CatalogManager installs catalog settings; set the fully merged extension list afterwards
    // so a runtime or job extension is not lost when a catalog also registers one.
    CatalogManager.install(Option(catalogs).getOrElse(Seq.empty), resolved)
    if (extensions.nonEmpty) {
      resolved.set(ExtensionsKey, extensions.mkString(","))
    }
    resolved
  }

  private def extensions(
      runtime: SparkConf,
      jobEntries: Map[String, String],
      catalogs: Seq[ResolvedCatalog]): Seq[String] = {
    val values = Option(runtime).toSeq.flatMap(_.get(ExtensionsKey, "").split(",")) ++
      jobEntries.getOrElse(ExtensionsKey, "").split(",") ++
      Option(catalogs).getOrElse(Seq.empty).flatMap(_.sparkConf.get(ExtensionsKey).toSeq)
        .flatMap(_.split(","))
    values.map(_.trim).filter(_.nonEmpty).distinct
  }
}
