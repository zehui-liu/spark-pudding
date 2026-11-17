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

package org.pudding.maintenance.spark

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import org.pudding.maintenance.api.{MaintenanceActions, MaintenanceUsageException, MaintenanceValues}

/**
 * Builds the one-shot SparkSession used by the maintenance CLI.
 *
 * Catalog settings, endpoints and credentials come from the properties file (only `spark.*`
 * keys are accepted). The format extension is appended automatically, and the maintenance
 * session uses UTC so `--older-than` timestamps are deterministic.
 */
object SparkMaintenanceSession {

  def create(
      format: String,
      appName: String,
      confFile: Option[String],
      master: Option[String],
      hadoopConf: Option[String]): SparkSession = {
    val properties = this.load(confFile)
    val builder = SparkSession.builder().appName(appName)
    properties.foreach { case (key, value) => builder.config(key, value) }
    master.foreach(value => builder.master(value))
    if (!properties.contains("spark.sql.session.timeZone")) {
      builder.config("spark.sql.session.timeZone", "UTC")
    }
    val extension = this.extension(format)
    val configured = properties.get("spark.sql.extensions")
      .toSeq.flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
    if (!configured.contains(extension)) {
      builder.config("spark.sql.extensions", (configured :+ extension).mkString(","))
    }
    val session = builder.getOrCreate()
    try {
      hadoopConf.foreach(directory => this.addHadoopResources(session, directory))
      session
    } catch {
      case NonFatal(error) =>
        // A session returned by getOrCreate is otherwise reused by the next task with the
        // wrong format extension; stop it before reporting the configuration failure.
        try session.stop() catch { case NonFatal(_) => }
        throw error
    }
  }

  def extension(format: String): String = MaintenanceValues.normalize(format) match {
    case MaintenanceActions.Iceberg =>
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    case MaintenanceActions.Paimon =>
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
    case MaintenanceActions.Hudi =>
      "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    case other =>
      throw new MaintenanceUsageException(s"unsupported format: $other")
  }

  private def load(confFile: Option[String]): Map[String, String] = {
    confFile match {
      case None => Map.empty
      case Some(value) =>
        val file = new File(value)
        if (!file.isFile) {
          throw new MaintenanceUsageException(s"--conf-file is not a file: $value")
        }
        val properties = new Properties()
        val input = new FileInputStream(file)
        try properties.load(input)
        finally input.close()
        properties.stringPropertyNames().asScala.toSeq.sorted.map { key =>
          if (!key.startsWith("spark.")) {
            throw new MaintenanceUsageException(
              s"--conf-file only accepts spark.* keys, found: $key")
          }
          key -> properties.getProperty(key)
        }.toMap
    }
  }

  private def addHadoopResources(session: SparkSession, directory: String): Unit = {
    val dir = new File(directory)
    if (!dir.isDirectory) {
      throw new MaintenanceUsageException("--hadoop-conf is not a directory: " + directory)
    }
    val configuration: Configuration = session.sparkContext.hadoopConfiguration
    Option(dir.listFiles()).getOrElse(Array.empty[File])
      .filter(file => file.isFile && file.getName.endsWith(".xml"))
      .sortBy(_.getName)
      .foreach(file => configuration.addResource(file.toURI.toURL))
  }
}
