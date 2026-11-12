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

package org.pudding.maintenance.cli

import java.io.PrintStream

import scala.util.control.NonFatal

import org.pudding.maintenance.api._
import org.pudding.maintenance.spark.{SparkMaintenanceSession, SparkTableMaintenanceService}

// scalastyle:off println
/**
 * One-shot CLI adapter for the table-maintenance service.
 */
object MaintenanceCli {

  def main(args: Array[String]): Unit = {
    System.exit(this.run(args, System.out, System.err))
  }

  def run(args: Array[String], out: PrintStream, err: PrintStream): Int = {
    var json = args != null && args.contains("--json")
    try {
      val arguments = CliArguments.parse(args)
      json = arguments.flags.json
      if (arguments.flags.help) {
        out.println(this.usage)
        return 0
      }
      val normalized = MaintenanceValidation.validate(this.request(arguments))
      val session = SparkMaintenanceSession.create(
        normalized.format,
        s"pudding-maintenance-${normalized.format}-${normalized.action}",
        arguments.spark.confFile,
        arguments.spark.master,
        arguments.spark.hadoopConf)
      try {
        val result = new SparkTableMaintenanceService(session).execute(normalized)
        if (json) out.println(MaintenanceJson.render(result))
        else this.printHuman(result, out)
        0
      } finally {
        session.stop()
      }
    } catch {
      case error: MaintenanceException =>
        this.printError(error.getMessage, json, out, err)
        error.exitCode
      case NonFatal(error) =>
        val message = Option(error.getMessage).map(_.trim).filter(_.nonEmpty)
          .getOrElse(error.getClass.getSimpleName)
        this.printError(message, json, out, err)
        1
    }
  }

  private def request(arguments: CliArguments): MaintenanceRequest = {
    MaintenanceRequest(
      format = arguments.format,
      action = arguments.action,
      catalog = arguments.target.catalog,
      table = arguments.target.table,
      path = arguments.target.path,
      retention = MaintenanceRetention(
        olderThan = arguments.retention.olderThan,
        retainLast = arguments.retention.retainLast,
        retainMax = arguments.retention.retainMax,
        retainMin = arguments.retention.retainMin),
      rewrite = MaintenanceRewrite(
        targetFileSize = arguments.rewrite.targetFileSize,
        minInputFiles = arguments.rewrite.minInputFiles,
        where = arguments.rewrite.where,
        strategy = arguments.rewrite.strategy),
      options = arguments.options,
      dryRun = arguments.flags.dryRun,
      confirmed = arguments.flags.confirmed)
  }

  private def printHuman(result: MaintenanceResult, out: PrintStream): Unit = {
    out.println(
      s"format=${result.format} action=${result.action} target=${result.target} " +
        s"dryRun=${result.dryRun} executed=${result.executed}")
    if (result.summary.nonEmpty) {
      out.println("summary:")
      result.summary.toSeq.sortBy(_._1).foreach { case (key, value) =>
        out.println(s"  $key=$value")
      }
    }
    if (result.rows.nonEmpty) {
      out.println("rows:")
      result.rows.zipWithIndex.foreach { case (row, index) =>
        out.println(s"  [$index] " + row.toSeq.sortBy(_._1)
          .map { case (key, value) => s"$key=$value" }.mkString(" "))
      }
    }
    if (result.statements.nonEmpty) {
      out.println("statements:")
      result.statements.foreach(statement => out.println(s"  $statement"))
    }
    if (result.message.nonEmpty) {
      out.println(s"message=${result.message}")
    }
  }

  private def printError(
      message: String,
      json: Boolean,
      out: PrintStream,
      err: PrintStream): Unit = {
    val text = Option(message).getOrElse("maintenance failed")
    if (json) {
      out.println(MaintenanceJson.error(text))
    } else {
      err.println(s"pudding maintenance: $text")
    }
  }

  private val usage =
    """Usage:
      |  pudding maintenance <format> <action> [options]
      |
      |Formats and actions:
      |  iceberg  status | rewrite-data-files | rewrite-manifests | expire-snapshots |
      |           remove-orphan-files
      |  paimon   status | compact | compact-manifests | expire-snapshots |
      |           remove-orphan-files
      |  hudi     status | clean | compact | cluster | archive
      |
      |Targets:
      |  --catalog <name>             Spark catalog for iceberg/paimon
      |  --table <db.table>           table identifier (or registered Hudi table)
      |  --path <uri>                 Hudi table path; clean still requires --table
      |
      |Safety:
      |  --dry-run                    preview SQL; native dry-run for orphan cleanup
      |  --confirm                    required for expire/clean/orphan without --dry-run
      |  --json                       JSON output
      |
      |Common options:
      |  --older-than <duration>      e.g. 24h, 7d; minimum 24h for orphan cleanup
      |  --retain-last <n>            Iceberg snapshot retention / Hudi clean commits
      |  --retain-max <n>             Paimon expire_snapshots retain_max
      |  --retain-min <n>             Paimon expire_snapshots retain_min
      |  --target-file-size <size>    Iceberg target-file-size-bytes, e.g. 128MB
      |  --min-input-files <n>        Iceberg rewrite_data_files min-input-files
      |  --where <expr>               Iceberg rewrite filter / Paimon compact where
      |  --strategy <strategy>        Iceberg rewrite strategy
      |  --option key=value           repeatable native option
      |
      |Spark:
      |  --conf-file <file>           properties containing only spark.* keys
      |  --hadoop-conf <dir>          XML files added to the Hadoop configuration
      |  --master <url>               override the Spark master
      |
      |Examples:
      |  pudding maintenance iceberg status --catalog lake --table db.orders --json
      |  pudding maintenance iceberg rewrite-data-files --catalog lake --table db.orders \
      |    --target-file-size 128MB
      |  pudding maintenance paimon compact --catalog paimon --table db.orders \
      |    --option compact-strategy=full
      |  pudding maintenance hudi compact --path hdfs:///warehouse/orders \
      |    --option op=scheduleAndExecute
      |""".stripMargin
}
// scalastyle:on println
