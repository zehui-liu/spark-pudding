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

package org.pudding.maintenance.cli

import scala.collection.mutable

import org.pudding.maintenance.api.MaintenanceUsageException

private[cli] case class CliTarget(
    catalog: Option[String] = None,
    table: Option[String] = None,
    path: Option[String] = None)

private[cli] case class CliRetention(
    olderThan: Option[String] = None,
    retainLast: Option[Int] = None,
    retainMax: Option[Int] = None,
    retainMin: Option[Int] = None)

private[cli] case class CliRewrite(
    targetFileSize: Option[String] = None,
    minInputFiles: Option[Int] = None,
    where: Option[String] = None,
    strategy: Option[String] = None)

private[cli] case class CliFlags(
    dryRun: Boolean = false,
    confirmed: Boolean = false,
    json: Boolean = false,
    help: Boolean = false)

private[cli] case class CliSpark(
    confFile: Option[String] = None,
    hadoopConf: Option[String] = None,
    master: Option[String] = None)

private[cli] case class CliArguments(
    format: String,
    action: String,
    target: CliTarget = CliTarget(),
    retention: CliRetention = CliRetention(),
    rewrite: CliRewrite = CliRewrite(),
    options: Map[String, String] = Map.empty,
    flags: CliFlags = CliFlags(),
    spark: CliSpark = CliSpark())

private[cli] object CliArguments {

  def parse(args: Array[String]): CliArguments = {
    if (args == null || args.isEmpty) {
      throw new MaintenanceUsageException("missing format and action")
    }
    if (args.exists(value => value == "-h" || value == "--help")) {
      return CliArguments("", "", flags = CliFlags(help = true))
    }
    if (args.length < 2) {
      throw new MaintenanceUsageException("expected <format> <action>")
    }
    var result = CliArguments(args(0), args(1))
    var index = 2
    val options = mutable.LinkedHashMap[String, String]()
    while (index < args.length) {
      val raw = args(index)
      val (name, inlineValue) = raw match {
        case value if value.startsWith("--") && value.contains("=") =>
          val split = value.substring(2).split("=", 2)
          "--" + split(0) -> Some(split(1))
        case value => value -> None
      }
      name match {
        case "--dry-run" =>
          this.noValue(name, inlineValue)
          result = result.copy(flags = result.flags.copy(dryRun = true))
          index += 1
        case "--confirm" =>
          this.noValue(name, inlineValue)
          result = result.copy(flags = result.flags.copy(confirmed = true))
          index += 1
        case "--json" =>
          this.noValue(name, inlineValue)
          result = result.copy(flags = result.flags.copy(json = true))
          index += 1
        case "--option" =>
          val value = this.value(args, index, inlineValue, name)
          val split = value.split("=", 2)
          if (split.length != 2 || split(0).trim.isEmpty) {
            throw new MaintenanceUsageException("--option must be key=value")
          }
          val key = split(0).trim
          if (options.contains(key)) {
            throw new MaintenanceUsageException(s"duplicate --option key: $key")
          }
          options += key -> split(1)
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--catalog" =>
          result = result.copy(target = result.target.copy(
            catalog = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--table" =>
          result = result.copy(target = result.target.copy(
            table = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--path" =>
          result = result.copy(target = result.target.copy(
            path = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--older-than" =>
          result = result.copy(retention = result.retention.copy(
            olderThan = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--retain-last" =>
          result = result.copy(retention = result.retention.copy(
            retainLast = Some(this.intValue(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--retain-max" =>
          result = result.copy(retention = result.retention.copy(
            retainMax = Some(this.intValue(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--retain-min" =>
          result = result.copy(retention = result.retention.copy(
            retainMin = Some(this.intValue(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--target-file-size" =>
          result = result.copy(rewrite = result.rewrite.copy(
            targetFileSize = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--min-input-files" =>
          result = result.copy(rewrite = result.rewrite.copy(
            minInputFiles = Some(this.intValue(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--where" =>
          result = result.copy(rewrite = result.rewrite.copy(
            where = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--strategy" =>
          result = result.copy(rewrite = result.rewrite.copy(
            strategy = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--conf-file" =>
          result = result.copy(spark = result.spark.copy(
            confFile = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--hadoop-conf" =>
          result = result.copy(spark = result.spark.copy(
            hadoopConf = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case "--master" =>
          result = result.copy(spark = result.spark.copy(
            master = Some(this.value(args, index, inlineValue, name))))
          index += (if (inlineValue.isDefined) 1 else 2)
        case other =>
          throw new MaintenanceUsageException(s"unknown option: $other")
      }
    }
    result.copy(options = options.toMap)
  }

  private def noValue(name: String, inlineValue: Option[String]): Unit = {
    if (inlineValue.isDefined) {
      throw new MaintenanceUsageException(s"$name does not accept a value")
    }
  }

  private def value(
      args: Array[String],
      index: Int,
      inlineValue: Option[String],
      name: String): String = {
    val value = inlineValue.getOrElse {
      if (index + 1 >= args.length || args(index + 1).startsWith("--")) {
        throw new MaintenanceUsageException(s"$name requires a value")
      }
      args(index + 1)
    }
    if (value.trim.isEmpty) {
      throw new MaintenanceUsageException(s"$name can not be empty")
    }
    value
  }

  private def intValue(
      args: Array[String],
      index: Int,
      inlineValue: Option[String],
      name: String): Int = {
    val value = this.value(args, index, inlineValue, name)
    try value.trim.toInt
    catch {
      case _: NumberFormatException =>
        throw new MaintenanceUsageException(s"$name must be an integer")
    }
  }
}
