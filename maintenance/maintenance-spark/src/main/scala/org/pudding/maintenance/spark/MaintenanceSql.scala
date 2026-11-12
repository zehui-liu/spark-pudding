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

package org.pudding.maintenance.spark

import org.pudding.maintenance.api.{MaintenanceRequest, MaintenanceUsageException, MaintenanceValidation, MaintenanceValues}

/**
 * A named maintenance SQL statement.
 */
private[spark] case class NamedSql(name: String, sql: String)

/**
 * SQL builders for native maintenance procedures. They are deliberately independent of a
 * SparkSession so the generated statements can be tested and reviewed without allocating Spark.
 */
private[spark] object MaintenanceSql {

  private val DefaultSmallFileBytes = 128L * 1024L * 1024L

  def smallFileBytes(request: MaintenanceRequest): Long = {
    request.options.get("small-file-bytes")
      .map(MaintenanceValues.size)
      .getOrElse(DefaultSmallFileBytes)
  }

  def quoteIdent(value: String): String = {
    if (value == null || value.trim.isEmpty) {
      throw new MaintenanceUsageException("SQL identifier can not be empty")
    }
    "`" + value.trim.replace("`", "``") + "`"
  }

  def qualified(catalog: String, tableParts: Seq[String]): String = {
    (Seq(catalog) ++ tableParts).map(this.quoteIdent).mkString(".")
  }

  def catalog(request: MaintenanceRequest): String = {
    this.quoteIdent(request.catalog.getOrElse(
      throw new MaintenanceUsageException("maintenance request requires --catalog")))
  }

  def tableBase(request: MaintenanceRequest): String = {
    val catalog = request.catalog.getOrElse(
      throw new MaintenanceUsageException("maintenance request requires --catalog"))
    this.qualified(catalog, MaintenanceValidation.tableParts(request))
  }

  def systemTable(request: MaintenanceRequest, suffix: String): String = {
    val catalog = request.catalog.getOrElse(
      throw new MaintenanceUsageException("maintenance request requires --catalog"))
    val parts = MaintenanceValidation.tableParts(request)
    val last = parts.last + suffix
    this.qualified(catalog, parts.dropRight(1) :+ last)
  }

  def positiveInt(value: String, key: String): Int = {
    val parsed = try value.trim.toInt
    catch {
      case _: NumberFormatException =>
        throw new MaintenanceUsageException(s"$key must be an integer, got: $value")
    }
    if (parsed <= 0) {
      throw new MaintenanceUsageException(s"$key must be greater than zero")
    }
    parsed
  }

  def literal(value: String): String = {
    // Spark's default SQL parser unescapes backslashes in string literals, so escape them before
    // doubling quotes; otherwise a value containing \' can terminate the literal early.
    val text = Option(value).getOrElse("").replace("\\", "\\\\").replace("'", "''")
    "'" + text + "'"
  }

  def whereArg(value: Option[String]): Option[String] = {
    value.map(item => s"where => ${this.literal(item)}")
  }

  def strategyArg(value: Option[String]): Option[String] = {
    value.map(item => s"strategy => ${this.literal(item)}")
  }

  /**
   * Render a Spark `map(...)` argument. Keys and values are string literals; an empty map
   * produces no argument.
   */
  def optionMap(options: Map[String, String]): Option[String] = {
    val entries = options.toSeq.sortBy(_._1).flatMap { case (key, value) =>
      Seq(this.literal(key), this.literal(value))
    }
    if (entries.isEmpty) None
    else Some(s"options => map(${entries.mkString(", ")})")
  }

  /**
   * Render Hudi's comma-separated `k=v` options argument. Hudi splits on comma and equals, so
   * keys and values containing those characters are rejected.
   */
  def hudiOptions(options: Map[String, String]): Option[String] = {
    val invalid = options.filter { case (key, value) =>
      key.contains(",") || key.contains("=") || value.contains(",") || value.contains("=")
    }
    if (invalid.nonEmpty) {
      throw new MaintenanceUsageException(
        "Hudi options can not contain ',' or '=' in keys or values: " +
          invalid.keys.toSeq.sorted.mkString(", "))
    }
    if (options.isEmpty) None
    else Some(this.literal(options.toSeq.sortBy(_._1).map {
      case (key, value) => s"$key=$value"
    }.mkString(",")))
  }

  def timeLiteral(request: MaintenanceRequest): Option[String] = {
    request.olderThan.map(value =>
      s"older_than => TIMESTAMP ${this.literal(MaintenanceValues.olderThanTimestamp(value))}")
  }

  def timeStringArg(request: MaintenanceRequest): Option[String] = {
    request.olderThan.map(value =>
      s"older_than => ${this.literal(MaintenanceValues.olderThanTimestamp(value))}")
  }

  def one(name: String, sql: String): Seq[NamedSql] = Seq(NamedSql(name, sql))
}
