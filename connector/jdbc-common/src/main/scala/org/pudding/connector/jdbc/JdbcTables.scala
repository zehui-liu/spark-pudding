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

package org.pudding.connector.jdbc

import java.sql.{Connection, DriverManager, SQLException}
import java.util.{Locale, UUID}

import org.pudding.core.PuddingException

/**
 * Small JDBC utilities shared by the batch replacement and streaming upsert paths.
 *
 * Table names are configuration, not credentials, so they may appear in SQL. Connection URLs,
 * users and passwords never appear in errors raised here.
 */
private[jdbc] object JdbcTables {

  val StagingPrefix = "_pudding_stage_"

  val StreamingPrefix = "_pudding_stream_"

  def loadDriver(options: Map[String, String]): Unit = {
    val driver = this.required(options, "driver")
    try {
      // scalastyle:off classforname
      Class.forName(driver)
      // scalastyle:on classforname
    } catch {
      case error: ClassNotFoundException =>
        throw new PuddingException(s"JDBC driver not found: $driver", error)
    }
  }

  def withConnection[T](
      url: String,
      user: String,
      password: String)(
      body: Connection => T): T = {
    val connection = DriverManager.getConnection(url, user, password)
    try {
      body(connection)
    } finally {
      connection.close()
    }
  }

  /**
   * Quote each dot-separated identifier with the database quote string. Values containing the
   * quote character are escaped so a configured table can not break out of its identifier.
   */
  def quoteTable(connection: Connection, table: String): String = {
    val quote = Option(connection.getMetaData.getIdentifierQuoteString)
      .map(_.trim).filter(_.nonEmpty).getOrElse("")
    table.split("\\.", -1)
      .map(part => this.quoteIdentifier(part, quote))
      .mkString(".")
  }

  def quoteIdentifier(connection: Connection, name: String): String = {
    val quote = Option(connection.getMetaData.getIdentifierQuoteString)
      .map(_.trim).filter(_.nonEmpty).getOrElse("")
    this.quoteIdentifier(name, quote)
  }

  def stagingTable(target: String, prefix: String = StagingPrefix): String = {
    val parts = target.split("\\.", -1).map(_.trim)
    if (parts.exists(_.isEmpty)) {
      throw new PuddingException("table must not contain an empty identifier")
    }
    val name = prefix + UUID.randomUUID().toString.replace("-", "").substring(0, 12)
    (parts.dropRight(1) :+ name).mkString(".")
  }

  def execute(connection: Connection, sql: String): Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute(sql)
    } finally {
      statement.close()
    }
  }

  def required(options: Map[String, String], key: String): String = {
    options.get(key).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"JDBC options are missing $key")
    }
  }

  def tableExists(
      url: String,
      user: String,
      password: String,
      table: String): Boolean = {
    this.withConnection(url, user, password) { connection =>
      val statement = connection.createStatement()
      try {
        statement.executeQuery(
          s"SELECT 1 FROM ${this.quoteTable(connection, table)} WHERE 1=0").close()
        true
      } catch {
        case error: SQLException if this.isMissingTable(error) =>
          // A quoted probe is case-sensitive on databases that fold unquoted identifiers.
          // Confirm through metadata with a case-insensitive comparison before deciding that
          // the target is absent and allowing a destructive overwrite.
          this.tableExistsInMetadata(connection, table)
        case error: SQLException =>
          throw new PuddingException(
            s"failed to inspect JDBC table '$table': ${error.getClass.getSimpleName}",
            error)
      } finally {
        statement.close()
      }
    }
  }

  private def tableExistsInMetadata(connection: Connection, table: String): Boolean = {
    val parts = table.split("\\.", -1)
      .map(_.trim)
      .map(part => this.stripIdentifierQuotes(part))
      .filter(_.nonEmpty)
    if (parts.isEmpty) {
      return false
    }
    val name = parts.last
    val qualifiers = parts.dropRight(1)
    val tables = connection.getMetaData.getTables(null, null, null, Array("TABLE"))
    try {
      while (tables.next()) {
        val tableName = Option(tables.getString("TABLE_NAME"))
        val tableSchema = Option(tables.getString("TABLE_SCHEM"))
        val tableCatalog = Option(tables.getString("TABLE_CAT"))
        if (tableName.exists(_.equalsIgnoreCase(name)) &&
          this.qualifiersMatch(qualifiers, tableCatalog, tableSchema)) {
          return true
        }
      }
      false
    } finally {
      tables.close()
    }
  }

  /**
   * Match the configured qualifiers against the catalog/schema returned by the driver. A null
   * metadata component is treated as unknown and accepted: a false "table absent" result would
   * allow a destructive overwrite, while a false "table present" result only takes the safer
   * staging path and fails if the name can not be addressed.
   */
  private def qualifiersMatch(
      qualifiers: Array[String],
      catalog: Option[String],
      schema: Option[String]): Boolean = {
    if (qualifiers.isEmpty) {
      true
    } else if (qualifiers.length == 1) {
      // A two-part name is either schema.table or database.table. Drivers expose the
      // namespace in TABLE_SCHEM or TABLE_CAT.
      this.matchesOptional(catalog, qualifiers(0)) ||
        this.matchesOptional(schema, qualifiers(0))
    } else {
      this.matchesOptional(catalog, qualifiers(qualifiers.length - 2)) &&
        this.matchesOptional(schema, qualifiers.last)
    }
  }

  private def matchesOptional(value: Option[String], expected: String): Boolean = {
    value.forall(_.equalsIgnoreCase(expected))
  }

  private def stripIdentifierQuotes(value: String): String = {
    if (value.length >= 2 &&
      ((value.startsWith("\"") && value.endsWith("\"")) ||
        (value.startsWith("`") && value.endsWith("`")))) {
      value.substring(1, value.length - 1)
    } else {
      value
    }
  }

  /**
   * A missing table is a normal probe result. Any other SQL failure must propagate: treating a
   * permission, lock or connection error as "missing" would send a full refresh down the
   * destructive create-or-overwrite path while the target still exists.
   */
  private[jdbc] def isMissingTable(error: SQLException): Boolean = {
    val state = Option(error.getSQLState).map(_.toUpperCase(Locale.ROOT))
    state.exists(MissingTableSqlStates.contains) ||
      MissingTableErrorCodes.contains(error.getErrorCode)
  }

  private val MissingTableSqlStates =
    Set("42S02", "42S03", "42S04", "42P01", "S0002", "90079")

  private val MissingTableErrorCodes = Set(942, 1146, 42102, 42103, 42104, 90079)

  private def quoteIdentifier(name: String, quote: String): String = {
    if (quote.isEmpty) {
      name
    } else {
      quote + name.replace(quote, quote + quote) + quote
    }
  }
}
