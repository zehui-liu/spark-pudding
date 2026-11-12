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

package org.pudding.connector.jdbc

import java.math.BigDecimal
import java.sql.{Connection, Types}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.Locale

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

import org.pudding.core.PuddingException
import org.pudding.core.definition.{IncrementalRead, IncrementalRequest, IncrementalSemantics}

/**
 * Cursor-based incremental reads through Spark's JDBC data source.
 *
 * A source selects rows with `cursorField > start` and `cursorField <= end`, then advances the
 * cursor to `end`. The cursor is stored as a string in the incremental state backend; the JDBC
 * column type is inspected on every run so numeric, date/time, timestamp and string cursors can
 * be quoted correctly.
 *
 * `upsert` semantics return the latest complete row per key inside the range. Deletes are not
 * observable in a table snapshot; use a CDC source when deletes must be propagated.
 */
object JdbcIncremental {

  val CursorFieldKey = "cursorField"

  private val UrlKey = "url"

  private val UserKey = "user"

  private val PasswordKey = "password"

  private val TableKey = "dbtable"

  private val Alias = "pudding_incremental"

  def read(
      sparkSession: SparkSession,
      options: Map[String, String],
      cfg: Map[String, Any],
      request: IncrementalRequest): IncrementalRead = {
    if (request.semantics == IncrementalSemantics.Cdc) {
      throw new PuddingException(
        "jdbc-source incremental semantics=cdc is not supported; table snapshots do not " +
          "contain deletes. Use a CDC source or append/upsert semantics")
    }
    val cursorField = this.requiredString(cfg, CursorFieldKey)
    val table = JdbcTables.required(options, TableKey)
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")

    JdbcTables.loadDriver(options)
    val (cursor, quotedTable, maxCursor) =
      JdbcTables.withConnection(url, user, password) { connection =>
        val inspected = this.inspectCursor(connection, table, cursorField)
        (inspected, JdbcTables.quoteTable(connection, table),
          this.maxCursor(connection, table, inspected))
      }

    if (request.semantics == IncrementalSemantics.Upsert && request.keyFields.isEmpty) {
      throw new PuddingException("jdbc-source incremental upsert requires keyFields")
    }

    val start = request.start.orElse {
      request.initial.toLowerCase(Locale.ROOT) match {
        case "latest" =>
          maxCursor.orElse {
            throw new PuddingException(
              s"jdbc-source can not resolve initial=latest because $CursorFieldKey has no " +
                "values in the table; insert data first or set incremental.start")
          }
        case "full" => None
        case "none" =>
          throw new PuddingException(
            "jdbc-source incremental read requires incremental.start or initial=latest/full " +
              "for the first run")
        case other =>
          throw new PuddingException(
            s"unsupported incremental.initial for jdbc-source: $other")
      }
    }
    val end = request.end.orElse(maxCursor).getOrElse {
      throw new PuddingException(
        s"jdbc-source can not determine the end cursor because $CursorFieldKey has no values " +
          "and incremental.end is not configured")
    }
    start.foreach { from =>
      if (from.trim.isEmpty) {
        throw new PuddingException(
          s"jdbc-source incremental.start can not be empty for $CursorFieldKey")
      }
      if (this.compare(from, end, cursor) > 0) {
        throw new PuddingException(
          s"jdbc-source incremental.start is after incremental.end for $CursorFieldKey; " +
            "reset the state or correct the configured range")
      }
    }

    val predicate = start match {
      case Some(from) =>
        s"${cursor.quoted} > ${this.literal(from, cursor)} AND " +
          s"${cursor.quoted} <= ${this.literal(end, cursor)}"
      case None =>
        s"${cursor.quoted} <= ${this.literal(end, cursor)}"
    }
    val query = s"(SELECT * FROM $quotedTable WHERE $predicate) $Alias"
    val raw = sparkSession.read
      .format("jdbc")
      .options(options + (TableKey -> query))
      .load()
    val dataFrame = request.semantics match {
      case IncrementalSemantics.Upsert =>
        this.latestByKey(raw, request.keyFields, cursor.name)
      case IncrementalSemantics.Append =>
        raw
      case IncrementalSemantics.Cdc =>
        throw new PuddingException("jdbc-source incremental cdc is not supported")
    }
    IncrementalRead(
      dataFrame = dataFrame,
      semantics = request.semantics,
      keyFields = request.keyFields,
      opColumn = request.opColumn,
      fromCursor = start,
      toCursor = end,
      cursorType = cursor.typeName,
      orderFields =
        if (request.semantics == IncrementalSemantics.Upsert) Seq(cursor.name) else Seq.empty)
  }

  /**
   * Keep the latest row per key, but retain the cursor column. The cursor is declared in
   * `IncrementalRead.orderFields`; BuildJob validates that every declared ordering column is
   * present in the returned frame.
   */
  private def latestByKey(
      dataFrame: DataFrame,
      keyFields: Seq[String],
      cursorField: String): DataFrame = {
    val rankColumn = this.uniqueColumnName(dataFrame, "__pudding_jdbc_rank")
    val window = Window
      .partitionBy(keyFields.map(col): _*)
      .orderBy(col(cursorField).desc)
    // Spark JDBC reads without partitionColumn produce one partition, and a single
    // partition satisfies any window distribution, so a large incremental upsert would
    // otherwise dedup and write in one task. Repartition by the business keys first.
    val shufflePartitions = dataFrame.sparkSession.sessionState.conf.numShufflePartitions
    val prepared =
      if (shufflePartitions > 1) {
        dataFrame.repartition(shufflePartitions, keyFields.map(col): _*)
      } else {
        dataFrame
      }
    prepared
      .withColumn(rankColumn, row_number().over(window))
      .filter(col(rankColumn) === 1)
      .drop(rankColumn)
  }

  private def uniqueColumnName(dataFrame: DataFrame, base: String): String = {
    val columns = dataFrame.columns.toSet
    var name = base
    var suffix = 0
    while (columns.contains(name)) {
      suffix += 1
      name = s"$base$suffix"
    }
    name
  }

  private final case class CursorColumn(
      name: String,
      quoted: String,
      sqlType: Int,
      typeName: String)

  private def inspectCursor(
      connection: Connection,
      table: String,
      cursorField: String): CursorColumn = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(
        s"SELECT * FROM ${JdbcTables.quoteTable(connection, table)} WHERE 1=0")
      try {
        val metadata = result.getMetaData
        val index = (1 to metadata.getColumnCount).find { value =>
          metadata.getColumnName(value).equalsIgnoreCase(cursorField)
        }.getOrElse {
          throw new PuddingException(
            s"$CursorFieldKey is not a column in the JDBC source table: $cursorField")
        }
        val name = metadata.getColumnName(index)
        CursorColumn(
          name = name,
          quoted = JdbcTables.quoteIdentifier(connection, name),
          sqlType = metadata.getColumnType(index),
          typeName = metadata.getColumnTypeName(index))
      } finally {
        result.close()
      }
    } catch {
      case error: PuddingException => throw error
      case error: java.sql.SQLException =>
        throw new PuddingException(
          s"failed to inspect jdbc-source $CursorFieldKey column '$cursorField': " +
            error.getClass.getSimpleName,
          error)
    } finally {
      statement.close()
    }
  }

  private def maxCursor(
      connection: Connection,
      table: String,
      cursor: CursorColumn): Option[String] = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(
        s"SELECT MAX(${cursor.quoted}) FROM ${JdbcTables.quoteTable(connection, table)}")
      try {
        if (!result.next()) {
          None
        } else {
          Option(result.getString(1)).map(_.trim).filter(_.nonEmpty)
        }
      } finally {
        result.close()
      }
    } catch {
      case error: java.sql.SQLException =>
        throw new PuddingException(
          s"failed to read the maximum jdbc-source cursor column '${cursor.name}': " +
            error.getClass.getSimpleName,
          error)
    } finally {
      statement.close()
    }
  }

  private def compare(left: String, right: String, cursor: CursorColumn): Int = {
    cursor.sqlType match {
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT |
          Types.DECIMAL | Types.NUMERIC | Types.REAL | Types.FLOAT | Types.DOUBLE =>
        try new BigDecimal(left).compareTo(new BigDecimal(right))
        catch {
          case _: NumberFormatException =>
            throw new PuddingException(
              s"jdbc-source cursor value for $CursorFieldKey is not numeric")
        }
      case Types.DATE =>
        try LocalDate.parse(left).compareTo(LocalDate.parse(right))
        catch {
          case _: RuntimeException => left.compareTo(right)
        }
      case Types.TIME | Types.TIME_WITH_TIMEZONE =>
        try LocalTime.parse(left).compareTo(LocalTime.parse(right))
        catch {
          case _: RuntimeException => left.compareTo(right)
        }
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        try {
          LocalDateTime.parse(left.replace(' ', 'T'))
            .compareTo(LocalDateTime.parse(right.replace(' ', 'T')))
        } catch {
          case _: RuntimeException => left.compareTo(right)
        }
      case _ => left.compareTo(right)
    }
  }

  private def literal(
      value: String,
      cursor: CursorColumn): String = {
    cursor.sqlType match {
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT |
          Types.DECIMAL | Types.NUMERIC | Types.REAL | Types.FLOAT | Types.DOUBLE =>
        try new BigDecimal(value).toPlainString
        catch {
          case _: NumberFormatException =>
            throw new PuddingException(
              s"jdbc-source cursor value for $CursorFieldKey is not numeric")
        }
      case Types.BOOLEAN | Types.BIT =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" | "1" => "TRUE"
          case "false" | "0" => "FALSE"
          case _ =>
            throw new PuddingException(
              s"jdbc-source cursor value for $CursorFieldKey is not boolean")
        }
      case _ =>
        "'" + value.replace("'", "''") + "'"
    }
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) =>
        throw new PuddingException(s"$key can not be empty")
      case Some(_) =>
        throw new PuddingException(s"$key must be a string")
      case None =>
        throw new PuddingException(
          s"$key is required for jdbc-source incremental reads")
    }
  }
}
