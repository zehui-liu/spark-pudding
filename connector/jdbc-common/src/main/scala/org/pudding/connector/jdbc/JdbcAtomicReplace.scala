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

import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException

/**
 * Atomic full-refresh replacement for JDBC sinks.
 *
 * The new snapshot is written to a staging table first. A single transaction then deletes the
 * old target rows and inserts the staged rows. The target schema, indexes and constraints are
 * preserved; readers see the old snapshot until the transaction commits. This is slower than a
 * database-native table swap but is portable across JDBC databases.
 *
 * The target table must already exist. When it does not, the initial creation has no previous
 * snapshot to protect and is written directly.
 */
object JdbcAtomicReplace {

  private val UrlKey = "url"

  private val UserKey = "user"

  private val PasswordKey = "password"

  private val TableKey = "dbtable"

  def write(dataFrame: DataFrame, options: Map[String, String]): Unit = {
    val url = JdbcTables.required(options, UrlKey)
    val user = JdbcTables.required(options, UserKey)
    val password = options.getOrElse(PasswordKey, "")
    val target = JdbcTables.required(options, TableKey)

    JdbcTables.loadDriver(options)

    if (!JdbcTables.tableExists(url, user, password, target)) {
      dataFrame.write
        .format("jdbc")
        .options(options)
        .mode("overwrite")
        .save()
      return
    }

    val staging = JdbcTables.stagingTable(target)
    this.createStaging(url, user, password, target, staging)
    try {
      dataFrame.write
        .format("jdbc")
        .options(options + (TableKey -> staging))
        .mode("append")
        .save()
      this.replace(url, user, password, target, staging)
    } finally {
      this.dropStaging(url, user, password, staging)
    }
  }

  private def createStaging(
      url: String,
      user: String,
      password: String,
      target: String,
      staging: String): Unit = {
    JdbcTables.withConnection(url, user, password) { connection =>
      JdbcTables.execute(
        connection,
        s"CREATE TABLE ${JdbcTables.quoteTable(connection, staging)} AS " +
          s"SELECT * FROM ${JdbcTables.quoteTable(connection, target)} WHERE 1=0")
    }
  }

  private def replace(
      url: String,
      user: String,
      password: String,
      target: String,
      staging: String): Unit = {
    JdbcTables.withConnection(url, user, password) { connection =>
      val targetName = JdbcTables.quoteTable(connection, target)
      val stagingName = JdbcTables.quoteTable(connection, staging)
      connection.setAutoCommit(false)
      try {
        JdbcTables.execute(connection, s"DELETE FROM $targetName")
        JdbcTables.execute(
          connection,
          s"INSERT INTO $targetName SELECT * FROM $stagingName")
        connection.commit()
      } catch {
        case NonFatal(error) =>
          try connection.rollback()
          catch {
            case NonFatal(_) => ()
          }
          throw new PuddingException(
            "atomic JDBC full-refresh replacement failed; the previous target remains " +
              "visible after rollback: " + error.getClass.getSimpleName,
            error)
      } finally {
        try connection.setAutoCommit(true)
        catch {
          case NonFatal(_) => ()
        }
      }
    }
  }

  private def dropStaging(
      url: String,
      user: String,
      password: String,
      staging: String): Unit = {
    try JdbcTables.withConnection(url, user, password) { connection =>
      JdbcTables.execute(
        connection,
        s"DROP TABLE IF EXISTS ${JdbcTables.quoteTable(connection, staging)}")
    } catch {
      case NonFatal(_) => ()
    }
  }
}
