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

package org.pudding.core.incremental

import java.sql.{Connection, DriverManager, SQLException}
import java.time.Instant

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.pudding.core.PuddingException

/**
 * JDBC state backend for deployments that need a strongly consistent, shared state store.
 *
 * The table is provisioned by the operator. A source row also carries its lock, so acquiring a
 * lock and committing a state transition are single-row updates. State writes are conditioned on
 * the lock token, so a writer whose lease expired and was taken over fails instead of overwriting
 * newer state. The backend opens one short connection per operation; connection pooling belongs to
 * the JDBC driver or a pool configured outside Pudding.
 */
class JdbcStateBackend(options: Map[String, String])
    extends AbstractIncrementalStateBackend(JdbcStateBackend.lockTtlMillis(options)) {

  private val url = this.required("url")

  private val user = this.optional("user")

  private val password = this.optional("password")

  private val driver = this.optional("driver")

  private val table = this.tableName(
    this.optional("table").getOrElse("pudding_incremental_state"))

  // scalastyle:off classforname
  // Loading a JDBC driver by name is intentional; the backend must remain usable by the
  // standalone state tool without a Spark context.
  driver.foreach { value =>
    try {
      Class.forName(value)
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"failed to load incremental state JDBC driver: ${error.getClass.getSimpleName}", error)
    }
  }
  // scalastyle:on classforname

  if (this.booleanOption("migrateSchema", default = false)) {
    this.migrateSchema()
  }

  override def location: String = s"jdbc:$table"

  override protected def readState(sourceId: String): Option[IncrementalState] = {
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"SELECT state_json FROM $table WHERE source_id = ?")
      try {
        statement.setString(1, sourceId)
        val result = statement.executeQuery()
        try {
          if (!result.next()) {
            None
          } else {
            val text = result.getString(1)
            if (text == null || text.trim.isEmpty) None
            else Some(IncrementalStateCodec.decode(sourceId, text))
          }
        } finally {
          result.close()
        }
      } finally {
        statement.close()
      }
    }
  }

  override protected def persistState(state: IncrementalState, token: String): Unit = {
    val encoded = IncrementalStateCodec.encode(state)
    this.withConnection { connection =>
      connection.setAutoCommit(false)
      try {
        val update = connection.prepareStatement(
          s"UPDATE $table SET state_json = ? WHERE source_id = ? AND lock_token = ?")
        val updated = try {
          update.setString(1, encoded)
          update.setString(2, state.sourceId)
          update.setString(3, token)
          update.executeUpdate()
        } finally {
          update.close()
        }
        if (updated != 1) {
          this.rollback(connection)
          throw this.lostLock(state.sourceId)
        }
        connection.commit()
      } catch {
        case error: SQLException =>
          this.rollback(connection)
          throw this.failure("write state", error)
      } finally {
        // The transaction has already committed or failed; a connection lost while restoring
        // auto-commit must not turn a successful commit into a reported failure.
        try connection.setAutoCommit(true) catch {
          case _: SQLException => ()
        }
      }
    }
  }

  override protected def readLock(sourceId: String): Option[IncrementalLockRecord] = {
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"SELECT lock_token, lock_owner, lock_host, lock_pid, lock_acquired_at, " +
          s"lock_fencing_token, lock_expires_at_ms FROM $table WHERE source_id = ?")
      try {
        statement.setString(1, sourceId)
        val result = statement.executeQuery()
        try {
          if (!result.next()) {
            None
          } else {
            val token = result.getString(1)
            if (token == null || token.trim.isEmpty) {
              None
            } else {
              val rawFencingToken = result.getLong(6)
              val fencingToken = if (result.wasNull()) 0L else rawFencingToken
              val expiresAtMillis = result.getLong(7)
              val expiresAt = if (result.wasNull()) {
                None
              } else {
                Some(Instant.ofEpochMilli(expiresAtMillis).toString)
              }
              Some(IncrementalLockRecord(
                token,
                IncrementalLockInfo(
                  owner = Option(result.getString(2)).getOrElse(""),
                  host = Option(result.getString(3)).getOrElse(""),
                  pid = Option(result.getString(4)).getOrElse(""),
                  acquiredAt = Option(result.getString(5)).getOrElse(""),
                  fencingToken = fencingToken,
                  expiresAt = expiresAt)))
            }
          }
        } finally {
          result.close()
        }
      } finally {
        statement.close()
      }
    }
  }

  override protected def createLock(
      sourceId: String,
      record: IncrementalLockRecord): Boolean = {
    this.ensureRow(sourceId)
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"UPDATE $table SET lock_owner = ?, lock_host = ?, lock_pid = ?, " +
          s"lock_token = ?, lock_acquired_at = ?, lock_fencing_token = ?, " +
          s"lock_expires_at_ms = ? " +
          s"WHERE source_id = ? AND (lock_token IS NULL OR lock_token = '' " +
          s"OR (lock_expires_at_ms IS NOT NULL AND lock_expires_at_ms <= ?))")
      try {
        statement.setString(1, record.info.owner)
        statement.setString(2, record.info.host)
        statement.setString(3, record.info.pid)
        statement.setString(4, record.token)
        statement.setString(5, record.info.acquiredAt)
        statement.setLong(6, record.info.fencingToken)
        this.setOptionalMillis(statement, 7, record.info.expiresAt)
        statement.setString(8, sourceId)
        statement.setLong(9, Instant.now().toEpochMilli)
        statement.executeUpdate() == 1
      } finally {
        statement.close()
      }
    }
  }

  override protected def renewLock(
      sourceId: String,
      token: String,
      expiresAt: Option[String]): Boolean = {
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"UPDATE $table SET lock_expires_at_ms = ? " +
          s"WHERE source_id = ? AND lock_token = ?")
      try {
        this.setOptionalMillis(statement, 1, expiresAt)
        statement.setString(2, sourceId)
        statement.setString(3, token)
        if (statement.executeUpdate() == 1) {
          true
        } else {
          // Some databases report zero changed rows when the expiry value is unchanged. The
          // lock still belongs to us if the token matches.
          val verify = connection.prepareStatement(
            s"SELECT 1 FROM $table WHERE source_id = ? AND lock_token = ?")
          try {
            verify.setString(1, sourceId)
            verify.setString(2, token)
            val result = verify.executeQuery()
            try result.next()
            finally result.close()
          } finally {
            verify.close()
          }
        }
      } finally {
        statement.close()
      }
    }
  }

  override protected def deleteLockIfToken(sourceId: String, token: String): Boolean = {
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"UPDATE $table SET lock_owner = NULL, lock_host = NULL, lock_pid = NULL, " +
          s"lock_token = NULL, lock_acquired_at = NULL, lock_fencing_token = NULL, " +
          s"lock_expires_at_ms = NULL WHERE source_id = ? AND lock_token = ?")
      try {
        statement.setString(1, sourceId)
        statement.setString(2, token)
        statement.executeUpdate() == 1
      } finally {
        statement.close()
      }
    }
  }

  override protected def deleteLock(sourceId: String): Boolean = {
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"UPDATE $table SET lock_owner = NULL, lock_host = NULL, lock_pid = NULL, " +
          s"lock_token = NULL, lock_acquired_at = NULL, lock_fencing_token = NULL, " +
          s"lock_expires_at_ms = NULL WHERE source_id = ? AND lock_token IS NOT NULL")
      try {
        statement.setString(1, sourceId)
        statement.executeUpdate() == 1
      } finally {
        statement.close()
      }
    }
  }

  /**
   * Store lease expiry as epoch milliseconds. ISO timestamps compare incorrectly as strings (for
   * example `...:00.100Z` sorts before `...:00Z`), which could treat a live lease as expired.
   */
  private def setOptionalMillis(
      statement: java.sql.PreparedStatement,
      index: Int,
      value: Option[String]): Unit = {
    value match {
      case Some(item) =>
        statement.setLong(index, Instant.parse(item).toEpochMilli)
      case None =>
        statement.setNull(index, java.sql.Types.BIGINT)
    }
  }

  override protected def listBackendSourceIds(): Seq[String] = {
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"SELECT source_id, state_json FROM $table ORDER BY source_id")
      try {
        val result = statement.executeQuery()
        try {
          val values = scala.collection.mutable.ListBuffer[String]()
          while (result.next()) {
            val sourceId = result.getString(1)
            val text = result.getString(2)
            try {
              if (text != null && !IncrementalStateCodec.decode(sourceId, text).isEmpty) {
                values += sourceId
              }
            } catch {
              case NonFatal(_) => values += sourceId
            }
          }
          values.toSeq
        } finally {
          result.close()
        }
      } finally {
        statement.close()
      }
    }
  }

  /**
   * A lock is attached to a source row. Create a valid empty row before the first lock attempt.
   */
  private def ensureRow(sourceId: String): Unit = {
    val state = IncrementalState(
      sourceId = sourceId,
      committedCursor = None,
      pending = None,
      updatedAt = java.time.Instant.now().toString)
    this.withConnection { connection =>
      val statement = connection.prepareStatement(
        s"INSERT INTO $table (source_id, state_json) VALUES (?, ?)")
      try {
        statement.setString(1, sourceId)
        statement.setString(2, IncrementalStateCodec.encode(state))
        statement.executeUpdate()
      } catch {
        case error: SQLException if this.isDuplicate(error) =>
        case error: SQLException =>
          throw this.failure("initialize state row", error)
      } finally {
        statement.close()
      }
    }
  }

  /**
   * Create the state table when absent, or add the TTL/fencing columns to a pre-TTL table.
   * Enabled with `migrateSchema=true`; the state tool also exposes it as `migrate`.
   */
  def migrateSchema(): Unit = {
    this.withConnection { connection =>
      val (configuredSchema, name) = this.tableParts
      val metadata = connection.getMetaData
      val catalog = this.currentCatalog(connection)
      val schema = configuredSchema.orElse(this.currentSchema(connection))
      val storedTable = this.storedTableName(metadata, catalog, schema, name)
      if (storedTable.isEmpty) {
        val statement = connection.createStatement()
        try {
          statement.executeUpdate(this.createTableSql)
        } finally {
          statement.close()
        }
      } else {
        val (storedSchema, storedName) = storedTable.get
        val columns = scala.collection.mutable.Set[String]()
        val result = metadata.getColumns(catalog.orNull, storedSchema.orNull, storedName, null)
        try {
          while (result.next()) {
            columns += result.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT)
          }
        } finally {
          result.close()
        }
        if (!columns.contains("lock_fencing_token")) {
          this.executeMigration(
            connection,
            s"ALTER TABLE $table ADD COLUMN lock_fencing_token BIGINT")
        }
        if (!columns.contains("lock_expires_at_ms")) {
          this.executeMigration(
            connection,
            s"ALTER TABLE $table ADD COLUMN lock_expires_at_ms BIGINT")
        }
      }
    }
  }

  /**
   * JDBC metadata patterns are case-sensitive in several databases even when unquoted DDL is
   * not. Try the configured spelling, upper case and lower case before deciding to create.
   * Restrict the lookup to the connection's catalog and current schema so a same-named table in
   * another schema or database is not mistaken for the state table.
   */
  private def storedTableName(
      metadata: java.sql.DatabaseMetaData,
      catalog: Option[String],
      schema: Option[String],
      name: String): Option[(Option[String], String)] = {
    val schemaNames = Seq(
      schema.orNull,
      schema.map(_.toUpperCase(java.util.Locale.ROOT)).orNull,
      schema.map(_.toLowerCase(java.util.Locale.ROOT)).orNull).distinct
    val tableNames = Seq(
      name,
      name.toUpperCase(java.util.Locale.ROOT),
      name.toLowerCase(java.util.Locale.ROOT)).distinct
    schemaNames.iterator.flatMap { schemaName =>
      tableNames.iterator.flatMap { tableName =>
        val tables = metadata.getTables(catalog.orNull, schemaName, tableName, Array("TABLE"))
        try {
          if (tables.next()) {
            Some(Option(tables.getString("TABLE_SCHEM")) -> tableName)
          } else {
            None
          }
        } finally {
          tables.close()
        }
      }
    }.toSeq.headOption
  }

  private def currentCatalog(connection: Connection): Option[String] = {
    try Option(connection.getCatalog).map(_.trim).filter(_.nonEmpty)
    catch {
      // Some drivers (or pooled proxies) do not implement catalog lookup.
      case NonFatal(_) => None
    }
  }

  private def currentSchema(connection: Connection): Option[String] = {
    try Option(connection.getSchema).map(_.trim).filter(_.nonEmpty)
    catch {
      case NonFatal(_) => None
    }
  }

  private def executeMigration(connection: Connection, sql: String): Unit = {
    val statement = connection.createStatement()
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  private def createTableSql: String = {
    s"CREATE TABLE $table (" +
      "source_id VARCHAR(255) PRIMARY KEY, " +
      "state_json TEXT NOT NULL, " +
      "lock_owner VARCHAR(255), " +
      "lock_host VARCHAR(255), " +
      "lock_pid VARCHAR(255), " +
      "lock_token VARCHAR(64), " +
      "lock_acquired_at VARCHAR(64), " +
      "lock_fencing_token BIGINT, " +
      "lock_expires_at_ms BIGINT)"
  }

  /** Split an optionally schema-qualified table name for JDBC metadata lookups. */
  private def tableParts: (Option[String], String) = {
    val parts = this.table.split("\\.").map(_.trim).filter(_.nonEmpty)
    if (parts.length > 1) {
      Some(parts(parts.length - 2)) -> parts.last
    } else {
      None -> parts.last
    }
  }

  private def booleanOption(key: String, default: Boolean): Boolean = {
    this.options.get(key).map(_.trim).filter(_.nonEmpty).map { value =>
      value.toLowerCase(java.util.Locale.ROOT) match {
        case "true" => true
        case "false" => false
        case _ => throw new PuddingException(s"$key must be true or false")
      }
    }.getOrElse(default)
  }

  private def withConnection[T](body: Connection => T): T = {
    val connection = try {
      (this.user, this.password) match {
        case (Some(name), Some(secret)) =>
          DriverManager.getConnection(this.url, name, secret)
        case (Some(name), None) =>
          DriverManager.getConnection(this.url, name, "")
        case _ =>
          DriverManager.getConnection(this.url)
      }
    } catch {
      case error: SQLException =>
        throw this.failure("connect", error)
    }
    try {
      body(connection)
    } catch {
      case error: SQLException =>
        throw this.failure("operation", error)
    } finally {
      try connection.close()
      catch { case _: SQLException => }
    }
  }

  private def rollback(connection: Connection): Unit = {
    try connection.rollback()
    catch { case _: SQLException => }
  }

  private def isDuplicate(error: SQLException): Boolean = {
    val state = Option(error.getSQLState).getOrElse("")
    state.startsWith("23") || error.getErrorCode == 1062
  }

  private def failure(action: String, error: SQLException): PuddingException = {
    new PuddingException(
      s"jdbc incremental state backend failed to $action: " +
        error.getClass.getSimpleName, error)
  }

  private def required(key: String): String = {
    this.options.get(key).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(
        s"jdbc incremental state backend requires option '$key'")
    }
  }

  private def optional(key: String): Option[String] = {
    this.options.get(key).map(_.trim).filter(_.nonEmpty)
  }

  private def tableName(value: String): String = {
    val normalized = Option(value).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("jdbc incremental state table can not be empty")
    }
    if (!normalized.matches("[A-Za-z0-9_.]+")) {
      throw new PuddingException(
        "jdbc incremental state table may contain only letters, digits, '_' and '.'")
    }
    normalized
  }
}

object JdbcStateBackend {

  /**
   * Create or upgrade the state table. This is intentionally separate from a normal job start;
   * callers opt in with `migrateSchema=true` or the state tool's `migrate` command.
   */
  def migrate(options: Map[String, String]): String = {
    val backend = new JdbcStateBackend(options + ("migrateSchema" -> "true"))
    backend.location
  }

  private[incremental] def lockTtlMillis(options: Map[String, String]): Long = {
    options.get("lockTtlMs")
      .map(_.trim).filter(_.nonEmpty).map { value =>
        try {
          val parsed = value.toLong
          if (parsed < 0L) {
            throw new PuddingException("lockTtlMs can not be negative")
          }
          parsed
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException("lockTtlMs must be a number of milliseconds")
        }
      }.getOrElse(0L)
  }
}

/**
 * Built-in provider for the JDBC state backend.
 */
class JdbcStateBackendProvider extends IncrementalStateBackendProvider {

  override def identifier: String = "jdbc"

  override def create(
      options: Map[String, String],
      hadoopConf: Configuration): IncrementalStateBackend =
    new JdbcStateBackend(options)
}
