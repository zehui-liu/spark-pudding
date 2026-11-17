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

package org.pudding.source.cdc

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.debezium.connector.mysql.MySqlConnector
import io.debezium.connector.postgresql.PostgresConnector
import org.apache.kafka.connect.source.{SourceConnector, SourceTask, SourceTaskContext}
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.slf4j.LoggerFactory

import org.pudding.core.PuddingException
import org.pudding.core.util.ConfigRedactor

/**
 * Drives one Debezium connector task on the Spark driver and feeds converted rows into the
 * micro-batch buffer. Spark's checkpoint is the only offset store; Kafka Connect and its offset
 * topic are intentionally not required.
 */
private[cdc] final class DebeziumCdcPoller(
    config: DebeziumConfig,
    buffer: CdcRecordBuffer,
    startOffset: CdcOffset,
    checkpointLocation: String) extends CdcPoller {

  private val logger = LoggerFactory.getLogger(classOf[DebeziumCdcPoller])

  private val TransientRetryMillis = 2000L

  @volatile private var running = true

  @volatile private var task: SourceTask = null

  @volatile private var connector: SourceConnector = null

  private val thread = new Thread(new Runnable {
    override def run(): Unit = poll()
  }, s"pudding-cdc-${config.kind}-${config.serverName}")

  thread.setDaemon(true)

  def start(): Unit = {
    if (startOffset.kind != CdcOffset.InitialKind &&
      startOffset.kind != CdcOffset.DebeziumKind) {
      throw new PuddingException(
        s"${config.label} checkpoint belongs to a different CDC source type")
    }
    thread.start()
  }

  def commit(offset: CdcOffset): Unit = {
    // The Spark offset log is the source of truth. Debezium's history store, when configured,
    // is flushed by task.stop(); no concurrent call into the connector is made here.
    ()
  }

  def stop(): Unit = {
    running = false
    val current = task
    if (current != null) {
      try current.stop()
      catch {
        case NonFatal(_) => ()
      }
    }
    thread.interrupt()
    try thread.join(TimeUnit.SECONDS.toMillis(10L))
    catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
  }

  private def poll(): Unit = {
    var historyPath: Option[String] = None
    try {
      historyPath = this.historyPath()
      historyPath.foreach { path =>
        config.hadoopToken
          .flatMap(HadoopDatabaseHistory.configuration)
          .foreach(configuration => HadoopDatabaseHistory.register(path, configuration))
      }
      var lastOffset = this.savedOffset()
      var sourceTask = this.openTask(lastOffset)
      logger.info(s"started ${config.kind} CDC task for '${config.table}'")

      var sequence = startOffset.sequence + 1L
      while (running) {
        val records =
          try {
            sourceTask.poll()
          } catch {
            case _: InterruptedException =>
              throw new InterruptedException()
            case NonFatal(error) if running && this.isTransient(error) =>
              // Debezium stops its task after a connection failure ("This connector will be
              // stopped"), so re-polling the same task can not recover. Recreate the
              // connector/task from the last emitted offset; a database restart or a short
              // network partition then resumes at the saved binlog/LSN position.
              logger.warn(
                s"${config.label} CDC task hit a transient connection error; reconnecting: " +
                  ConfigRedactor.redactLine(Option(error.getMessage).getOrElse(
                    error.getClass.getSimpleName)))
              var reconnected = false
              while (running && !reconnected) {
                this.stopConnector()
                Thread.sleep(TransientRetryMillis)
                try {
                  sourceTask = this.openTask(lastOffset)
                  reconnected = true
                  logger.info(s"reconnected ${config.kind} CDC task for '${config.table}'")
                } catch {
                  case _: InterruptedException =>
                    throw new InterruptedException()
                  case NonFatal(retryError) if running && this.isTransient(retryError) =>
                    logger.warn(
                      s"${config.label} CDC reconnect failed; retrying: " +
                        ConfigRedactor.redactLine(Option(retryError.getMessage).getOrElse(
                          retryError.getClass.getSimpleName)))
                }
              }
              null
          }
        if (records != null && !records.isEmpty) {
          records.asScala.foreach { record =>
            if (running) {
              CdcRecordEncoder.debezium(record, config, sequence).foreach { json =>
                val sourceOffset = Option(record.sourceOffset())
                val cursor = sourceOffset
                  .map(_.asScala.toMap)
                  .getOrElse(Map.empty[String, Any])
                lastOffset = sourceOffset.map(offset =>
                  new java.util.HashMap[String, Object](
                    offset.asInstanceOf[java.util.Map[String, Object]]))
                buffer.append(CdcOffset.debezium(cursor, sequence), json)
                // Heartbeat and transaction-metadata records are skipped without consuming a
                // sequence, so the synthetic sequence stays contiguous for data rows.
                sequence += 1L
              }
            }
          }
        }
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
      case NonFatal(error) =>
        if (running) {
          // Debezium often wraps the useful failure in a cause with an empty message. Keep the
          // class chain so the operator can see whether this is a connector, driver or config
          // failure without exposing option values.
          val detail = Iterator.iterate(error)(_.getCause)
            .takeWhile(_ != null)
            .map(cause => {
              val message = Option(cause.getMessage).filter(_.nonEmpty)
                .getOrElse("<no message>")
              s"${cause.getClass.getName}: $message"
            })
            .mkString(" <- ")
          logger.warn(s"${config.label} CDC task failed", error)
          buffer.fail(new PuddingException(
            s"${config.label} failed while reading changes: " +
              ConfigRedactor.redactLine(detail)))
        }
    } finally {
      this.stopConnector()
      historyPath.foreach(HadoopDatabaseHistory.unregister)
      config.hadoopToken.foreach(HadoopDatabaseHistory.releaseToken)
    }
  }

  /**
   * Connection-level failures are retriable; configuration, authentication and schema errors
   * are not, so they still fail the job immediately with the original message.
   */
  private def isTransient(error: Throwable): Boolean = {
    Iterator.iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .exists { cause =>
        cause.isInstanceOf[org.apache.kafka.connect.errors.RetriableException] ||
          cause.isInstanceOf[java.sql.SQLTransientException] ||
          cause.isInstanceOf[java.net.ConnectException] ||
          cause.isInstanceOf[java.net.SocketTimeoutException] ||
          cause.isInstanceOf[java.net.SocketException] ||
          cause.isInstanceOf[java.io.EOFException] ||
          cause.getClass.getName.endsWith("CommunicationsException") ||
          (cause.isInstanceOf[java.sql.SQLException] &&
            Option(cause.asInstanceOf[java.sql.SQLException].getSQLState)
              .exists(_.startsWith("08")))
      }
  }

  /**
   * Create and start a fresh Debezium connector and task. Used for the initial start and for
   * reconnecting after a transient failure; Debezium tasks can not be polled after they have
   * stopped themselves.
   */
  private def openTask(
      savedOffset: Option[java.util.Map[String, Object]]): SourceTask = {
    val props = this.connectorProperties()
    val sourceConnector = this.newConnector()
    connector = sourceConnector
    sourceConnector.start(props.asJava)
    val taskClass = sourceConnector.taskClass()
    val sourceTask = taskClass.getDeclaredConstructor().newInstance().asInstanceOf[SourceTask]
    task = sourceTask
    sourceTask.initialize(new DriverTaskContext(props, savedOffset))
    sourceTask.start(sourceConnector.taskConfigs(1).get(0))
    sourceTask
  }

  private def historyPath(): Option[String] = {
    if (config.kind == "mysql" &&
      !config.connectorOptions.contains("database.history")) {
      Some(new org.apache.hadoop.fs.Path(
        new org.apache.hadoop.fs.Path(this.checkpointLocation),
        s"cdc-schema-history/${config.serverName}").toString)
    } else {
      None
    }
  }

  private def savedOffset(): Option[java.util.Map[String, Object]] = {
    if (startOffset.kind == CdcOffset.DebeziumKind && startOffset.data.nonEmpty) {
      Some(startOffset.javaData)
    } else {
      None
    }
  }

  private def newConnector(): SourceConnector = {
    config.kind match {
      case "mysql" => new MySqlConnector()
      case "postgres" => new PostgresConnector()
      case other =>
        throw new PuddingException(s"unsupported Debezium connector kind: $other")
    }
  }

  private def connectorProperties(): Map[String, String] = {
    val required = config.kind match {
      case "mysql" =>
        Map(
          "name" -> s"pudding-${config.serverName}",
          "connector.class" -> "io.debezium.connector.mysql.MySqlConnector",
          "database.hostname" -> config.host,
          "database.port" -> config.port.toString,
          "database.user" -> config.user,
          "database.password" -> config.password,
          "database.server.id" -> config.serverId.toString,
          "database.server.name" -> config.serverName,
          "database.include.list" -> config.database,
          "table.include.list" -> s"${config.database}.${config.tableName}",
          "snapshot.mode" -> config.snapshotMode,
          "tombstones.on.delete" -> "false",
          "include.schema.changes" -> "false",
          "decimal.handling.mode" -> "precise",
          "time.precision.mode" -> "connect")
      case "postgres" =>
        val schemaName = config.schemaName.getOrElse("public")
        Map(
          "name" -> s"pudding-${config.serverName}",
          "connector.class" -> "io.debezium.connector.postgresql.PostgresConnector",
          "database.hostname" -> config.host,
          "database.port" -> config.port.toString,
          "database.user" -> config.user,
          "database.password" -> config.password,
          "database.dbname" -> config.database,
          "database.server.name" -> config.serverName,
          "schema.include.list" -> schemaName,
          "table.include.list" -> s"$schemaName.${config.tableName}",
          "plugin.name" -> "pgoutput",
          "publication.autocreate.mode" -> "filtered",
          "snapshot.mode" -> config.snapshotMode,
          "tombstones.on.delete" -> "false",
          "include.schema.changes" -> "false",
          "decimal.handling.mode" -> "precise",
          "time.precision.mode" -> "connect")
      case other =>
        throw new PuddingException(s"unsupported Debezium connector kind: $other")
    }
    val defaults = config.kind match {
      case "mysql" =>
        this.historyPath().map { historyPath =>
          Map(
            "database.history" -> classOf[HadoopDatabaseHistory].getName,
            HadoopDatabaseHistory.PathKey -> historyPath)
        }.getOrElse(Map.empty[String, String])
      case "postgres" =>
        Map(
          "slot.name" -> this.postgresName("slot"),
          "publication.name" -> this.postgresName("publication"))
      case _ => Map.empty[String, String]
    }
    defaults ++ config.connectorOptions ++ required
  }

  private def postgresName(suffix: String): String = {
    val suffixText = if (suffix == "slot") "slot" else "pub"
    val hash = Integer.toHexString((config.database + "." +
      config.schemaName.getOrElse("public") + "." + config.tableName).hashCode)
    s"pudding_${suffixText}_$hash"
  }

  private def stopConnector(): Unit = {
    val currentTask = task
    if (currentTask != null) {
      try currentTask.stop()
      catch {
        case NonFatal(_) => ()
      }
      task = null
    }
    val currentConnector = connector
    if (currentConnector != null) {
      try currentConnector.stop()
      catch {
        case NonFatal(error) =>
          logger.warn(s"failed to stop ${config.kind} CDC connector: " +
            ConfigRedactor.redactLine(Option(error.getMessage).getOrElse(
              error.getClass.getSimpleName)))
      }
      connector = null
    }
  }

  private class DriverTaskContext(
      values: Map[String, String],
      saved: Option[java.util.Map[String, Object]]) extends SourceTaskContext {

    override def configs(): java.util.Map[String, String] = values.asJava

    override def offsetStorageReader(): OffsetStorageReader = new OffsetStorageReader {
      override def offset[T](
          partition: java.util.Map[String, T]): java.util.Map[String, Object] = {
        saved.orNull
      }

      override def offsets[T](
          partitions: util.Collection[java.util.Map[String, T]]):
          util.Map[util.Map[String, T], util.Map[String, Object]] = {
        saved match {
          case Some(value) =>
            partitions.asScala.map(partition => partition -> value).toMap.asJava
          case None => util.Collections.emptyMap()
        }
      }
    }
  }
}
