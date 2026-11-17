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

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

import scala.util.control.NonFatal

import io.debezium.config.{Configuration => DebeziumConfiguration}
import io.debezium.document.{DocumentReader, DocumentWriter}
import io.debezium.relational.history.{
  AbstractDatabaseHistory,
  DatabaseHistoryException,
  DatabaseHistoryListener,
  HistoryRecord,
  HistoryRecordComparator
}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.errors.ConnectException

/**
 * Debezium schema history stored under the Spark checkpoint directory using Hadoop FileSystem.
 *
 * One immutable file per history record is used instead of an append-only file. This is required
 * for object stores such as S3A, which do not support append, and it keeps recovery independent
 * of the driver's local disk. Record file names are sortable and recovery replays them in order.
 */
class HadoopDatabaseHistory extends AbstractDatabaseHistory {

  private val sequence = new AtomicLong(0L)

  private val writer = DocumentWriter.defaultWriter()

  private val reader = DocumentReader.defaultReader()

  private var path: Path = _

  private var fileSystem: FileSystem = _

  override def configure(
      config: DebeziumConfiguration,
      comparator: HistoryRecordComparator,
      listener: DatabaseHistoryListener,
      useCatalogBeforeSchema: Boolean): Unit = {
    super.configure(config, comparator, listener, useCatalogBeforeSchema)
    val configured = config.getString(HadoopDatabaseHistory.PathKey)
    if (configured == null || configured.trim.isEmpty) {
      throw new ConnectException(
        s"${HadoopDatabaseHistory.PathKey} is required for ${
          classOf[HadoopDatabaseHistory].getSimpleName
        }")
    }
    this.path = new Path(configured)
    this.fileSystem = HadoopDatabaseHistory.fileSystem(this.path)
  }

  override def start(): Unit = {
    super.start()
    if (!this.fileSystem.exists(this.path)) {
      this.fileSystem.mkdirs(this.path)
    }
  }

  override def initializeStorage(): Unit = {
    if (!this.fileSystem.exists(this.path)) {
      this.fileSystem.mkdirs(this.path)
    }
  }

  override def storeRecord(record: HistoryRecord): Unit = {
    if (record == null) {
      return
    }
    try {
      this.initializeStorage()
      val name = f"${System.currentTimeMillis()}%013d-${this.sequence.incrementAndGet()}%08d.json"
      val destination = new Path(this.path, name)
      val output = this.fileSystem.create(destination, true)
      try {
        output.write(this.writer.write(record.document()).getBytes(StandardCharsets.UTF_8))
      } finally {
        output.close()
      }
    } catch {
      case NonFatal(error) =>
        throw new DatabaseHistoryException(
          s"unable to store schema history at ${this.path.toString}: " +
            error.getClass.getSimpleName,
          error)
    }
  }

  override protected def recoverRecords(records: Consumer[HistoryRecord]): Unit = {
    try {
      if (!this.fileSystem.exists(this.path)) {
        return
      }
      val statuses = this.fileSystem.listStatus(this.path)
        .filter(status => status.isFile && status.getPath.getName.endsWith(".json"))
        .sortBy(_.getPath.getName)
      statuses.foreach { status =>
        val input = this.fileSystem.open(status.getPath)
        try {
          val text = HadoopDatabaseHistory.readAll(input)
          if (text.trim.nonEmpty) {
            records.accept(new HistoryRecord(this.reader.read(text)))
          }
        } finally {
          input.close()
        }
      }
    } catch {
      case NonFatal(error) =>
        throw new DatabaseHistoryException(
          s"unable to recover schema history at ${this.path.toString}: " +
            error.getClass.getSimpleName,
          error)
    }
  }

  override def storageExists(): Boolean = this.fileSystem.exists(this.path)

  override def exists(): Boolean = {
    this.storageExists() && this.fileSystem.listStatus(this.path)
      .exists(status => status.isFile && status.getPath.getName.endsWith(".json"))
  }

  override def toString: String = s"hadoop ${Option(this.path).map(_.toString).getOrElse("(unstarted)")}"
}

object HadoopDatabaseHistory {

  val PathKey = "database.history.file.filename"

  private def readAll(input: InputStream): String = {
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    var read = input.read(buffer)
    while (read >= 0) {
      if (read > 0) {
        output.write(buffer, 0, read)
      }
      read = input.read(buffer)
    }
    new String(output.toByteArray, StandardCharsets.UTF_8)
  }

  /**
   * The driver-side poller can register the Spark Hadoop configuration for a checkpoint path.
   * This is used for object-store checkpoints whose credentials and endpoint are not in the
   * process-wide Hadoop defaults.
   */
  private val configurations =
    new java.util.concurrent.ConcurrentHashMap[String, HadoopConfiguration]()

  private val tokenConfigurations =
    new java.util.concurrent.ConcurrentHashMap[String, HadoopConfiguration]()

  def registerToken(token: String, configuration: HadoopConfiguration): Unit = {
    tokenConfigurations.put(token, configuration)
  }

  def releaseToken(token: String): Unit = {
    tokenConfigurations.remove(token)
  }

  def configuration(token: String): Option[HadoopConfiguration] = {
    Option(tokenConfigurations.get(token))
  }

  def register(path: String, configuration: HadoopConfiguration): Unit = {
    configurations.put(path, configuration)
  }

  def unregister(path: String): Unit = {
    configurations.remove(path)
  }

  private[cdc] def fileSystem(path: Path): FileSystem = {
    val uri = path.toUri
    val parent = Option(path.getParent).map(_.toString)
    val configured = Option(configurations.get(path.toString))
      .orElse(parent.flatMap(value => Option(configurations.get(value))))
    configured match {
      case Some(configuration) => FileSystem.get(uri, configuration)
      case None => FileSystem.get(uri, new HadoopConfiguration())
    }
  }
}
