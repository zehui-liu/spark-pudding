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

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.changestream.{ChangeStreamDocument, FullDocument}
import com.mongodb.client.{
  ChangeStreamIterable,
  MongoChangeStreamCursor,
  MongoClient,
  MongoCollection,
  MongoCursor,
  MongoDatabase
}
import org.bson.json.{JsonMode, JsonWriterSettings}
import org.bson.{BsonDocument, BsonTimestamp, BsonValue}
import org.bson.Document
import org.slf4j.LoggerFactory

import org.pudding.core.PuddingException
import org.pudding.core.util.ConfigRedactor

/**
 * Native MongoDB change-stream reader.
 *
 * The initial snapshot is read in `_id` order and a change stream is opened before the snapshot
 * starts, so changes that happen during the scan are not lost. Snapshot offsets and change-stream
 * resume tokens are both stored in the Spark checkpoint; no MongoDB-side state collection is
 * required. An interrupted snapshot restarts after its last emitted `_id`.
 */
private[cdc] final class MongodbCdcPoller(
    config: MongodbConfig,
    buffer: CdcRecordBuffer,
    startOffset: CdcOffset) extends CdcPoller {

  private val logger = LoggerFactory.getLogger(classOf[MongodbCdcPoller])

  private val JsonSettings = JsonWriterSettings.builder()
    .outputMode(JsonMode.RELAXED)
    .build()

  @volatile private var running = true

  @volatile private var client: MongoClient = null

  @volatile private var changeCursor:
      MongoChangeStreamCursor[ChangeStreamDocument[Document]] = null

  @volatile private var snapshotCursor: MongoCursor[Document] = null

  private var sequence: Long = startOffset.sequence

  private val thread = new Thread(new Runnable {
    override def run(): Unit = poll()
  }, s"pudding-cdc-mongodb-${config.database}-${config.collection}")

  thread.setDaemon(true)

  def start(): Unit = {
    if (startOffset.kind != CdcOffset.InitialKind &&
      startOffset.kind != CdcOffset.MongodbKind) {
      throw new PuddingException(
        s"${config.label} checkpoint belongs to a different CDC source type")
    }
    thread.start()
  }

  def commit(offset: CdcOffset): Unit = ()

  def stop(): Unit = {
    running = false
    this.closeCursor()
    thread.interrupt()
    try thread.join(TimeUnit.SECONDS.toMillis(10L))
    catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
  }

  private def poll(): Unit = {
    try {
      val mongo = MongoClients.create(config.uri)
      client = mongo
      val database = mongo.getDatabase(config.database)
      val collection = database.getCollection(config.collection)
      val state = this.initialState(startOffset)

      changeCursor = this.openChangeStream(collection, state)
      state.snapshotStartAt.foreach { startAt =>
        this.snapshot(collection, startAt, state.lastId)
      }
      this.stream()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
      case NonFatal(error) =>
        if (running) {
          val detail = Option(error.getMessage).filter(_.nonEmpty)
            .getOrElse(error.getClass.getSimpleName)
          buffer.fail(new PuddingException(
            s"${config.label} failed while reading changes: " +
              ConfigRedactor.redactLine(detail)))
        }
    } finally {
      this.closeCursor()
      val currentClient = client
      if (currentClient != null) {
        try currentClient.close()
        catch {
          case NonFatal(_) => ()
        }
        client = null
      }
      config.hadoopToken.foreach(HadoopDatabaseHistory.releaseToken)
    }
  }

  private case class StreamState(
      snapshotStartAt: Option[BsonTimestamp],
      lastId: Option[BsonValue],
      resumeToken: Option[BsonDocument],
      firstAttempt: Boolean)

  private def initialState(offset: CdcOffset): StreamState = {
    if (offset.kind == CdcOffset.InitialKind || offset.data.isEmpty) {
      if (config.snapshotMode == "initial") {
        StreamState(Some(this.currentOperationTime()), None, None, firstAttempt = true)
      } else {
        StreamState(None, None, None, firstAttempt = true)
      }
    } else {
      val mode = this.offsetString(offset, "mode")
      mode match {
        case "snapshot" =>
          val startAt = this.decodeTimestamp(this.offsetString(offset, "startAt"))
          val lastId = offset.data.get("lastId").map(value =>
            this.decodeBson(value.toString))
          StreamState(Some(startAt), lastId, None, firstAttempt = false)
        case "stream" =>
          val token = this.decodeDocument(this.offsetString(offset, "resumeToken"))
          StreamState(None, None, Some(token), firstAttempt = false)
        case other =>
          throw new PuddingException(
            s"${config.label} checkpoint has an unknown MongoDB mode: $other")
      }
    }
  }

  private def snapshot(
      collection: MongoCollection[Document],
      startAt: BsonTimestamp,
      lastId: Option[BsonValue]): Unit = {
    val filter = lastId match {
      case Some(value) =>
        new BsonDocument("_id", new BsonDocument("$gt", value))
      case None => new BsonDocument()
    }
    logger.info(s"starting MongoDB snapshot for '${config.table}'")
    val cursor = collection.find(filter)
      .sort(Sorts.ascending("_id"))
      .batchSize(1000)
      .noCursorTimeout(true)
      .iterator()
    snapshotCursor = cursor
    try {
      while (running && cursor.hasNext) {
        val document = cursor.next()
        val id = this.documentId(document)
        sequence += 1L
        val json = CdcRecordEncoder.mongoSnapshot(document, config, sequence)
        val data = Map[String, Any](
          "mode" -> "snapshot",
          "startAt" -> this.encodeTimestamp(startAt),
          "lastId" -> this.encodeBson(id))
        buffer.append(CdcOffset.mongodb(data, sequence), json)
      }
      logger.info(s"completed MongoDB snapshot for '${config.table}'")
    } finally {
      snapshotCursor = null
      try cursor.close()
      catch {
        case NonFatal(_) => ()
      }
    }
  }

  private def stream(): Unit = {
    while (running) {
      val cursor = changeCursor
      if (cursor == null) {
        throw new PuddingException(
          s"${config.label} change stream is not available")
      }
      val event = cursor.tryNext()
      if (event != null) {
        // The snapshot increments the sequence before emitting, so the first streamed
        // change must use the next value. Reusing the current value duplicates the last
        // snapshot sequence and violates the non-decreasing contract in the reader.
        val next = sequence + 1L
        this.changeEvent(event, next).foreach { case (json, cursorData) =>
          sequence = next
          buffer.append(CdcOffset.mongodb(cursorData, sequence), json)
        }
      }
    }
  }

  private def changeEvent(
      event: ChangeStreamDocument[Document],
      sequence: Long): Option[(String, Map[String, Any])] = {
    val operation = Option(event.getOperationType).map(_.getValue).getOrElse("unknown")
    val timestamp = this.clusterOrderingTime(event.getClusterTime)
    val json = operation match {
      case "insert" =>
        Some(CdcRecordEncoder.mongoInsert(
          this.requiredDocument(event, operation), timestamp, config, sequence))
      case "replace" =>
        Some(CdcRecordEncoder.mongoUpdate(
          this.requiredDocument(event, operation), timestamp, config, sequence))
      case "update" =>
        // updateLookup can return no document when the document was deleted between the
        // update and the lookup. Skip it; the subsequent delete event carries the key.
        Option(event.getFullDocument).map(document =>
          CdcRecordEncoder.mongoUpdate(document, timestamp, config, sequence))
      case "delete" =>
        val key = Option(event.getDocumentKey).flatMap(document =>
          Option(document.get("_id"))).getOrElse {
          throw new PuddingException(
            s"${config.label} delete event does not contain an _id")
        }
        Some(CdcRecordEncoder.mongoDelete(key, timestamp, config, sequence))
      case other =>
        throw new PuddingException(
          s"${config.label} received unsupported MongoDB change operation: $other")
    }
    json.map { value =>
      val token = Option(event.getResumeToken).getOrElse {
        throw new PuddingException(
          s"${config.label} change event does not contain a resume token")
      }
      val data = Map[String, Any](
        "mode" -> "stream",
        "resumeToken" -> this.encodeDocument(token))
      value -> data
    }
  }

  private def requiredDocument(
      event: ChangeStreamDocument[Document],
      operation: String): Document = {
    Option(event.getFullDocument).getOrElse {
      throw new PuddingException(
        s"${config.label} $operation event did not contain the full document")
    }
  }

  private def openChangeStream(
      collection: MongoCollection[Document],
      state: StreamState): MongoChangeStreamCursor[ChangeStreamDocument[Document]] = {
    val iterable: ChangeStreamIterable[Document] = collection.watch()
      .fullDocument(FullDocument.UPDATE_LOOKUP)
      .maxAwaitTime(config.maxAwaitTimeMs, TimeUnit.MILLISECONDS)
    val configured = state.resumeToken match {
      case Some(token) => iterable.resumeAfter(token)
      case None if state.firstAttempt =>
        // Opening a change stream without a start option begins at the cursor creation
        // point. A timestamp from 'hello' can lag the last oplog write, which would
        // replay DDL (for example the rename produced by an aggregation $out) that
        // happened before the snapshot.
        iterable
      case None =>
        // Resuming an interrupted snapshot: replay from the snapshot start so changes
        // that occurred while the snapshot was running are not lost.
        state.snapshotStartAt match {
          case Some(startAt) => iterable.startAtOperationTime(startAt)
          case None => iterable
        }
    }
    configured.cursor()
  }

  private def currentOperationTime(): BsonTimestamp = {
    val mongo = client
    if (mongo == null) {
      throw new PuddingException(s"${config.label} MongoDB client is not connected")
    }
    this.currentOperationTime(mongo.getDatabase(config.database))
  }

  private def currentOperationTime(database: MongoDatabase): BsonTimestamp = {
    val modern = try {
      Option(database.runCommand(new Document("hello", 1)).get("operationTime"))
        .collect { case value: BsonTimestamp => value }
    } catch {
      case NonFatal(_) => None
    }
    modern.orElse {
      try {
        Option(database.runCommand(new Document("isMaster", 1)).get("operationTime"))
          .collect { case value: BsonTimestamp => value }
      } catch {
        case NonFatal(_) => None
      }
    }.getOrElse {
      throw new PuddingException(
        s"${config.label} MongoDB deployment did not report an operation time; " +
          "change streams require a replica set or sharded cluster")
    }
  }

  private def documentId(document: Document): BsonValue = {
    val bson = document.toBsonDocument(classOf[Document],
      MongoClientSettings.getDefaultCodecRegistry())
    Option(bson.get("_id")).getOrElse {
      throw new PuddingException(
        s"${config.label} snapshot document has no _id")
    }
  }

  private def clusterOrderingTime(timestamp: BsonTimestamp): Long = {
    timestamp.getTime.toLong * 1000000000L + timestamp.getInc.toLong
  }

  private def offsetString(offset: CdcOffset, key: String): String = {
    offset.data.get(key).map(_.toString).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(
        s"${config.label} checkpoint is missing '$key'")
    }
  }

  private def encodeTimestamp(value: BsonTimestamp): String = {
    this.encodeBson(value)
  }

  private def decodeTimestamp(value: String): BsonTimestamp = {
    this.decodeBson(value) match {
      case timestamp: BsonTimestamp => timestamp
      case _ =>
        throw new PuddingException(
          s"${config.label} checkpoint contains an invalid MongoDB operation time")
    }
  }

  private def encodeBson(value: BsonValue): String = {
    new BsonDocument("v", value).toJson(JsonSettings)
  }

  private def decodeBson(value: String): BsonValue = {
    BsonDocument.parse(value).get("v")
  }

  private def encodeDocument(value: BsonDocument): String = {
    value.toJson(JsonSettings)
  }

  private def decodeDocument(value: String): BsonDocument = {
    BsonDocument.parse(value)
  }

  private def closeCursor(): Unit = {
    val currentSnapshot = snapshotCursor
    if (currentSnapshot != null) {
      snapshotCursor = null
      try currentSnapshot.close()
      catch {
        case NonFatal(_) => ()
      }
    }
    val currentChange = changeCursor
    if (currentChange != null) {
      changeCursor = null
      try currentChange.close()
      catch {
        case NonFatal(error) =>
          logger.warn(s"failed to close MongoDB change stream: " +
            ConfigRedactor.redactLine(Option(error.getMessage).getOrElse(
              error.getClass.getSimpleName)))
      }
    }
  }
}
