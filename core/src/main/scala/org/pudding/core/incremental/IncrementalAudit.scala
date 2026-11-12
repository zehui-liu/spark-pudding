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

import org.apache.spark.internal.Logging

/**
 * Structured audit events for incremental state transitions.
 *
 * Messages are single-line key/value records. They deliberately omit the private lock token and
 * all configuration values; source ids, transaction ids, cursors and fencing generations are
 * safe to retain in driver logs.
 */
object IncrementalAudit extends Logging {

  def lockAcquired(
      sourceId: String,
      info: IncrementalLockInfo,
      takeover: Boolean): Unit = {
    val event = if (takeover) "incremental.lock.takeover" else "incremental.lock.acquired"
    logInfo(this.render(
      event,
      "source" -> sourceId,
      "owner" -> info.owner,
      "host" -> info.host,
      "pid" -> info.pid,
      "fencingToken" -> info.fencingToken.toString,
      "expiresAt" -> info.expiresAt.getOrElse("none")))
  }

  def lockReleased(sourceId: String): Unit = {
    logInfo(this.render("incremental.lock.released", "source" -> sourceId))
  }

  def lockForced(sourceId: String, info: Option[IncrementalLockInfo]): Unit = {
    logWarning(this.render(
      "incremental.lock.forced",
      "source" -> sourceId,
      "owner" -> info.map(_.owner).getOrElse("unknown"),
      "host" -> info.map(_.host).getOrElse("unknown"),
      "pid" -> info.map(_.pid).getOrElse("unknown"),
      "fencingToken" -> info.map(_.fencingToken.toString).getOrElse("0")))
  }

  def lockLost(sourceId: String, reason: String): Unit = {
    logWarning(this.render(
      "incremental.lock.lost",
      "source" -> sourceId,
      "reason" -> reason))
  }

  def transactionBegin(
      sourceId: String,
      transactionId: String,
      fromCursor: Option[String],
      toCursor: String,
      fencingToken: Long): Unit = {
    logInfo(this.render(
      "incremental.transaction.begin",
      "source" -> sourceId,
      "transaction" -> transactionId,
      "fromCursor" -> fromCursor.getOrElse("<initial>"),
      "toCursor" -> toCursor,
      "fencingToken" -> fencingToken.toString))
  }

  def transactionCommit(
      sourceId: String,
      transactionId: String,
      toCursor: String,
      fencingToken: Long): Unit = {
    logInfo(this.render(
      "incremental.transaction.commit",
      "source" -> sourceId,
      "transaction" -> transactionId,
      "toCursor" -> toCursor,
      "fencingToken" -> fencingToken.toString))
  }

  def transactionAbort(
      sourceId: String,
      transactionId: String,
      fencingToken: Long): Unit = {
    logInfo(this.render(
      "incremental.transaction.abort",
      "source" -> sourceId,
      "transaction" -> transactionId,
      "fencingToken" -> fencingToken.toString))
  }

  def transactionReplay(
      sourceId: String,
      transactionId: String,
      attempts: Int): Unit = {
    logWarning(this.render(
      "incremental.transaction.replay",
      "source" -> sourceId,
      "transaction" -> transactionId,
      "attempts" -> attempts.toString))
  }

  def transactionFailure(
      sourceId: String,
      transactionId: Option[String],
      reason: String): Unit = {
    logWarning(this.render(
      "incremental.transaction.failure",
      "source" -> sourceId,
      "transaction" -> transactionId.getOrElse("<none>"),
      "reason" -> reason))
  }

  def stateWriteFailure(sourceId: String, reason: String): Unit = {
    logWarning(this.render(
      "incremental.state.write.failure",
      "source" -> sourceId,
      "reason" -> reason))
  }

  def fencingRejected(
      sink: String,
      sourceId: String,
      transactionId: String,
      fencingToken: Long,
      newestFencingToken: Long): Unit = {
    logWarning(this.render(
      "incremental.sink.fencing.rejected",
      "sink" -> sink,
      "source" -> sourceId,
      "transaction" -> transactionId,
      "fencingToken" -> fencingToken.toString,
      "newestFencingToken" -> newestFencingToken.toString))
  }

  def runCompleted(metrics: Map[String, Long]): Unit = {
    val fields = metrics.toSeq.sortBy(_._1).map { case (key, value) => key -> value.toString }
    logInfo(this.render("incremental.run.completed", fields: _*))
  }

  private def render(event: String, fields: (String, String)*): String = {
    val encoded = fields.map { case (key, value) =>
      s"$key=${this.quote(Option(value).getOrElse(""))}"
    }
    (s"event=$event" +: encoded).mkString(" ")
  }

  private def quote(value: String): String = {
    val singleLine = value.replace("\r", " ").replace("\n", " ")
    if (singleLine.exists(character =>
      character.isWhitespace || character == '"' || character == '=')) {
      "\"" + singleLine.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    } else {
      singleLine
    }
  }
}
