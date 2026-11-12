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

package org.pudding.provider.state.etcd

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, URI, URL}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

import org.pudding.core.PuddingException
import org.pudding.core.incremental.{
  AbstractIncrementalStateBackend,
  IncrementalLockRecord,
  IncrementalState,
  IncrementalStateBackend,
  IncrementalStateBackendProvider,
  IncrementalStateCodec,
  LockDocument
}

/**
 * etcd state backend using the etcd v3 HTTP/JSON gateway (`/v3/kv/range` and `/v3/kv/txn`).
 *
 * State writes are compare-and-swap transactions on the lock value, so a stale writer fails
 * instead of overwriting newer state. A positive `lockTtlMs` attaches a native etcd lease to the
 * lock key and replaces the lease on every renewal; an abrupt process exit removes the key when
 * the lease expires. An expired lock without a lease is also replaced with a transaction that
 * compares the old value.
 *
 * Options: `endpoint` (comma-separated, default `http://127.0.0.1:2379`), `apiPrefix` (`/v3`),
 * `user`, `password`, `prefix` (`pudding:incremental:`), `timeoutMs` (5000) and `lockTtlMs`
 * (0, disabled). The location shown by the state tool never contains credentials.
 */
class EtcdStateBackend(options: Map[String, String])
    extends AbstractIncrementalStateBackend(EtcdStateBackend.lockTtlMillis(options)) {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val endpoints = {
    val values = this.options.get("endpoint")
      .map(_.trim).filter(_.nonEmpty).getOrElse("http://127.0.0.1:2379")
      .split(",").map(_.trim).filter(_.nonEmpty)
      .map(value => this.checkedEndpoint(value).stripSuffix("/")).toSeq
    if (values.isEmpty) {
      throw new PuddingException("endpoint must contain at least one etcd URL")
    }
    values
  }

  private val apiPrefix = {
    val value = this.options.get("apiPrefix")
      .map(_.trim).filter(_.nonEmpty).getOrElse("/v3")
    "/" + value.stripPrefix("/").stripSuffix("/")
  }

  private val prefix = {
    val value = this.options.get("prefix")
      .map(_.trim).filter(_.nonEmpty).getOrElse("pudding:incremental:")
    if (!value.forall(character => character >= 32 && character < 127)) {
      throw new PuddingException("etcd state prefix must contain printable ASCII characters")
    }
    value
  }

  private val timeoutMs = {
    val value = this.options.get("timeoutMs")
      .map(_.trim).filter(_.nonEmpty).map { item =>
        try item.toInt catch {
          case _: NumberFormatException =>
            throw new PuddingException("timeoutMs must be an integer")
        }
      }.getOrElse(5000)
    if (value <= 0) {
      throw new PuddingException("timeoutMs must be positive")
    }
    value
  }

  private val endpointCursor = new AtomicInteger(0)

  private val authorization: Option[String] = {
    val user = this.options.get("user").map(_.trim).filter(_.nonEmpty)
    val password = this.options.get("password")
    if (user.isEmpty && password.isEmpty) {
      None
    } else {
      val raw = user.getOrElse("") + ":" + password.getOrElse("")
      Some("Basic " + Base64.getEncoder.encodeToString(raw.getBytes(StandardCharsets.UTF_8)))
    }
  }

  override def location: String = {
    val hosts = endpoints.map { value =>
      val uri = new URI(value)
      val port = if (uri.getPort > 0) ":" + uri.getPort else ""
      uri.getHost + port
    }
    "etcd://" + hosts.mkString(",")
  }

  override protected def readState(sourceId: String): Option[IncrementalState] = {
    this.readRaw(this.stateKey(sourceId)).map(text => IncrementalStateCodec.decode(sourceId, text))
  }

  override protected def persistState(state: IncrementalState, token: String): Unit = {
    val lockKey = this.lockKey(state.sourceId)
    val current = this.readRaw(lockKey).getOrElse {
      throw this.lostLock(state.sourceId)
    }
    if (LockDocument.decode(current, lockKey).token != token) {
      throw this.lostLock(state.sourceId)
    }
    val written = this.transaction(
      this.compareValue(lockKey, current),
      Seq(this.putRequest(this.stateKey(state.sourceId), IncrementalStateCodec.encode(state))))
    if (!written && !this.readState(state.sourceId).contains(state)) {
      // The transaction may have been applied before a lost response; a retry then reports
      // `succeeded=false`. Treat the write as successful only when it is actually visible.
      throw this.lostLock(state.sourceId)
    }
  }

  override protected def readLock(sourceId: String): Option[IncrementalLockRecord] = {
    val key = this.lockKey(sourceId)
    this.readRaw(key).map(text => LockDocument.decode(text, key).record)
  }

  override protected def createLock(
      sourceId: String,
      record: IncrementalLockRecord): Boolean = {
    val key = this.lockKey(sourceId)
    val lease = this.grantLease()
    val document = LockDocument.encode(this.lockDocument(record, lease))
    val created = try {
      this.readRaw(key) match {
        case None =>
          this.transaction(
            this.compareVersionZero(key),
            Seq(this.putRequest(key, document, lease)))
        case Some(current) =>
          val currentDocument = LockDocument.decode(current, key)
          if (!this.isExpired(currentDocument.record.info)) {
            false
          } else {
            val replaced = this.transaction(
              this.compareValue(key, current),
              Seq(this.putRequest(key, document, lease)))
            if (replaced) {
              currentDocument.leaseId.foreach(this.revokeLease)
            }
            replaced
          }
      }
    } catch {
      case NonFatal(error) =>
        // A lost response does not mean the transaction failed. Re-read before revoking the
        // lease: revoking a lease attached to the live lock would delete the lock.
        if (this.lockAttached(key, record.token, lease)) {
          true
        } else {
          lease.foreach(this.revokeLease)
          throw error
        }
    }
    if (!created) {
      if (this.lockAttached(key, record.token, lease)) {
        true
      } else {
        lease.foreach(this.revokeLease)
        false
      }
    } else {
      true
    }
  }

  override protected def renewLock(
      sourceId: String,
      token: String,
      expiresAt: Option[String]): Boolean = {
    if (this.lockTtlMillis <= 0L) {
      false
    } else {
      val key = this.lockKey(sourceId)
      val lease = this.grantLease()
      val renewed = try {
        this.readRaw(key) match {
          case Some(current) =>
            val document = LockDocument.decode(current, key)
            if (document.token != token) {
              false
            } else {
              val updated = document.copy(expiresAt = expiresAt, leaseId = lease)
              val success = this.transaction(
                this.compareValue(key, current),
                Seq(this.putRequest(key, LockDocument.encode(updated), lease)))
              if (success) {
                document.leaseId.foreach(this.revokeLease)
              }
              success
            }
          case None => false
        }
      } catch {
        case NonFatal(error) =>
          // The renewal transaction may have succeeded even though the response was lost.
          // Verify the intended document before revoking a lease that may now own the lock.
          if (this.renewalVisible(key, token, expiresAt, lease)) {
            true
          } else {
            lease.foreach(this.revokeLease)
            throw error
          }
      }
      if (!renewed) {
        if (this.renewalVisible(key, token, expiresAt, lease)) {
          true
        } else {
          lease.foreach(this.revokeLease)
          false
        }
      } else {
        true
      }
    }
  }

  override protected def deleteLockIfToken(sourceId: String, token: String): Boolean = {
    val key = this.lockKey(sourceId)
    this.readRaw(key) match {
      case Some(current) =>
        val document = LockDocument.decode(current, key)
        if (document.token != token) {
          false
        } else {
          val deleted = try {
            this.transaction(
              this.compareValue(key, current),
              Seq(this.deleteRequest(key)))
          } catch {
            case NonFatal(error) =>
              // A lost response may still have removed the key.
              if (this.readRaw(key).isEmpty) true else throw error
          }
          if (deleted) {
            document.leaseId.foreach(this.revokeLease)
          }
          deleted
        }
      case None => false
    }
  }

  override protected def deleteLock(sourceId: String): Boolean = {
    val key = this.lockKey(sourceId)
    this.readRaw(key) match {
      case Some(current) =>
        val document = LockDocument.decode(current, key)
        val deleted = try {
          this.transaction(
            this.compareValue(key, current),
            Seq(this.deleteRequest(key)))
        } catch {
          case NonFatal(error) =>
            if (this.readRaw(key).isEmpty) true else throw error
        }
        if (deleted) {
          document.leaseId.foreach(this.revokeLease)
        }
        deleted
      case None => false
    }
  }

  override protected def listBackendSourceIds(): Seq[String] = {
    val suffix = "/state"
    this.range(this.prefix, Some(this.prefixEnd(this.prefix)), keysOnly = false)
      .flatMap { case (key, value) =>
        if (!key.endsWith(suffix)) {
          None
        } else {
          val sourceId = key.substring(this.prefix.length, key.length - suffix.length)
          if (sourceId.isEmpty) {
            None
          } else {
            try {
              if (IncrementalStateCodec.decode(sourceId, value).isEmpty) None
              else Some(sourceId)
            } catch {
              case NonFatal(_) => Some(sourceId)
            }
          }
        }
      }
  }

  override def close(): Unit = super.close()

  /** Whether the lock key currently holds our token (and lease, when one was granted). */
  private def lockAttached(
      key: String,
      token: String,
      lease: Option[Long]): Boolean = {
    try {
      this.readRaw(key).exists { current =>
        val document = LockDocument.decode(current, key)
        document.token == token && lease.forall(id => document.leaseId.contains(id))
      }
    } catch {
      case NonFatal(_) => false
    }
  }

  /** Whether a renewal we may have applied is already visible in etcd. */
  private def renewalVisible(
      key: String,
      token: String,
      expiresAt: Option[String],
      lease: Option[Long]): Boolean = {
    try {
      this.readRaw(key).exists { current =>
        val document = LockDocument.decode(current, key)
        document.token == token && document.expiresAt == expiresAt &&
          lease.forall(id => document.leaseId.contains(id))
      }
    } catch {
      case NonFatal(_) => false
    }
  }

  private def lockDocument(
      record: IncrementalLockRecord,
      leaseId: Option[Long]): LockDocument = {
    LockDocument(
      token = record.token,
      owner = record.info.owner,
      host = record.info.host,
      pid = record.info.pid,
      acquiredAt = record.info.acquiredAt,
      fencingToken = record.info.fencingToken,
      expiresAt = record.info.expiresAt,
      leaseId = leaseId)
  }

  private def stateKey(sourceId: String): String =
    this.prefix + this.requireSourceId(sourceId) + "/state"

  private def lockKey(sourceId: String): String =
    this.prefix + this.requireSourceId(sourceId) + "/lock"

  private def readRaw(key: String): Option[String] = {
    this.range(key, None, keysOnly = false).headOption.map(_._2)
  }

  /**
   * Read a single key or a key prefix. etcd range_end is exclusive, so the end is the next
   * lexicographic key after the prefix.
   */
  private def range(
      key: String,
      rangeEnd: Option[String],
      keysOnly: Boolean): Seq[(String, String)] = {
    val fields = List(JField("key", JString(this.encode(key)))) ++
      rangeEnd.map(value => JField("range_end", JString(this.encode(value)))) ++
      (if (keysOnly) List(JField("keys_only", JBool(true))) else Nil)
    val response = this.post("/kv/range", JObject(fields))
    (response \ "kvs") match {
      case JArray(items) =>
        items.map {
          case item: JObject =>
            val storedKey = this.requiredString(item, "key")
            val value = (item \ "value") match {
              case JString(text) => this.decode(text)
              case _ => ""
            }
            this.decode(storedKey) -> value
          case _ =>
            throw new PuddingException("etcd incremental state backend returned an invalid kv")
        }
      case JNothing | JNull => Seq.empty
      case _ =>
        throw new PuddingException("etcd incremental state backend returned invalid kvs")
    }
  }

  private def transaction(compare: JValue, success: Seq[JValue]): Boolean = {
    val response = this.post(
      "/kv/txn",
      JObject(List(
        JField("compare", JArray(List(compare))),
        JField("success", JArray(success.toList)),
        JField("failure", JArray(Nil)))))
    (response \ "succeeded") match {
      case JBool(value) => value
      case _ => false
    }
  }

  private def compareVersionZero(key: String): JValue =
    JObject(List(
      JField("target", JString("VERSION")),
      JField("result", JString("EQUAL")),
      JField("key", JString(this.encode(key))),
      JField("version", JString("0"))))

  private def compareValue(key: String, value: String): JValue =
    JObject(List(
      JField("target", JString("VALUE")),
      JField("result", JString("EQUAL")),
      JField("key", JString(this.encode(key))),
      JField("value", JString(this.encode(value)))))

  private def putRequest(
      key: String,
      value: String,
      leaseId: Option[Long] = None): JValue = {
    val fields = List(
      JField("key", JString(this.encode(key))),
      JField("value", JString(this.encode(value)))) ++
      leaseId.map(id => JField("lease", JString(id.toString)))
    JObject(List(JField("requestPut", JObject(fields))))
  }

  private def grantLease(): Option[Long] = {
    if (this.lockTtlMillis <= 0L) {
      None
    } else {
      val seconds = math.max(1L, (this.lockTtlMillis + 999L) / 1000L)
      val response = this.post(
        "/lease/grant",
        JObject(List(JField("TTL", JString(seconds.toString)))))
      (response \ "ID") match {
        case JString(value) =>
          try Some(value.toLong)
          catch {
            case _: NumberFormatException =>
              throw new PuddingException("etcd lease grant returned an invalid ID")
          }
        case JInt(value) => Some(value.toLong)
        case _ =>
          throw new PuddingException("etcd lease grant returned no lease ID")
      }
    }
  }

  private def revokeLease(id: Long): Unit = {
    try {
      this.post(
        "/lease/revoke",
        JObject(List(JField("ID", JString(id.toString)))))
    } catch {
      case NonFatal(_) =>
        // A revoked or expired lease is already gone; lease cleanup is best effort.

    }
  }

  private def deleteRequest(key: String): JValue =
    JObject(List(JField(
      "requestDeleteRange",
      JObject(List(JField("key", JString(this.encode(key))))))))

  private def post(path: String, body: JValue): JValue = {
    val payload = Serialization.write(body).getBytes(StandardCharsets.UTF_8)
    var lastError: Option[Throwable] = None
    var accepted: Option[JValue] = None
    var attempts = 0
    while (accepted.isEmpty && attempts < this.endpoints.size) {
      val index = math.abs(this.endpointCursor.getAndIncrement() % this.endpoints.size)
      val endpoint = this.endpoints(index)
      try {
        accepted = Some(this.postOne(endpoint, path, payload))
      } catch {
        case NonFatal(error) => lastError = Some(error)
      }
      attempts += 1
    }
    accepted.getOrElse {
      throw new PuddingException(
        "etcd incremental state backend request failed: " +
          lastError.map(_.getClass.getSimpleName).getOrElse("no endpoint"),
        lastError.orNull)
    }
  }

  private def postOne(endpoint: String, path: String, payload: Array[Byte]): JValue = {
    val connection = new URL(endpoint + this.apiPrefix + path)
      .openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setConnectTimeout(this.timeoutMs)
      connection.setReadTimeout(this.timeoutMs)
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json")
      this.authorization.foreach(connection.setRequestProperty("Authorization", _))
      val output = connection.getOutputStream
      try output.write(payload)
      finally output.close()
      val status = connection.getResponseCode
      if (status < 200 || status >= 300) {
        val error = connection.getErrorStream
        if (error != null) {
          try error.close()
          catch { case NonFatal(_) => }
        }
        throw new PuddingException(
          s"etcd incremental state backend returned HTTP $status")
      }
      val input = connection.getInputStream
      val text = try this.readAll(input)
      finally input.close()
      parse(text)
    } finally {
      connection.disconnect()
    }
  }

  private def readAll(stream: InputStream): String = {
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](4096)
    var read = stream.read(buffer)
    while (read >= 0) {
      output.write(buffer, 0, read)
      read = stream.read(buffer)
    }
    new String(output.toByteArray, StandardCharsets.UTF_8)
  }

  private def prefixEnd(value: String): String = {
    var index = value.length - 1
    while (index >= 0 && value.charAt(index) == 0xffff.toChar) {
      index -= 1
    }
    if (index < 0) {
      "\u0000"
    } else {
      value.substring(0, index) + (value.charAt(index) + 1).toChar
    }
  }

  private def requiredString(item: JObject, field: String): String = {
    (item \ field) match {
      case JString(value) if value.nonEmpty => value
      case _ =>
        throw new PuddingException(
          s"etcd incremental state backend response is missing $field")
    }
  }

  private def encode(value: String): String =
    Base64.getEncoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def decode(value: String): String =
    new String(Base64.getDecoder.decode(value), StandardCharsets.UTF_8)

  /**
   * Reject endpoint URLs that embed credentials. Credentials belong in the `user`/`password`
   * options so they never appear in the location printed by the state tool.
   */
  private def checkedEndpoint(value: String): String = {
    val uri = try new URI(value) catch {
      case error: Exception =>
        throw new PuddingException(
          s"invalid etcd endpoint: ${error.getClass.getSimpleName}", error)
    }
    if (uri.getUserInfo != null) {
      throw new PuddingException(
        "etcd endpoint must not contain credentials; use user and password options")
    }
    if (uri.getHost == null || (uri.getScheme != "http" && uri.getScheme != "https")) {
      throw new PuddingException("etcd endpoint must be an http or https URL")
    }
    value
  }

  private def requireSourceId(sourceId: String): String = {
    Option(sourceId).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("source id can not be empty for incremental state")
    }
  }
}

object EtcdStateBackend {

  def lockTtlMillis(options: Map[String, String]): Long = {
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
 * Built-in provider for the etcd state backend.
 */
class EtcdStateBackendProvider extends IncrementalStateBackendProvider {

  override def identifier: String = "etcd"

  override def create(
      options: Map[String, String],
      hadoopConf: Configuration): IncrementalStateBackend =
    new EtcdStateBackend(options)
}
