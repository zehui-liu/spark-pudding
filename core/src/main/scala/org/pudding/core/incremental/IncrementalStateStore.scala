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

package org.pudding.core.incremental

import java.io.{FileInputStream, IOException}
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Locale, Properties, ServiceLoader, UUID}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  ChecksumException,
  FileAlreadyExistsException,
  FileContext,
  FileSystem,
  Options,
  Path
}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

import org.pudding.core.PuddingException
import org.pudding.core.connection.SecretResolver
import org.pudding.core.definition.IncrementalSpec
import org.pudding.core.util.ConfigRedactor

/**
 * Storage contract for incremental state. A backend owns both the cursor journal and the
 * single-writer lock. Implementations must make lock acquisition atomic and state replacement
 * atomic; the shared transition logic in [[AbstractIncrementalStateBackend]] relies on that.
 */
trait IncrementalStateBackend {

  def location: String

  def load(sourceId: String): IncrementalState

  def acquire(sourceId: String): IncrementalLockInfo

  def release(sourceId: String): Boolean

  def forceUnlock(sourceId: String): Boolean

  def lockInfo(sourceId: String): Option[IncrementalLockInfo]

  def ensureLock(sourceId: String): Unit

  /**
   * The fencing generation owned by this process, or `None` when it no longer holds the lock.
   * Callers pass this value to sinks instead of re-reading the lock, which could observe a
   * newer writer's generation after a takeover.
   */
  def activeFencingToken(sourceId: String): Option[Long] = None

  def begin(
      sourceId: String,
      fromCursor: Option[String],
      toCursor: String,
      transactionId: String,
      plan: IncrementalPlanDescriptor): IncrementalState

  def commit(sourceId: String, transactionId: String): IncrementalState

  def abort(sourceId: String, transactionId: String): IncrementalState

  def reset(sourceId: String, cursor: Option[String]): IncrementalState

  def clear(sourceId: String): Unit

  def recordAttempt(sourceId: String, transactionId: String): IncrementalState

  def recordFailure(sourceId: String, message: String): IncrementalState

  def listSourceIds(): Seq[String]

  def close(): Unit = ()
}

/**
 * Factory for state backends. `filesystem` (a `statePath`, or
 * `incremental.state.backend=filesystem`) and `jdbc` are built into core. The distribution also
 * ships optional `redis` and `etcd` provider jars; additional backends register this provider
 * through `META-INF/services`.
 */
trait IncrementalStateBackendProvider {

  def identifier: String

  def create(options: Map[String, String], hadoopConf: Configuration): IncrementalStateBackend
}

object IncrementalStateBackendProvider {

  def providers(): Map[String, IncrementalStateBackendProvider] = {
    ServiceLoader.load(classOf[IncrementalStateBackendProvider])
      .iterator().asScala
      .map(provider => provider.identifier -> provider)
      .toMap
  }

  def forType(identifier: String): IncrementalStateBackendProvider = {
    this.providers().getOrElse(identifier,
      throw new PuddingException(
        s"unknown incremental state backend: $identifier, available: " +
          this.available()))
  }

  private def available(): String = {
    val types = this.providers().keySet
    if (types.isEmpty) "none" else types.toSeq.sorted.mkString(", ")
  }
}

/**
 * Value stored by a backend for one held lock. The token is a private random value; backends
 * must never expose it through [[IncrementalLockInfo]] or logs.
 */
case class IncrementalLockRecord(
  token: String,
  info: IncrementalLockInfo)

/**
 * Default JSON lock document used by the built-in filesystem backend and reusable by external
 * backends. Older documents without `fencingToken` or `expiresAt` remain readable.
 */
case class LockDocument(
  token: String,
  owner: String,
  host: String,
  pid: String,
  acquiredAt: String,
  fencingToken: Long = 0L,
  expiresAt: Option[String] = None,
  leaseId: Option[Long] = None) {

  def record: IncrementalLockRecord =
    IncrementalLockRecord(
      token,
      IncrementalLockInfo(owner, host, pid, acquiredAt, fencingToken, expiresAt))
}

object LockDocument {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def encode(document: LockDocument): String = Serialization.write(document)

  /**
   * Decode a lock document written by this version or by an earlier version without TTL and
   * fencing fields.
   */
  def decode(text: String, location: String): LockDocument = {
    val value = try {
      parse(text)
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"invalid incremental state lock at $location: ${error.getClass.getSimpleName}", error)
    }
    value match {
      case document: JObject =>
        LockDocument(
          token = required(document, "token", location),
          owner = required(document, "owner", location),
          host = required(document, "host", location),
          pid = required(document, "pid", location),
          acquiredAt = required(document, "acquiredAt", location),
          fencingToken = (document \ "fencingToken") match {
            case JNothing | JNull => 0L
            case JInt(item) if item.isValidLong && item.toLong >= 0L => item.toLong
            case _ =>
              throw new PuddingException(
                s"invalid incremental state lock at $location: fencingToken must be a " +
                  "non-negative integer")
          },
          expiresAt = (document \ "expiresAt") match {
            case JNothing | JNull => None
            case JString(item) if item.trim.nonEmpty => Some(item.trim)
            case _ =>
              throw new PuddingException(
                s"invalid incremental state lock at $location: expiresAt must be a timestamp")
          },
          leaseId = (document \ "leaseId") match {
            case JNothing | JNull => None
            case JInt(item) if item.isValidLong => Some(item.toLong)
            case JString(item) if item.trim.nonEmpty =>
              try Some(item.trim.toLong)
              catch {
                case _: NumberFormatException =>
                  throw new PuddingException(
                    s"invalid incremental state lock at $location: leaseId must be an integer")
              }
            case _ =>
              throw new PuddingException(
                s"invalid incremental state lock at $location: leaseId must be an integer")
          })
      case _ =>
        throw new PuddingException(
          s"invalid incremental state lock at $location: expected an object")
    }
  }

  private def required(document: JObject, field: String, location: String): String = {
    (document \ field) match {
      case JString(item) if item.trim.nonEmpty => item.trim
      case _ =>
        throw new PuddingException(
          s"invalid incremental state lock at $location: missing $field")
    }
  }
}

private final class ActiveLock(
    val sourceId: String,
    val token: String,
    val fencingToken: Long,
    initialExpiry: Option[String]) {

  @volatile var expiresAt: Option[String] = initialExpiry

  @volatile var lost: Boolean = false
}

/**
 * Shared state transitions. Backends only persist documents and implement atomic locking.
 *
 * A positive `lockTtlMillis` turns the lock into a lease. The active process renews the lease in
 * the background and before every state write; an expired lease can be taken over by a new
 * process. The lock token and fencing generation are checked on every write, and a writer whose
 * token was replaced fails with a "lock was lost" error instead of overwriting newer state.
 */
abstract class AbstractIncrementalStateBackend(
    protected val lockTtlMillis: Long = 0L)
    extends IncrementalStateBackend {

  require(lockTtlMillis >= 0L, "incremental state lock TTL can not be negative")

  protected def readState(sourceId: String): Option[IncrementalState]

  protected def persistState(state: IncrementalState, token: String): Unit

  protected def readLock(sourceId: String): Option[IncrementalLockRecord]

  protected def createLock(sourceId: String, record: IncrementalLockRecord): Boolean

  protected def renewLock(
      sourceId: String,
      token: String,
      expiresAt: Option[String]): Boolean

  protected def deleteLockIfToken(sourceId: String, token: String): Boolean

  protected def deleteLock(sourceId: String): Boolean

  protected def listBackendSourceIds(): Seq[String]

  @volatile private var activeLock: Option[ActiveLock] = None

  @volatile private var renewalExecutor: ScheduledExecutorService = _

  private val localHost: String = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case NonFatal(_) => "unknown"
    }
  }

  override def load(sourceId: String): IncrementalState = {
    this.readState(sourceId).getOrElse(this.empty(sourceId))
  }

  override def acquire(sourceId: String): IncrementalLockInfo = this.synchronized {
    val previous = this.readLock(sourceId)
    if (previous.exists(record => !this.isExpired(record.info))) {
      throw this.locked(sourceId)
    }
    val takeover = previous.nonEmpty
    val state = this.load(sourceId)
    val fencingToken = math.max(
      previous.map(_.info.fencingToken).getOrElse(0L),
      state.fencingToken) + 1L
    val info = IncrementalLockInfo(
      owner = System.getProperty("user.name", "unknown"),
      host = this.localHost,
      pid = ManagementFactory.getRuntimeMXBean.getName,
      acquiredAt = this.now(),
      fencingToken = fencingToken,
      expiresAt = this.nextExpiry())
    val token = UUID.randomUUID().toString
    val record = IncrementalLockRecord(token, info)
    if (!this.createLock(sourceId, record)) {
      throw this.locked(sourceId)
    }
    this.activeLock = Some(
      new ActiveLock(sourceId, token, fencingToken, info.expiresAt))
    IncrementalMetrics.increment(IncrementalMetrics.LockAcquired)
    if (takeover) {
      IncrementalMetrics.increment(IncrementalMetrics.LockTakeover)
    }
    IncrementalAudit.lockAcquired(sourceId, info, takeover)
    this.startRenewal()
    info
  }

  override def release(sourceId: String): Boolean = this.synchronized {
    this.activeLock match {
      case Some(active) if active.sourceId == sourceId =>
        this.stopRenewal()
        this.activeLock = None
        val released = try {
          this.deleteLockIfToken(sourceId, active.token)
        } catch {
          case NonFatal(_) => false
        }
        if (released) {
          IncrementalMetrics.increment(IncrementalMetrics.LockReleased)
          IncrementalAudit.lockReleased(sourceId)
        }
        released
      case _ => false
    }
  }

  override def forceUnlock(sourceId: String): Boolean = this.synchronized {
    this.activeLock match {
      case Some(active) if active.sourceId == sourceId =>
        this.stopRenewal()
        this.activeLock = None
      case _ =>
    }
    val previous = try this.readLock(sourceId).map(_.info) catch { case NonFatal(_) => None }
    val removed = this.deleteLock(sourceId)
    if (removed) {
      IncrementalMetrics.increment(IncrementalMetrics.LockForced)
      IncrementalAudit.lockForced(sourceId, previous)
    }
    removed
  }

  override def lockInfo(sourceId: String): Option[IncrementalLockInfo] =
    this.readLock(sourceId).map(_.info)

  override def ensureLock(sourceId: String): Unit = this.requireLock(sourceId)

  override def begin(
      sourceId: String,
      fromCursor: Option[String],
      toCursor: String,
      transactionId: String,
      plan: IncrementalPlanDescriptor): IncrementalState = this.synchronized {
    val token = this.requireLock(sourceId)
    this.requireText(transactionId, "incremental transaction id")
    this.requireText(toCursor, "incremental target cursor")
    val state = this.load(sourceId)
    if (state.pending.nonEmpty) {
      throw new PuddingException(
        s"source '$sourceId' already has an uncommitted incremental transaction: " +
          state.pending.get.transactionId)
    }
    if (state.committedCursor.nonEmpty && state.committedCursor != fromCursor) {
      throw new PuddingException(
        s"source '$sourceId' incremental state cursor " +
          s"'${state.committedCursor.get}' does not match read start " +
          s"'${fromCursor.getOrElse("")}'")
    }
    val pending = IncrementalPendingTransaction(
      transactionId = transactionId,
      fromCursor = fromCursor,
      toCursor = toCursor,
      plan = Some(plan),
      startedAt = this.now())
    val next = state.copy(
      pending = Some(pending),
      attempts = 1,
      updatedAt = this.now())
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionBegin)
    IncrementalAudit.transactionBegin(
      sourceId,
      transactionId,
      fromCursor,
      toCursor,
      this.activeFencingToken(sourceId).getOrElse(0L))
    next
  }

  override def commit(sourceId: String, transactionId: String): IncrementalState = this.synchronized {
    val token = this.requireLock(sourceId)
    val state = this.load(sourceId)
    val pending = this.requirePending(sourceId, state, transactionId)
    val committedAt = this.now()
    val next = state.copy(
      committedCursor = Some(pending.toCursor),
      pending = None,
      lastTransactionId = Some(pending.transactionId),
      lastCommittedAt = Some(committedAt),
      attempts = 0,
      lastFailureAt = None,
      lastFailureReason = None,
      updatedAt = committedAt)
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionCommit)
    IncrementalAudit.transactionCommit(
      sourceId,
      transactionId,
      next.committedCursor.getOrElse(""),
      this.activeFencingToken(sourceId).getOrElse(0L))
    next
  }

  override def abort(sourceId: String, transactionId: String): IncrementalState = this.synchronized {
    val token = this.requireLock(sourceId)
    val state = this.load(sourceId)
    this.requirePending(sourceId, state, transactionId)
    val next = state.copy(
      pending = None,
      attempts = 0,
      updatedAt = this.now())
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionAbort)
    IncrementalAudit.transactionAbort(
      sourceId,
      transactionId,
      this.activeFencingToken(sourceId).getOrElse(0L))
    next
  }

  override def reset(sourceId: String, cursor: Option[String]): IncrementalState = this.synchronized {
    val token = this.requireLock(sourceId)
    cursor.foreach(value => this.requireText(value, "incremental cursor"))
    val state = this.load(sourceId)
    val next = state.copy(
      committedCursor = cursor,
      pending = None,
      attempts = 0,
      updatedAt = this.now())
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionReset)
    next
  }

  override def clear(sourceId: String): Unit = this.synchronized {
    val token = this.requireLock(sourceId)
    val state = this.load(sourceId)
    // Keep the fencing generation even though the cursor and transaction history are gone. The
    // next acquisition must be newer than every fence a sink may already have observed.
    val next = IncrementalState(
      sourceId = sourceId,
      committedCursor = None,
      pending = None,
      updatedAt = this.now(),
      fencingToken = state.fencingToken)
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionReset)
  }

  override def recordAttempt(
      sourceId: String,
      transactionId: String): IncrementalState = this.synchronized {
    val token = this.requireLock(sourceId)
    val state = this.load(sourceId)
    this.requirePending(sourceId, state, transactionId)
    val next = state.copy(
      attempts = state.attempts + 1,
      updatedAt = this.now())
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionAttempt)
    next
  }

  override def recordFailure(sourceId: String, message: String): IncrementalState = this.synchronized {
    val token = this.requireLock(sourceId)
    val state = this.load(sourceId)
    val redacted = Option(ConfigRedactor.redactLine(message))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse("unknown failure")
    val next = state.copy(
      lastFailureAt = Some(this.now()),
      lastFailureReason = Some(redacted.take(1000)),
      updatedAt = this.now())
    this.persist(next, token)
    IncrementalMetrics.increment(IncrementalMetrics.TransactionFailure)
    IncrementalAudit.transactionFailure(sourceId, state.pending.map(_.transactionId), redacted)
    next
  }

  override def listSourceIds(): Seq[String] =
    this.listBackendSourceIds().filter(_.nonEmpty).distinct.sorted

  override def close(): Unit = this.synchronized {
    this.stopRenewal()
    this.activeLock = None
  }

  /**
   * Best-effort token check for backends that can not make the state write conditional. Strict
   * backends should instead reject the write atomically and call [[lostLock]].
   */
  protected final def requireLockToken(sourceId: String, token: String): Unit = {
    val owned = try {
      this.readLock(sourceId).exists(_.token == token)
    } catch {
      case NonFatal(_) => false
    }
    if (!owned) {
      this.activeLock.filter(_.sourceId == sourceId)
        .foreach(active => this.markLost(sourceId, active, "lock token check failed"))
      throw this.lostLock(sourceId)
    }
  }

  /** A lock without `expiresAt` never expires and must be removed with the state tool. */
  protected final def isExpired(info: IncrementalLockInfo): Boolean = {
    info.expiresAt.exists { value =>
      try {
        !Instant.parse(value).isAfter(Instant.now())
      } catch {
        case NonFatal(_) => false
      }
    }
  }

  /** Expiry timestamp for a new or renewed lease, or `None` when TTL is disabled. */
  protected final def nextExpiry(): Option[String] = {
    if (this.lockTtlMillis > 0L) {
      Some(Instant.now().plusMillis(this.lockTtlMillis).toString)
    } else {
      None
    }
  }

  /** Carry the active lock generation into the journal so it survives lock release. */
  private def withFencingToken(state: IncrementalState): IncrementalState = {
    this.activeLock match {
      case Some(active) if active.sourceId == state.sourceId =>
        state.copy(fencingToken = math.max(state.fencingToken, active.fencingToken))
      case _ => state
    }
  }

  /** Persist with the active fencing generation and account for backend write failures. */
  private def persist(state: IncrementalState, token: String): Unit = {
    try {
      this.persistState(this.withFencingToken(state), token)
    } catch {
      case NonFatal(error) =>
        IncrementalMetrics.increment(IncrementalMetrics.StateWriteFailure)
        IncrementalAudit.stateWriteFailure(state.sourceId, error.getClass.getSimpleName)
        throw error
    }
  }

  override def activeFencingToken(sourceId: String): Option[Long] = {
    this.activeLock.filter(_.sourceId == sourceId).map(_.fencingToken)
  }

  private def markLost(sourceId: String, active: ActiveLock, reason: String): Unit = {
    if (!active.lost) {
      active.lost = true
      IncrementalMetrics.increment(IncrementalMetrics.LockLost)
      IncrementalAudit.lockLost(sourceId, reason)
    }
  }

  private def requireLock(sourceId: String): String = this.synchronized {
    val active = this.activeLock match {
      case Some(value) if value.sourceId == sourceId => value
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' is not locked by this process")
    }
    if (active.lost) {
      this.activeLock = None
      throw this.lostLock(sourceId)
    }
    val owned = try {
      this.readLock(sourceId).exists(_.token == active.token)
    } catch {
      case NonFatal(_) => false
    }
    if (!owned) {
      this.markLost(sourceId, active, "lock token was replaced or removed")
      this.activeLock = None
      throw this.lostLock(sourceId)
    }
    if (this.lockTtlMillis > 0L) {
      val expiry = this.nextExpiry()
      val renewed = try {
        this.renewLock(sourceId, active.token, expiry)
      } catch {
        case NonFatal(_) => false
      }
      if (!renewed) {
        this.markLost(sourceId, active, "lease renewal failed")
        this.activeLock = None
        throw this.lostLock(sourceId)
      }
      active.expiresAt = expiry
      IncrementalMetrics.increment(IncrementalMetrics.LockRenewed)
    }
    active.token
  }

  private def startRenewal(): Unit = {
    if (this.lockTtlMillis > 0L) {
      this.stopRenewal()
      val executor = Executors.newSingleThreadScheduledExecutor(
        new java.util.concurrent.ThreadFactory {
          override def newThread(runnable: Runnable): Thread = {
            val thread = new Thread(runnable, "pudding-state-lock-renewal")
            thread.setDaemon(true)
            thread
          }
        })
      this.renewalExecutor = executor
      val period = math.max(50L, this.lockTtlMillis / 3L)
      executor.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit =
            AbstractIncrementalStateBackend.this.renewActiveLock()
        },
        period,
        period,
        TimeUnit.MILLISECONDS)
    }
  }

  private def renewActiveLock(): Unit = this.synchronized {
    this.activeLock.foreach { active =>
      if (!active.lost) {
        val expiry = this.nextExpiry()
        val renewed = try {
          this.renewLock(active.sourceId, active.token, expiry)
        } catch {
          case NonFatal(_) => false
        }
        if (renewed) {
          active.expiresAt = expiry
          IncrementalMetrics.increment(IncrementalMetrics.LockRenewed)
        } else {
          this.markLost(active.sourceId, active, "background lease renewal failed")
        }
      }
    }
  }

  private def stopRenewal(): Unit = {
    val executor = this.renewalExecutor
    this.renewalExecutor = null
    if (executor != null) {
      executor.shutdownNow()
    }
  }

  private def requirePending(
      sourceId: String,
      state: IncrementalState,
      transactionId: String): IncrementalPendingTransaction = {
    val pending = state.pending.getOrElse {
      throw new PuddingException(
        s"source '$sourceId' has no pending incremental transaction to resolve")
    }
    if (pending.transactionId != transactionId) {
      throw new PuddingException(
        s"source '$sourceId' pending transaction '${pending.transactionId}' does not match " +
          s"'$transactionId'")
    }
    pending
  }

  private def locked(sourceId: String): PuddingException = {
    val detail = try {
      this.readLock(sourceId).map(_.info) match {
        case Some(info) =>
          val expires = info.expiresAt.map(value => s", lease expires at $value").getOrElse("")
          s"${info.owner} on ${info.host} (pid ${info.pid}) since ${info.acquiredAt}" +
            s" (fencing token ${info.fencingToken}$expires)"
        case None => "an unknown owner"
      }
    } catch {
      case NonFatal(_) => "an unknown owner"
    }
    new PuddingException(
      s"incremental state for source '$sourceId' is locked by $detail at ${this.location}; " +
        "if the previous job is no longer running, unlock it with the state tool")
  }

  protected final def lostLock(sourceId: String): PuddingException = {
    new PuddingException(
      s"incremental state lock for source '$sourceId' was lost; refusing to update state")
  }

  private def empty(sourceId: String): IncrementalState =
    IncrementalState(sourceId, None, None, updatedAt = this.now())

  private def now(): String = Instant.now().toString

  private def requireText(value: String, field: String): Unit = {
    if (value == null || value.trim.isEmpty) {
      throw new PuddingException(s"$field can not be empty")
    }
  }
}

/**
 * Filesystem backend. State and lock are separate files under `statePath`.
 *
 * HDFS and local filesystems provide atomic create and overwrite rename. Object stores do not,
 * so the lock is best effort and a replacement may be a visible delete + rename; use JDBC,
 * Redis or etcd when strict fencing is required.
 */
class FileSystemStateBackend(
    statePath: String,
    hadoopConf: Configuration,
    lockTtlMillis: Long = 0L)
    extends AbstractIncrementalStateBackend(0L) {

  if (lockTtlMillis > 0L) {
    throw new PuddingException(
      "filesystem incremental state does not support lockTtlMs because it has no atomic " +
        "compare-and-swap; use the JDBC, Redis or etcd backend for lease-based takeover")
  }

  private val MaxStateBytes = 1024 * 1024

  private val root = new Path(this.requireStatePath(statePath))

  private val fileSystem: FileSystem = root.getFileSystem(hadoopConf)

  override def location: String = root.toString

  override protected def readState(sourceId: String): Option[IncrementalState] = {
    val stateFile = this.statePath(sourceId)
    if (this.fileSystem.exists(stateFile)) {
      Some(IncrementalStateCodec.decode(sourceId, this.readText(stateFile, "incremental state")))
    } else {
      val legacy = this.legacyCursorPath(sourceId)
      if (this.fileSystem.exists(legacy)) {
        val value = this.readText(legacy, s"incremental cursor for source '$sourceId'").trim
        Some(IncrementalState(
          sourceId = sourceId,
          committedCursor = if (value.isEmpty) None else Some(value),
          pending = None,
          updatedAt = this.modifiedAt(legacy)))
      } else {
        None
      }
    }
  }

  override protected def persistState(state: IncrementalState, token: String): Unit = {
    this.requireLockToken(state.sourceId, token)
    val target = this.statePath(state.sourceId)
    val temporary = new Path(
      target.getParent,
      s".${target.getName}.${UUID.randomUUID()}.tmp")
    this.fileSystem.mkdirs(root)
    val stream = this.fileSystem.create(temporary, true)
    try {
      stream.write(
        IncrementalStateCodec.encode(state).getBytes(StandardCharsets.UTF_8))
      stream.hflush()
    } finally {
      stream.close()
    }
    // Filesystems have no compare-and-set primitive. Re-check the token immediately before the
    // replacement so a stale writer has the smallest possible window to overwrite newer state.
    if (!this.replaceLockChecked(state.sourceId, token, temporary, target)) {
      this.fileSystem.delete(temporary, false)
      throw this.lostLock(state.sourceId)
    }
    // The new state is authoritative. Remove the legacy cursor so a later downgrade can not
    // silently use an older position.
    this.fileSystem.delete(this.legacyCursorPath(state.sourceId), false)
  }

  override protected def readLock(sourceId: String): Option[IncrementalLockRecord] = {
    val path = this.lockPath(sourceId)
    if (!this.fileSystem.exists(path)) {
      None
    } else {
      val text = this.readText(path, "incremental state lock")
      Some(LockDocument.decode(text, path.toString).record)
    }
  }

  override protected def createLock(
      sourceId: String,
      record: IncrementalLockRecord): Boolean = {
    val path = this.lockPath(sourceId)
    val existing = this.readLock(sourceId)
    if (existing.exists(item => !this.isExpired(item.info))) {
      false
    } else {
      this.fileSystem.mkdirs(root)
      if (existing.nonEmpty) {
        // Claim an expired lock by moving it to a unique path. Rename succeeds for one contender
        // only; the others observe the new lock or retry the exclusive create below.
        val quarantine = new Path(
          root,
          s".${path.getName}.${UUID.randomUUID()}.expired")
        try {
          if (this.fileSystem.rename(path, quarantine)) {
            this.fileSystem.delete(quarantine, false)
          }
        } catch {
          case NonFatal(_) =>
        }
        if (this.fileSystem.exists(path)) {
          return false
        }
      }
      this.writeLock(path, record, sourceId)
    }
  }

  private def writeLock(
      path: Path,
      record: IncrementalLockRecord,
      sourceId: String): Boolean = {
    val temporary = new Path(
      root,
      s".${path.getName}.${UUID.randomUUID()}.tmp")
    try {
      val stream = this.fileSystem.create(temporary, true)
      try {
        stream.write(
          LockDocument.encode(this.lockDocument(record)).getBytes(StandardCharsets.UTF_8))
        stream.hflush()
      } finally {
        stream.close()
      }
      // Rename without overwrite is the atomic claim. Never delete the target if the rename
      // fails: it may belong to another live writer.
      val created = this.fileSystem.rename(temporary, path)
      if (!created) {
        this.fileSystem.delete(temporary, false)
      }
      created
    } catch {
      case _: FileAlreadyExistsException =>
        try this.fileSystem.delete(temporary, false)
        catch { case NonFatal(_) => }
        false
      case error: IOException =>
        try this.fileSystem.delete(temporary, false)
        catch { case NonFatal(_) => }
        throw new PuddingException(
          s"failed to acquire incremental state lock for source '$sourceId': " +
            error.getClass.getSimpleName, error)
    }
  }

  override protected def renewLock(
      sourceId: String,
      token: String,
      expiresAt: Option[String]): Boolean = false

  override protected def deleteLockIfToken(sourceId: String, token: String): Boolean = {
    val path = this.lockPath(sourceId)
    if (this.readLock(sourceId).exists(_.token == token)) {
      this.fileSystem.delete(path, false)
    } else {
      false
    }
  }

  override protected def deleteLock(sourceId: String): Boolean = {
    val path = this.lockPath(sourceId)
    this.fileSystem.exists(path) && this.fileSystem.delete(path, false)
  }

  private def lockDocument(record: IncrementalLockRecord): LockDocument = {
    LockDocument(
      token = record.token,
      owner = record.info.owner,
      host = record.info.host,
      pid = record.info.pid,
      acquiredAt = record.info.acquiredAt,
      fencingToken = record.info.fencingToken,
      expiresAt = record.info.expiresAt)
  }

  override protected def listBackendSourceIds(): Seq[String] = {
    if (!this.fileSystem.exists(root)) {
      Seq.empty
    } else {
      this.fileSystem.listStatus(root).toSeq.flatMap { status =>
        val name = status.getPath.getName
        if (name.endsWith(".state")) {
          val base = name.stripSuffix(".state")
          try {
            val text = this.readText(status.getPath, "incremental state")
            val sourceId = IncrementalStateCodec.sourceIdOf(text).getOrElse(base)
            val state = IncrementalStateCodec.decode(sourceId, text)
            if (state.isEmpty) None else Some(sourceId)
          } catch {
            case NonFatal(_) => Some(base)
          }
        } else if (name.endsWith(".cursor")) {
          Some(name.stripSuffix(".cursor"))
        } else {
          None
        }
      }
    }
  }

  private def requireStatePath(value: String): String = {
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("incremental statePath can not be empty")
    }
  }

  private def replaceLockChecked(
      sourceId: String,
      token: String,
      temporary: Path,
      target: Path): Boolean = {
    if (!this.readLock(sourceId).exists(_.token == token)) {
      false
    } else if (!this.replace(temporary, target)) {
      throw new PuddingException(
        s"failed to commit incremental state for source '$sourceId' at $target")
    } else {
      true
    }
  }

  /**
   * Replace a state file. FileContext rename with OVERWRITE is atomic where supported (HDFS,
   * local). Object stores fall back to delete + rename, which is not crash-atomic.
   */
  private def replace(temporary: Path, target: Path): Boolean = {
    val objectStore = Set(
      "s3a", "s3", "oss", "abfs", "abfss", "wasb", "wasbs", "gs", "cosn", "cos")
      .contains(Option(this.fileSystem.getScheme)
        .map(_.toLowerCase(Locale.ROOT)).getOrElse(""))
    try {
      val context = FileContext.getFileContext(target.toUri, hadoopConf)
      context.rename(temporary, target, Options.Rename.OVERWRITE)
      true
    } catch {
      case _: UnsupportedOperationException if objectStore =>
        this.replaceByDeleteAndRename(temporary, target)
      case _: IOException if objectStore =>
        this.replaceByDeleteAndRename(temporary, target)
      case error: Exception =>
        // A transient HDFS/local failure must not delete the previous committed state.
        throw new PuddingException(
          s"failed to replace incremental state at $target: " +
            error.getClass.getSimpleName, error)
    }
  }

  private def replaceByDeleteAndRename(temporary: Path, target: Path): Boolean = {
    if (this.fileSystem.exists(target) && !this.fileSystem.delete(target, false)) {
      false
    } else {
      this.fileSystem.rename(temporary, target)
    }
  }

  /**
   * Read a state or lock document. Local and HDFS files carry sidecar/block checksums; a
   * concurrent overwrite rename can briefly pair a new data file with an old checksum file.
   * Retry such a transient mismatch before reporting corruption.
   */
  private def readText(path: Path, field: String): String = {
    var attempt = 0
    var result: Option[String] = None
    while (result.isEmpty) {
      try {
        result = Some(this.readTextOnce(path, field))
      } catch {
        case _: ChecksumException if attempt < 3 =>
          attempt += 1
          try Thread.sleep(20L * attempt)
          catch { case _: InterruptedException => Thread.currentThread().interrupt() }
        case error: Exception =>
          throw new PuddingException(
            s"failed to read $field: ${error.getClass.getSimpleName}", error)
      }
    }
    result.get
  }

  private def readTextOnce(path: Path, field: String): String = {
    val length = this.fileSystem.getFileStatus(path).getLen
    if (length > MaxStateBytes) {
      throw new PuddingException(s"$field is too large: $length bytes")
    }
    val bytes = new Array[Byte](length.toInt)
    val stream = this.fileSystem.open(path)
    try {
      stream.readFully(bytes)
    } finally {
      stream.close()
    }
    new String(bytes, StandardCharsets.UTF_8)
  }

  private def modifiedAt(path: Path): String = {
    Instant.ofEpochMilli(this.fileSystem.getFileStatus(path).getModificationTime).toString
  }

  private def statePath(sourceId: String): Path =
    new Path(root, s"${this.safeName(sourceId)}.state")

  private def legacyCursorPath(sourceId: String): Path =
    new Path(root, s"${this.safePart(sourceId)}.cursor")

  private def lockPath(sourceId: String): Path =
    new Path(root, s"${this.safeName(sourceId)}.lock")

  /**
   * Sanitizing alone can map different source ids to the same file (for example `a/b` and
   * `a_b`), which would make their journals and locks collide. Append a hash of the original id
   * to keep names readable while making them unique.
   */
  private def safeName(sourceId: String): String = {
    val value = Option(sourceId).getOrElse("")
    s"${this.safePart(value)}-${IncrementalStateCodec.sha256(value).take(12)}"
  }

  private def safePart(sourceId: String): String = {
    val safe = Option(sourceId).getOrElse("")
      .replaceAll("[^A-Za-z0-9._-]", "_")
    if (safe.isEmpty) {
      throw new PuddingException("source id can not be empty for incremental state")
    }
    safe
  }
}

/**
 * Built-in provider for the filesystem state backend. The standalone `statePath` form remains
 * supported and is equivalent to `incremental.state.backend=filesystem` with a `statePath`
 * option.
 */
class FileSystemStateBackendProvider extends IncrementalStateBackendProvider {

  override def identifier: String = "filesystem"

  override def create(
      options: Map[String, String],
      hadoopConf: Configuration): IncrementalStateBackend = {
    val statePath = options.get("statePath")
      .map(_.trim).filter(_.nonEmpty).getOrElse {
        throw new PuddingException(
          "filesystem incremental state backend requires option 'statePath'")
      }
    new FileSystemStateBackend(
      statePath,
      hadoopConf,
      this.lockTtlMillis(options))
  }

  private def lockTtlMillis(options: Map[String, String]): Long = {
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
 * Facade used by the engine and the state tool. Selection is explicit so a job can move from a
 * filesystem to a JDBC backend without changing the transaction protocol.
 */
class IncrementalStateStore(private val backend: IncrementalStateBackend) {

  def this(statePath: String, hadoopConf: Configuration) =
    this(new FileSystemStateBackend(statePath, hadoopConf))

  def location: String = this.backend.location

  def load(sourceId: String): IncrementalState = this.backend.load(sourceId)

  def acquire(sourceId: String): IncrementalLockInfo = this.backend.acquire(sourceId)

  def release(sourceId: String): Boolean = this.backend.release(sourceId)

  def forceUnlock(sourceId: String): Boolean = this.backend.forceUnlock(sourceId)

  def lockInfo(sourceId: String): Option[IncrementalLockInfo] =
    this.backend.lockInfo(sourceId)

  def ensureLock(sourceId: String): Unit = this.backend.ensureLock(sourceId)

  def activeFencingToken(sourceId: String): Option[Long] =
    this.backend.activeFencingToken(sourceId)

  def begin(
      sourceId: String,
      fromCursor: Option[String],
      toCursor: String,
      transactionId: String,
      plan: IncrementalPlanDescriptor): IncrementalState =
    this.backend.begin(sourceId, fromCursor, toCursor, transactionId, plan)

  def commit(sourceId: String, transactionId: String): IncrementalState =
    this.backend.commit(sourceId, transactionId)

  def abort(sourceId: String, transactionId: String): IncrementalState =
    this.backend.abort(sourceId, transactionId)

  def reset(sourceId: String, cursor: Option[String]): IncrementalState =
    this.backend.reset(sourceId, cursor)

  def clear(sourceId: String): Unit = this.backend.clear(sourceId)

  def recordAttempt(sourceId: String, transactionId: String): IncrementalState =
    this.backend.recordAttempt(sourceId, transactionId)

  def recordFailure(sourceId: String, message: String): IncrementalState =
    this.backend.recordFailure(sourceId, message)

  def listSourceIds(): Seq[String] = this.backend.listSourceIds()

  def close(): Unit = this.backend.close()
}

object IncrementalStateStore {

  def open(statePath: String, hadoopConf: Configuration): IncrementalStateStore =
    new IncrementalStateStore(new FileSystemStateBackend(statePath, hadoopConf))

  /**
   * Open the backend declared by an incremental source configuration.
   */
  def open(spec: IncrementalSpec, hadoopConf: Configuration): IncrementalStateStore = {
    spec.state match {
      case Some(state) =>
        val resolvedOptions = SecretResolver.resolveMap(state.options)
        if (state.backend == "filesystem") {
          val path = resolvedOptions.getOrElse("statePath", "")
          val allow = spec.allowBestEffortLock ||
            resolvedOptions.get("allowBestEffortLock")
              .exists(_.trim.equalsIgnoreCase("true"))
          this.validateStatePath(path, allow, hadoopConf)
        }
        this.openBackend(state.backend, resolvedOptions, hadoopConf)
      case None =>
        val path = spec.statePath.getOrElse {
          throw new PuddingException(
            "incremental statePath or incremental.state backend is required")
        }
        this.validateStatePath(path, spec.allowBestEffortLock, hadoopConf)
        this.open(path, hadoopConf)
    }
  }

  private val ObjectStoreSchemes =
    Set("s3a", "s3", "oss", "abfs", "abfss", "wasb", "wasbs", "gs", "cosn", "cos")

  /**
   * Object stores do not provide an exclusive lock primitive. Keep the explicit statePath form
   * usable, but require an opt-in so a production job does not accidentally rely on a best-effort
   * single-writer lock.
   */
  private def validateStatePath(
      path: String,
      allowBestEffortLock: Boolean,
      hadoopConf: Configuration): Unit = {
    val explicit = Option(path).filter(_.nonEmpty)
      .map(value => new Path(value).toUri.getScheme)
      .flatMap(Option(_))
    // A scheme-less path inherits fs.defaultFS; an object-store default FS must be treated the
    // same as an explicit s3a/oss/... URI.
    val defaultScheme = Option(hadoopConf.get("fs.defaultFS"))
      .filter(_.trim.nonEmpty)
      .flatMap(value => Option(new Path(value).toUri.getScheme))
    val scheme = explicit.orElse(defaultScheme).map(_.toLowerCase(Locale.ROOT))
    if (scheme.exists(ObjectStoreSchemes.contains) && !allowBestEffortLock) {
      throw new PuddingException(
        s"filesystem incremental state on object store scheme '${scheme.get}' has " +
          "only a best-effort lock; use JDBC, Redis or etcd, or set " +
          "incremental.allowBestEffortLock=true to accept the risk")
    }
  }

  /**
   * Open a state target used by the CLI. A plain path selects the filesystem backend; a target
   * prefixed with `@` is a properties file with `backend`, `statePath` or JDBC options.
   */
  def openTarget(target: String, hadoopConf: Configuration): IncrementalStateStore = {
    val (backend, options) = this.targetOptions(target)
    if (backend == "filesystem") {
      val allow = options.get("allowBestEffortLock")
        .exists(_.trim.equalsIgnoreCase("true"))
      this.validateStatePath(options.getOrElse("statePath", ""), allow, hadoopConf)
    }
    this.openBackend(backend, options, hadoopConf)
  }

  /**
   * Read a state target without opening the backend. A plain path selects the filesystem
   * backend; `@path` reads a properties file with `backend` and backend options. Secret
   * references are resolved here.
   */
  def targetOptions(target: String): (String, Map[String, String]) = {
    if (target.startsWith("@")) {
      val properties = new Properties()
      val input = new FileInputStream(target.substring(1))
      try {
        properties.load(input)
      } catch {
        case error: Exception =>
          throw new PuddingException(
            s"failed to read state configuration: ${error.getClass.getSimpleName}", error)
      } finally {
        input.close()
      }
      val values = properties.stringPropertyNames().asScala
        .map(name => name -> properties.getProperty(name)).toMap
      val backend = values.getOrElse("backend", "filesystem").trim.toLowerCase(Locale.ROOT)
      backend -> SecretResolver.resolveMap(values - "backend")
    } else {
      "filesystem" -> Map("statePath" -> target)
    }
  }

  def backendName(spec: IncrementalSpec): String =
    spec.state.map(_.backend).getOrElse("filesystem")

  private def openBackend(
      backend: String,
      options: Map[String, String],
      hadoopConf: Configuration): IncrementalStateStore = {
    val provider = IncrementalStateBackendProvider.forType(backend)
    new IncrementalStateStore(provider.create(options, hadoopConf))
  }
}
