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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

import org.pudding.core.PuddingException
import org.pudding.core.definition.IncrementalSemantics
import org.pudding.core.util.ConfigRedactor

/**
 * Non-sensitive identity of the source, sink and semantics that produced a pending transaction.
 * Configuration is represented by redacted hashes, never by raw values.
 */
case class IncrementalPlanDescriptor(
  sourceType: String,
  sourceId: String,
  sinkType: String,
  sinkId: String,
  semantics: String,
  keyFields: Seq[String],
  opColumn: String,
  orderFields: Seq[String],
  sourceConfigHash: String,
  sinkConfigHash: String,
  stateBackend: String,
  stateVersion: Int = IncrementalStateCodec.SchemaVersion,
  contractHash: String = "") {

  def fingerprint: String = {
    val fields = Seq(
      this.sourceType,
      this.sourceId,
      this.sinkType,
      this.sinkId,
      this.semantics,
      this.keyFields.mkString(","),
      this.opColumn,
      this.orderFields.mkString(","),
      this.sourceConfigHash,
      this.sinkConfigHash,
      this.stateBackend,
      this.stateVersion.toString)
    // Older state documents have no contractHash. Omitting it from the canonical string keeps
    // their fingerprints readable while new writes include resolved resources and transforms.
    val canonical = if (this.contractHash.isEmpty) fields else fields :+ this.contractHash
    IncrementalStateCodec.sha256(canonical.mkString("\u0001"))
  }
}

object IncrementalPlanDescriptor {

  def create(
      sourceType: String,
      sourceId: String,
      sinkType: String,
      sinkId: String,
      semantics: IncrementalSemantics,
      keyFields: Seq[String],
      opColumn: String,
      sourceConfig: Option[Map[String, Any]],
      sinkConfig: Option[Map[String, Any]],
      stateBackend: String): IncrementalPlanDescriptor = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    IncrementalPlanDescriptor(
      sourceType = sourceType,
      sourceId = sourceId,
      sinkType = sinkType,
      sinkId = sinkId,
      semantics = semantics.name,
      keyFields = keyFields,
      opColumn = opColumn,
      orderFields = Seq.empty,
      sourceConfigHash = IncrementalStateCodec.sha256(
        ConfigRedactor.canonicalRedactedJson(sourceConfig.getOrElse(Map.empty))),
      sinkConfigHash = IncrementalStateCodec.sha256(
        ConfigRedactor.canonicalRedactedJson(sinkConfig.getOrElse(Map.empty))),
      stateBackend = stateBackend)
  }

  /**
   * Add the resolved-resource and transform contract. Kept separate so the original create
   * signature remains usable by older callers and state-compatible tests.
   */
  def withContract(
      descriptor: IncrementalPlanDescriptor,
      contract: Option[Map[String, Any]]): IncrementalPlanDescriptor = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    contract match {
      case Some(value) => descriptor.copy(
        contractHash = IncrementalStateCodec.sha256(
          ConfigRedactor.canonicalRedactedJson(value)))
      case None => descriptor
    }
  }
}

/**
 * The sink write that may have completed but was not committed to the cursor yet.
 */
case class IncrementalPendingTransaction(
  transactionId: String,
  fromCursor: Option[String],
  toCursor: String,
  plan: Option[IncrementalPlanDescriptor],
  startedAt: String)

/**
 * Durable state for one incremental source. Only cursors, plan identity and transaction
 * metadata are stored; credentials and source configuration are never written here.
 */
case class IncrementalState(
  sourceId: String,
  committedCursor: Option[String],
  pending: Option[IncrementalPendingTransaction],
  lastTransactionId: Option[String] = None,
  lastCommittedAt: Option[String] = None,
  attempts: Int = 0,
  lastFailureAt: Option[String] = None,
  lastFailureReason: Option[String] = None,
  updatedAt: String = "",
  fencingToken: Long = 0L) {

  def isEmpty: Boolean =
    this.committedCursor.isEmpty &&
      this.pending.isEmpty &&
      this.attempts == 0 &&
      this.lastFailureAt.isEmpty &&
      this.lastFailureReason.isEmpty
}

/**
 * Lock owner information without the private lock token.
 *
 * `fencingToken` increases on every lock acquisition and is carried into state writes; a writer
 * whose token was replaced must fail instead of overwriting newer state. `expiresAt` is present
 * only when a lock TTL is configured.
 */
case class IncrementalLockInfo(
  owner: String,
  host: String,
  pid: String,
  acquiredAt: String,
  fencingToken: Long = 0L,
  expiresAt: Option[String] = None)

/**
 * JSON codec shared by the filesystem and JDBC state backends.
 */
object IncrementalStateCodec {

  val SchemaVersion = 1

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def encode(state: IncrementalState): String = {
    val document = StateDocument(
      version = SchemaVersion,
      sourceId = state.sourceId,
      committedCursor = state.committedCursor,
      pending = state.pending,
      lastTransactionId = state.lastTransactionId,
      lastCommittedAt = state.lastCommittedAt,
      attempts = state.attempts,
      lastFailureAt = state.lastFailureAt,
      lastFailureReason = state.lastFailureReason,
      updatedAt = state.updatedAt,
      fencingToken = state.fencingToken)
    Serialization.write(document)
  }

  def decode(sourceId: String, text: String): IncrementalState = {
    val value = try {
      parse(text)
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"invalid incremental state for source '$sourceId': " +
            error.getClass.getSimpleName, error)
    }
    val document = value match {
      case _: JObject => value
      case _ =>
        throw new PuddingException(s"incremental state for source '$sourceId' must be an object")
    }
    val version = (document \ "version") match {
      case JInt(item) => item.toInt
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' has no numeric version")
    }
    if (version != SchemaVersion) {
      throw new PuddingException(
        s"unsupported incremental state version $version for source '$sourceId'")
    }
    val storedSource = this.requiredString(document, "sourceId", sourceId)
    if (storedSource != sourceId) {
      throw new PuddingException(
        s"incremental state file belongs to source '$storedSource', not '$sourceId'")
    }
    val pending = (document \ "pending") match {
      case JNothing | JNull => None
      case value: JObject => Some(this.pending(value, sourceId))
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' has an invalid pending transaction")
    }
    val attempts = (document \ "attempts") match {
      case JNothing | JNull => 0
      case JInt(value) if value.isValidInt && value.toInt >= 0 => value.toInt
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' has an invalid attempts value")
    }
    IncrementalState(
      sourceId = sourceId,
      committedCursor = this.optionalString(document, "committedCursor", sourceId),
      pending = pending,
      lastTransactionId = this.optionalString(document, "lastTransactionId", sourceId),
      lastCommittedAt = this.optionalString(document, "lastCommittedAt", sourceId),
      attempts = attempts,
      lastFailureAt = this.optionalString(document, "lastFailureAt", sourceId),
      lastFailureReason = this.optionalString(document, "lastFailureReason", sourceId),
      updatedAt = this.optionalString(document, "updatedAt", sourceId).getOrElse(""),
      fencingToken = (document \ "fencingToken") match {
        case JNothing | JNull => 0L
        case JInt(value) if value.isValidLong && value.toLong >= 0L => value.toLong
        case _ =>
          throw new PuddingException(
            s"incremental state for source '$sourceId' has an invalid fencingToken value")
      })
  }

  /**
   * Read only the source id from a state document, used to list sanitized filesystem names.
   */
  def sourceIdOf(text: String): Option[String] = {
    try {
      (parse(text) \ "sourceId") match {
        case JString(value) if value.trim.nonEmpty => Some(value.trim)
        case _ => None
      }
    } catch {
      case _: Exception => None
    }
  }

  def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def pending(
      value: JObject,
      sourceId: String): IncrementalPendingTransaction = {
    val plan = (value \ "plan") match {
      case JNothing | JNull => None
      case plan: JObject => Some(this.plan(plan, sourceId))
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' has an invalid plan fingerprint")
    }
    IncrementalPendingTransaction(
      transactionId = this.requiredString(value, "transactionId", sourceId),
      fromCursor = this.optionalString(value, "fromCursor", sourceId),
      toCursor = this.requiredString(value, "toCursor", sourceId),
      plan = plan,
      startedAt = this.optionalString(value, "startedAt", sourceId).getOrElse(""))
  }

  private def plan(value: JObject, sourceId: String): IncrementalPlanDescriptor = {
    IncrementalPlanDescriptor(
      sourceType = this.requiredString(value, "sourceType", sourceId),
      sourceId = this.requiredString(value, "sourceId", sourceId),
      sinkType = this.requiredString(value, "sinkType", sourceId),
      sinkId = this.requiredString(value, "sinkId", sourceId),
      semantics = this.requiredString(value, "semantics", sourceId),
      keyFields = this.stringArray(value, "keyFields", sourceId),
      opColumn = this.requiredString(value, "opColumn", sourceId),
      orderFields = this.stringArray(value, "orderFields", sourceId),
      sourceConfigHash = this.requiredString(value, "sourceConfigHash", sourceId),
      sinkConfigHash = this.requiredString(value, "sinkConfigHash", sourceId),
      stateBackend = this.requiredString(value, "stateBackend", sourceId),
      stateVersion = (value \ "stateVersion") match {
        case JInt(item) => item.toInt
        case _ => SchemaVersion
      },
      contractHash = this.optionalString(value, "contractHash", sourceId).getOrElse(""))
  }

  private def requiredString(value: JValue, key: String, sourceId: String): String = {
    (value \ key) match {
      case JString(text) if text.trim.nonEmpty => text.trim
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' has no valid $key")
    }
  }

  private def optionalString(
      value: JValue,
      key: String,
      sourceId: String): Option[String] = (value \ key) match {
    case JNothing | JNull => None
    case JString(text) if text.trim.nonEmpty => Some(text.trim)
    case JString(_) => None
    case _ =>
      throw new PuddingException(
        s"incremental state for source '$sourceId' has an invalid $key")
  }

  private def stringArray(value: JValue, key: String, sourceId: String): Seq[String] = {
    (value \ key) match {
      case JNothing | JNull => Seq.empty
      case JArray(values) =>
        values.map {
          case JString(text) if text.trim.nonEmpty => text.trim
          case _ =>
            throw new PuddingException(
              s"incremental state for source '$sourceId' has an invalid $key")
        }
      case _ =>
        throw new PuddingException(
          s"incremental state for source '$sourceId' has an invalid $key")
    }
  }

  private case class StateDocument(
    version: Int,
    sourceId: String,
    committedCursor: Option[String],
    pending: Option[IncrementalPendingTransaction],
    lastTransactionId: Option[String],
    lastCommittedAt: Option[String],
    attempts: Int,
    lastFailureAt: Option[String],
    lastFailureReason: Option[String],
    updatedAt: String,
    fencingToken: Long)
}

/**
 * Stable transaction identifier for one source cursor range. It contains no credentials and can
 * be used in logs and in a sink's own commit metadata.
 */
object IncrementalTransaction {

  def id(
      sourceId: String,
      fromCursor: Option[String],
      toCursor: String,
      semantics: IncrementalSemantics,
      keyFields: Seq[String],
      opColumn: String,
      orderFields: Seq[String],
      plan: Option[IncrementalPlanDescriptor] = None): String = {
    val value = Seq(
      sourceId,
      fromCursor.getOrElse(""),
      toCursor,
      semantics.name,
      keyFields.mkString(","),
      opColumn,
      orderFields.mkString(","),
      plan.map(_.fingerprint).getOrElse("")).mkString("\u0001")
    IncrementalStateCodec.sha256(value)
  }
}
