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

package org.pudding.core.definition

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.PuddingException

/**
 * The semantic contract between an incremental source and a sink.
 *
 * `Append` means every row is new. `Upsert` means the rows contain the latest state for their
 * keys. `Cdc` means each row carries an operation in `opColumn`.
 */
sealed trait IncrementalSemantics {
  def name: String
}

object IncrementalSemantics {

  case object Append extends IncrementalSemantics {
    override val name: String = "append"
  }

  case object Upsert extends IncrementalSemantics {
    override val name: String = "upsert"
  }

  case object Cdc extends IncrementalSemantics {
    override val name: String = "cdc"
  }

  val values: Set[IncrementalSemantics] = Set(Append, Upsert, Cdc)

  def parse(value: String, field: String): IncrementalSemantics = {
    val normalized = Option(value).map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"$field can not be empty!")
    }
    this.values.find(_.name == normalized).getOrElse {
      throw new PuddingException(
        s"unsupported $field: $value, supported: ${this.values.map(_.name).toSeq.sorted.mkString(", ")}")
    }
  }

}

/**
 * What to do when a previous run left an uncommitted sink transaction.
 *
 * `auto` retries only when the selected semantics are idempotent for the configured sink,
 * `retry` trusts the operator that a repeated write is safe, and `fail` requires an explicit
 * `pudding state resolve` decision.
 */
sealed trait IncrementalRecoveryPolicy {
  def name: String
}

object IncrementalRecoveryPolicy {

  case object Auto extends IncrementalRecoveryPolicy {
    override val name: String = "auto"
  }

  case object Retry extends IncrementalRecoveryPolicy {
    override val name: String = "retry"
  }

  case object Fail extends IncrementalRecoveryPolicy {
    override val name: String = "fail"
  }

  val values: Set[IncrementalRecoveryPolicy] = Set(Auto, Retry, Fail)

  def parse(value: String, field: String): IncrementalRecoveryPolicy = {
    val normalized = Option(value).map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .getOrElse(throw new PuddingException(s"$field can not be empty!"))
    this.values.find(_.name == normalized).getOrElse {
      throw new PuddingException(
        s"unsupported $field: $value, supported: " +
          this.values.map(_.name).toSeq.sorted.mkString(", "))
    }
  }
}

/**
 * Pluggable incremental state backend configuration. Options are resolved as secret references
 * on the driver and passed to the selected provider.
 */
case class IncrementalStateConf(
  backend: String,
  options: Map[String, String] = Map.empty)

/**
 * Parsed source-side incremental configuration.
 */
case class IncrementalSpec(
  semantics: IncrementalSemantics,
  keyFields: Seq[String] = Seq.empty,
  opColumn: String = "__op",
  start: Option[String] = None,
  end: Option[String] = None,
  initial: String = "none",
  statePath: Option[String] = None,
  state: Option[IncrementalStateConf] = None,
  recovery: IncrementalRecoveryPolicy = IncrementalRecoveryPolicy.Auto,
  allowBestEffortLock: Boolean = false)

/**
 * Sent to an incremental source for one run. `end` is resolved by the source when absent.
 */
case class IncrementalRequest(
  semantics: IncrementalSemantics,
  keyFields: Seq[String],
  opColumn: String,
  start: Option[String],
  end: Option[String],
  initial: String)

/**
 * One bounded incremental read. The source does not commit `toCursor`; the caller must persist
 * it only after all sinks have completed successfully.
 */
case class IncrementalRead(
  dataFrame: DataFrame,
  semantics: IncrementalSemantics,
  keyFields: Seq[String],
  opColumn: String,
  fromCursor: Option[String],
  toCursor: String,
  cursorType: String,
  orderFields: Seq[String] = Seq.empty)

/**
 * Metadata passed to a sink when it applies an incremental result.
 *
 * `orderFields` is optional source-provided change ordering. A CDC sink uses it to keep only the
 * last operation per key before applying the batch; the fields are metadata and are removed by
 * the sink after deduplication. `transactionId` identifies the durable state transaction and is
 * used for logs and for matching a retry with the pending journal entry.
 *
 * `sourceId` and `fencingToken` come from the incremental lock. A fencing-aware sink records
 * them in its commit metadata and rejects a writer whose token is older than the newest token it
 * has observed for the source. The token is a monotonically increasing generation, not a secret.
 */
case class IncrementalWrite(
  semantics: IncrementalSemantics,
  keyFields: Seq[String],
  opColumn: String,
  fromCursor: Option[String],
  toCursor: String,
  cursorType: String,
  orderFields: Seq[String] = Seq.empty,
  transactionId: String = "",
  sourceId: String = "",
  fencingToken: Long = 0L)

/**
 * Optional source capability. Existing DataSource implementations are unaffected.
 */
trait IncrementalSource extends DataSource {

  def supportedIncrementalSemantics: Set[IncrementalSemantics]

  def readIncremental(
      sparkSession: SparkSession,
      context: SourceContext,
      request: IncrementalRequest): IncrementalRead
}

/**
 * Strict parser for the nested `incremental` source configuration.
 */
object IncrementalConfig {

  val DefaultOpColumn = "__op"

  private val IncrementalKey = "incremental"

  private val InitialModes = Set("none", "latest", "full")

  def parse(config: Option[Map[String, Any]], sourceId: String): Option[IncrementalSpec] = {
    val root = config.getOrElse(Map.empty).get(IncrementalKey).filter(_ != null).getOrElse {
      return None
    }
    val values = asMap(root, s"source '$sourceId' incremental")
    val enabled = values.get("enabled").filter(_ != null).forall(booleanValue(_, "incremental.enabled"))
    if (!enabled) {
      return None
    }

    val mode = optionalString(values, "mode", s"source '$sourceId' incremental.mode")
      .map(_.toLowerCase(Locale.ROOT))
      .getOrElse("batch")
    if (mode != "batch") {
      throw new PuddingException(
        s"source '$sourceId' incremental.mode must be 'batch', but got: $mode")
    }

    val semantics = IncrementalSemantics.parse(
      requiredString(values, "semantics", s"source '$sourceId' incremental.semantics"),
      s"source '$sourceId' incremental.semantics")
    val keyFields = stringSeq(values.get("keyFields"), s"source '$sourceId' incremental.keyFields")
    val opColumn = optionalString(values, "opColumn", s"source '$sourceId' incremental.opColumn")
      .getOrElse(DefaultOpColumn)
    val initial = optionalString(values, "initial", s"source '$sourceId' incremental.initial")
      .map(_.toLowerCase(Locale.ROOT))
      .getOrElse("none")
    if (!InitialModes.contains(initial)) {
      throw new PuddingException(
        s"source '$sourceId' incremental.initial must be one of " +
          s"${InitialModes.toSeq.sorted.mkString(", ")}, but got: $initial")
    }
    val recovery = IncrementalRecoveryPolicy.parse(
      optionalString(values, "recovery", s"source '$sourceId' incremental.recovery")
        .getOrElse(IncrementalRecoveryPolicy.Auto.name),
      s"source '$sourceId' incremental.recovery")
    val state = values.get("state").filter(_ != null).map(value =>
      this.stateConfig(value, sourceId))
    val allowBestEffortLock = values.get("allowBestEffortLock").filter(_ != null)
      .map(value => this.booleanValue(
        value, s"source '$sourceId' incremental.allowBestEffortLock"))
      .getOrElse(false)

    val start = optionalString(values, "start", s"source '$sourceId' incremental.start")
    start.foreach { value =>
      if (value.equalsIgnoreCase("latest")) {
        throw new PuddingException(
          s"source '$sourceId' incremental.start must be a cursor; " +
            "use initial=latest to start at the latest cursor")
      }
    }
    val end = optionalString(values, "end", s"source '$sourceId' incremental.end")
      .filterNot(_.equalsIgnoreCase("latest"))
    if (start.nonEmpty && initial != "none") {
      throw new PuddingException(
        s"source '$sourceId' incremental.start can not be combined with initial=$initial")
    }
    if (keyFields.distinct.size != keyFields.size) {
      throw new PuddingException(
        s"source '$sourceId' incremental.keyFields contains duplicates: " +
          keyFields.mkString(", "))
    }
    if (semantics != IncrementalSemantics.Append && keyFields.isEmpty) {
      throw new PuddingException(
        s"source '$sourceId' incremental.semantics=${semantics.name} requires keyFields")
    }
    if (semantics == IncrementalSemantics.Cdc && opColumn.trim.isEmpty) {
      throw new PuddingException(
        s"source '$sourceId' incremental.semantics=cdc requires opColumn")
    }
    if (semantics == IncrementalSemantics.Cdc && keyFields.contains(opColumn)) {
      throw new PuddingException(
        s"source '$sourceId' incremental.keyFields must not contain opColumn '$opColumn'")
    }

    Some(IncrementalSpec(
      semantics = semantics,
      keyFields = keyFields,
      opColumn = opColumn,
      start = start,
      end = end,
      initial = initial,
      statePath = optionalString(values, "statePath", s"source '$sourceId' incremental.statePath"),
      state = state,
      recovery = recovery,
      allowBestEffortLock = allowBestEffortLock))
  }

  private def stateConfig(value: Any, sourceId: String): IncrementalStateConf = {
    val values = this.asMap(value, s"source '$sourceId' incremental.state")
    val backend = this.optionalString(
      values, "backend", s"source '$sourceId' incremental.state.backend").getOrElse {
      throw new PuddingException(
        s"source '$sourceId' incremental.state.backend can not be empty")
    }.toLowerCase(Locale.ROOT)
    val options = (values - "backend").map {
      case (key, item: String) => key -> item.trim
      case (key, item: Boolean) => key -> item.toString
      case (key, item: Number) => key -> item.toString
      case (key, _) =>
        throw new PuddingException(
          s"source '$sourceId' incremental.state.$key must be a string, boolean or number")
    }
    IncrementalStateConf(backend, options.filter { case (_, item) => item.nonEmpty })
  }

  private def asMap(value: Any, field: String): Map[String, Any] = value match {
    case map: Map[_, _] =>
      this.checkedMap(map, field)
    case map: java.util.Map[_, _] =>
      this.checkedMap(map.asScala, field)
    case _ => throw new PuddingException(s"$field must be a map!")
  }

  private def checkedMap(
      map: scala.collection.Map[_, _],
      field: String): Map[String, Any] = {
    map.map {
      case (key, item) =>
        if (key == null || item == null) {
          throw new PuddingException(s"$field can not contain a null key or value")
        }
        key.toString -> item
    }.toMap
  }

  private def optionalString(values: Map[String, Any], key: String, field: String): Option[String] = {
    values.get(key).filter(_ != null).map {
      case value: String => value.trim
      case value => throw new PuddingException(s"$field must be a string, but got: $value")
    }.filter(_.nonEmpty)
  }

  private def requiredString(values: Map[String, Any], key: String, field: String): String = {
    this.optionalString(values, key, field).getOrElse {
      throw new PuddingException(s"can not find param: $field")
    }
  }

  private def booleanValue(value: Any, field: String): Boolean = value match {
    case item: Boolean => item
    case item: java.lang.Boolean => item.booleanValue()
    case item: String =>
      item.trim.toLowerCase(Locale.ROOT) match {
        case "true" => true
        case "false" => false
        case _ => throw new PuddingException(s"$field must be true or false, but got: $item")
      }
    case other => throw new PuddingException(s"$field must be a boolean, but got: $other")
  }

  private def stringSeq(value: Option[Any], field: String): Seq[String] = value.filter(_ != null) match {
    case Some(text: String) =>
      text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    case Some(seq: Seq[_]) =>
      seq.map(item => this.requiredText(item, field)).toSeq
    case Some(array: Array[_]) =>
      array.map(item => this.requiredText(item, field)).toSeq
    case Some(list: java.util.List[_]) =>
      list.asScala.map(item => this.requiredText(item, field)).toSeq
    case Some(other) =>
      throw new PuddingException(s"$field must be a string or a list of strings, but got: $other")
    case None => Seq.empty
  }

  private def requiredText(value: Any, field: String): String = value match {
    case text: String if text.trim.nonEmpty => text.trim
    case _ => throw new PuddingException(s"$field must contain non-empty strings")
  }
}
