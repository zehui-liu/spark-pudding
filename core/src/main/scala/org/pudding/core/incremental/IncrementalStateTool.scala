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

import java.io.PrintStream

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.pudding.core.PuddingException

/**
 * Inspect and repair incremental state without starting a Spark job.
 *
 * The tool reads the selected state backend and resolves only state-backend secret references.
 * It never resolves source, sink, connection or catalog configuration.
 */
// scalastyle:off println
object IncrementalStateTool {

  private val Usage =
    """Usage:
      |  pudding state show <state-target> [source-id]
      |  pudding state status <state-target> [source-id] [--json]
      |  pudding state list <state-target>
      |  pudding state reset <state-target> <source-id> --cursor <cursor> [--force]
      |  pudding state reset <state-target> <source-id> --clear [--force]
      |  pudding state resolve <state-target> <source-id> --commit|--abort [--force]
      |  pudding state unlock <state-target> <source-id>
      |  pudding state migrate <state-target>
      |  pudding state --help
      |
      |A state target is a filesystem path or @/path/to/state.properties. The properties file
      |selects the backend and supplies its options. `status` emits machine-readable JSON.
      |`resolve --commit` is used when the sink write succeeded but the cursor commit did not.
      |`resolve --abort` discards the pending transaction so the next job run replays it.
      |`--force` breaks a lock left by a failed process; verify that no job is still running.
      |""".stripMargin

  private final class UsageException(message: String) extends RuntimeException(message)

  def main(args: Array[String]): Unit = {
    System.exit(this.run(args, System.out, System.err))
  }

  def run(args: Array[String], out: PrintStream, err: PrintStream): Int = {
    try {
      args.toList match {
        case Nil | List("-h") | List("--help") =>
          out.println(Usage)
          0
        case "show" :: rest =>
          this.show(rest, out)
        case "status" :: rest =>
          this.status(rest, out)
        case "list" :: rest =>
          this.show(rest, out)
        case "reset" :: rest =>
          this.reset(rest, out)
        case "resolve" :: rest =>
          this.resolve(rest, out)
        case "unlock" :: rest =>
          this.unlock(rest, out)
        case "migrate" :: rest =>
          this.migrate(rest, out)
        case _ =>
          throw new UsageException("unknown command")
      }
    } catch {
      case error: UsageException =>
        err.println(s"pudding state: ${error.getMessage}")
        err.println(Usage)
        2
      case error: PuddingException =>
        err.println(s"pudding state: ${error.getMessage}")
        1
      case NonFatal(error) =>
        val detail = Option(error.getMessage).filter(_.nonEmpty)
          .getOrElse(error.getClass.getSimpleName)
        err.println(s"pudding state: $detail")
        1
    }
  }

  private def show(args: List[String], out: PrintStream): Int = {
    val (positional, options) = this.parse(
      args, Set.empty, Set.empty)
    if (options.nonEmpty || positional.isEmpty || positional.size > 2) {
      throw new UsageException("show requires a state target and an optional source id")
    }
    val store = this.store(positional.head)
    positional match {
      case List(_) =>
        val sources = store.listSourceIds()
        if (sources.isEmpty) {
          out.println(s"no incremental state found in ${positional.head}")
        } else {
          sources.foreach(sourceId => this.printState(store, sourceId, out))
        }
      case List(_, sourceId) =>
        this.printState(store, sourceId, out)
      case _ =>
        throw new UsageException("show requires a state target and an optional source id")
    }
    0
  }

  private def status(args: List[String], out: PrintStream): Int = {
    val (positional, _) = this.parse(args, Set("--json"), Set.empty)
    if (positional.isEmpty || positional.size > 2) {
      throw new UsageException("status requires a state target and an optional source id")
    }
    val store = this.store(positional.head)
    val values = positional match {
      case List(_) =>
        store.listSourceIds().map(sourceId => this.statusJson(store, sourceId))
      case List(_, sourceId) =>
        Seq(this.statusJson(store, sourceId))
      case _ =>
        throw new UsageException("status requires a state target and an optional source id")
    }
    val payload = if (positional.size == 1) JArray(values.toList) else values.head
    out.println(compact(render(payload)))
    0
  }

  private def reset(args: List[String], out: PrintStream): Int = {
    val (positional, options) = this.parse(
      args, Set("--clear", "--force"), Set("--cursor"))
    if (positional.size != 2) {
      throw new UsageException("reset requires a state target and a source id")
    }
    val cursor = options.get("--cursor")
    val clear = options.contains("--clear")
    val force = options.contains("--force")
    if (cursor.nonEmpty == clear) {
      throw new UsageException("reset requires exactly one of --cursor or --clear")
    }
    val store = this.store(positional.head)
    val sourceId = positional(1)
    this.withLock(store, sourceId, force) {
      if (clear) {
        store.clear(sourceId)
        out.println(s"cleared incremental state for source '$sourceId'")
      } else {
        store.reset(sourceId, cursor)
        out.println(
          s"reset incremental cursor for source '$sourceId' to " +
            s"'${cursor.getOrElse("")}'")
      }
    }
    0
  }

  private def resolve(args: List[String], out: PrintStream): Int = {
    val (positional, options) = this.parse(
      args, Set("--commit", "--abort", "--force"), Set.empty)
    if (positional.size != 2) {
      throw new UsageException("resolve requires a state target and a source id")
    }
    val commit = options.contains("--commit")
    val abort = options.contains("--abort")
    val force = options.contains("--force")
    if (commit == abort) {
      throw new UsageException("resolve requires exactly one of --commit or --abort")
    }
    val store = this.store(positional.head)
    val sourceId = positional(1)
    this.withLock(store, sourceId, force) {
      val state = store.load(sourceId)
      val pending = state.pending.getOrElse {
        throw new PuddingException(
          s"source '$sourceId' has no pending incremental transaction")
      }
      if (commit) {
        val committed = store.commit(sourceId, pending.transactionId)
        out.println(
          s"committed transaction '${pending.transactionId}' for source '$sourceId' " +
            s"at cursor '${committed.committedCursor.getOrElse("")}'")
      } else {
        store.abort(sourceId, pending.transactionId)
        out.println(
          s"aborted transaction '${pending.transactionId}' for source '$sourceId'; " +
            "the next run will replay it")
      }
    }
    0
  }

  private def unlock(args: List[String], out: PrintStream): Int = {
    val (positional, options) = this.parse(args, Set.empty, Set.empty)
    if (options.nonEmpty || positional.size != 2) {
      throw new UsageException("unlock requires a state target and a source id")
    }
    val store = this.store(positional.head)
    val sourceId = positional(1)
    val removed = store.forceUnlock(sourceId)
    if (removed) {
      out.println(s"removed incremental state lock for source '$sourceId'")
    } else {
      out.println(s"no incremental state lock found for source '$sourceId'")
    }
    0
  }

  private def migrate(args: List[String], out: PrintStream): Int = {
    val (positional, options) = this.parse(args, Set.empty, Set.empty)
    if (options.nonEmpty || positional.size != 1) {
      throw new UsageException("migrate requires a JDBC state target")
    }
    val (backend, values) = IncrementalStateStore.targetOptions(positional.head)
    if (backend != "jdbc") {
      throw new PuddingException("migrate requires a jdbc state target")
    }
    val location = JdbcStateBackend.migrate(values)
    out.println(s"migrated incremental state table at $location")
    0
  }

  private def printState(
      store: IncrementalStateStore,
      sourceId: String,
      out: PrintStream): Unit = {
    val state = store.load(sourceId)
    out.println(s"source: $sourceId")
    out.println(s"  state: ${store.location}")
    out.println(s"  cursor: ${state.committedCursor.getOrElse("<none>")}")
    state.pending match {
      case Some(pending) =>
        out.println(
          s"  pending: ${pending.transactionId} " +
            s"${pending.fromCursor.getOrElse("<initial>")} -> ${pending.toCursor} " +
            s"(started ${pending.startedAt})")
        out.println(
          s"  plan: ${pending.plan.map(_.fingerprint).getOrElse("<missing>")}")
      case None =>
        out.println("  pending: none")
    }
    out.println(s"  attempts: ${state.attempts}")
    out.println(s"  last transaction: ${state.lastTransactionId.getOrElse("<none>")}")
    out.println(s"  last commit: ${state.lastCommittedAt.getOrElse("<none>")}")
    state.lastFailureAt.foreach { value =>
      out.println(
        s"  last failure: $value ${state.lastFailureReason.getOrElse("")}")
    }
    val lock = try {
      store.lockInfo(sourceId)
    } catch {
      case NonFatal(_) => Some(IncrementalLockInfo("<unreadable>", "<unknown>", "<unknown>", ""))
    }
    lock match {
      case Some(info) =>
        val expiry = info.expiresAt.map(value => s", lease expires at $value").getOrElse("")
        out.println(
          s"  lock: ${info.owner} on ${info.host} (pid ${info.pid}) since " +
            s"${info.acquiredAt} (fencing token ${info.fencingToken}$expiry)")
      case None =>
        out.println("  lock: none")
    }
  }

  private def statusJson(
      store: IncrementalStateStore,
      sourceId: String): JValue = {
    val state = store.load(sourceId)
    val lock = try {
      store.lockInfo(sourceId)
    } catch {
      case NonFatal(_) =>
        Some(IncrementalLockInfo("<unreadable>", "<unknown>", "<unknown>", ""))
    }
    JObject(List(
      JField("sourceId", JString(sourceId)),
      JField("location", JString(store.location)),
      JField("committedCursor", this.optionalJson(state.committedCursor)),
      JField("pending", state.pending.map(this.pendingJson).getOrElse(JNull)),
      JField("attempts", JInt(BigInt(state.attempts))),
      JField("lastTransactionId", this.optionalJson(state.lastTransactionId)),
      JField("lastCommittedAt", this.optionalJson(state.lastCommittedAt)),
      JField("lastFailureAt", this.optionalJson(state.lastFailureAt)),
      JField("lastFailureReason", this.optionalJson(state.lastFailureReason)),
      JField("updatedAt", JString(state.updatedAt)),
      JField("fencingToken", JInt(BigInt(state.fencingToken))),
      JField("lock", lock.map(this.lockJson).getOrElse(JNull))))
  }

  private def pendingJson(pending: IncrementalPendingTransaction): JValue = {
    JObject(List(
      JField("transactionId", JString(pending.transactionId)),
      JField("fromCursor", this.optionalJson(pending.fromCursor)),
      JField("toCursor", JString(pending.toCursor)),
      JField("startedAt", JString(pending.startedAt)),
      JField(
        "planFingerprint",
        pending.plan.map(plan => JString(plan.fingerprint)).getOrElse(JNull)),
      JField("plan", pending.plan.map(this.planJson).getOrElse(JNull))))
  }

  private def planJson(plan: IncrementalPlanDescriptor): JValue = {
    JObject(List(
      JField("sourceType", JString(plan.sourceType)),
      JField("sourceId", JString(plan.sourceId)),
      JField("sinkType", JString(plan.sinkType)),
      JField("sinkId", JString(plan.sinkId)),
      JField("semantics", JString(plan.semantics)),
      JField("keyFields", JArray(plan.keyFields.map(JString).toList)),
      JField("opColumn", JString(plan.opColumn)),
      JField("orderFields", JArray(plan.orderFields.map(JString).toList)),
      JField("sourceConfigHash", JString(plan.sourceConfigHash)),
      JField("sinkConfigHash", JString(plan.sinkConfigHash)),
      JField("stateBackend", JString(plan.stateBackend)),
      JField("stateVersion", JInt(BigInt(plan.stateVersion)))))
  }

  private def lockJson(lock: IncrementalLockInfo): JValue = {
    JObject(List(
      JField("owner", JString(lock.owner)),
      JField("host", JString(lock.host)),
      JField("pid", JString(lock.pid)),
      JField("acquiredAt", JString(lock.acquiredAt)),
      JField("fencingToken", JInt(BigInt(lock.fencingToken))),
      JField("expiresAt", this.optionalJson(lock.expiresAt))))
  }

  private def optionalJson(value: Option[String]): JValue =
    value.map(JString).getOrElse(JNull)

  private def withLock(
      store: IncrementalStateStore,
      sourceId: String,
      force: Boolean)(body: => Unit): Unit = {
    if (force) {
      store.forceUnlock(sourceId)
    }
    store.acquire(sourceId)
    try {
      body
    } finally {
      store.release(sourceId)
    }
  }

  private def store(target: String): IncrementalStateStore = {
    IncrementalStateStore.openTarget(target, new Configuration())
  }

  /**
   * Split positional arguments from flags. Flags may take one value; `withValue` lists them.
   */
  private def parse(
      args: List[String],
      flags: Set[String],
      withValue: Set[String]): (List[String], Map[String, String]) = {
    val positional = scala.collection.mutable.ListBuffer[String]()
    val options = scala.collection.mutable.Map[String, String]()
    var remaining = args
    while (remaining.nonEmpty) {
      remaining.head match {
        case value if flags.contains(value) =>
          options += value -> ""
          remaining = remaining.tail
        case value if withValue.contains(value) =>
          remaining.tail match {
            case next :: tail =>
              options += value -> next
              remaining = tail
            case Nil =>
              throw new UsageException(s"$value requires a value")
          }
        case value if value.startsWith("-") =>
          throw new UsageException(s"unknown option: $value")
        case value =>
          positional += value
          remaining = remaining.tail
      }
    }
    (positional.toList, options.toMap)
  }
}
// scalastyle:on println
