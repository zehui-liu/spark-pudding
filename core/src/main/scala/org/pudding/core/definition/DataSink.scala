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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import org.pudding.core.configure.{ExecutionMode, StreamingOutputMode}
import org.pudding.core.{EasyHandle, PuddingException}

/**
 * Support writing dataframes to different data sources, with parameters passed in the form of maps.
 * Interface implementers need to perform interface validation.
 */
trait DataSink extends EasyHandle with Logging {

  /**
   * Unique identifier used as the sink `type` in job configuration.
   */
  def identifier: String

  /**
   * Writes the DataFrame. Implementations validate their own configuration.
   */
  def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit

  /**
   * Writes the DataFrame with resolved connection and catalog references. The default
   * implementation delegates to the config-only overload so existing connectors keep working.
   */
  def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    this.writer(dataFrame, context.config)
  }

  /**
   * Incremental semantics supported by this sink. Existing sinks are append-capable by default;
   * a sink that supports upsert or CDC must override this and `writeIncremental`.
   */
  def supportedIncrementalSemantics: Set[IncrementalSemantics] =
    Set(IncrementalSemantics.Append)

  /**
   * Whether applying the same incremental batch twice is idempotent for the given semantics.
   * This is used when a previous run left an uncommitted sink transaction: an idempotent sink
   * can safely replay the batch, while an append-only sink requires an operator decision.
   */
  def isIdempotentIncremental(semantics: IncrementalSemantics): Boolean = false

  /**
   * Whether this sink records the incremental source id and fencing token in its commit
   * metadata and rejects a writer whose token is older than the newest committed token for the
   * source. This narrows the window in which a writer whose lock lease expired can commit, but
   * it is not a substitute for a transactional sink or an idempotent transaction id.
   */
  def supportsIncrementalFencing: Boolean = false

  /**
   * Applies one incremental result. The default implementation only accepts append semantics
   * and delegates to the ordinary writer, keeping existing sinks compatible.
   */
  def writeIncremental(
      dataFrame: DataFrame,
      context: SinkContext,
      incremental: IncrementalWrite): Unit = {
    if (incremental.semantics != IncrementalSemantics.Append) {
      throw new PuddingException(
        s"sink '$identifier' does not support ${incremental.semantics.name} semantics")
    }
    this.writer(dataFrame, context)
  }

  /**
   * Execution modes this sink supports. Batch is the safe default for existing connectors.
   */
  def supportedExecutionModes: Set[ExecutionMode] = Set(ExecutionMode.Batch)

  /**
   * Streaming output modes this sink accepts. Append is the safe default. A sink that accepts
   * update must either implement key-based upsert itself or delegate to a connector that does.
   */
  def supportedStreamingOutputModes: Set[String] = Set(StreamingOutputMode.Append)

  /**
   * Whether update output has key-based upsert semantics. This is a capability declaration used
   * by the manifest and documentation; the sink still validates its own key configuration.
   */
  def supportsStreamingUpsert: Boolean = false

  /**
   * Whether this sink can write a stateful streaming query. A stateful transform needs a stable
   * checkpoint that Spark can use to restore operator state; sinks without one must reject it.
   */
  def supportsStatefulStreaming: Boolean = true

  /**
   * Whether this sink supports a full-refresh write, meaning it can replace the target contents
   * with one complete snapshot. For `sync` + `full`, BuildJob injects overwrite mode; a direct
   * caller of the sink must select it explicitly.
   */
  def supportsFullRefresh: Boolean = false

  /**
   * Whether [[writeFullRefresh]] replaces the target atomically for the selected backend. This
   * is stronger than [[supportsFullRefresh]]: a sink may be able to overwrite a target without
   * being able to guarantee that readers see either the old or the new complete snapshot.
   */
  def supportsAtomicReplace: Boolean = false

  /**
   * Replace the target contents for `sync` with `strategy: full`. The default implementation
   * uses the ordinary writer; the caller has already forced overwrite mode. Atomic sinks
   * override this method and stage the write before publishing it.
   */
  def writeFullRefresh(dataFrame: DataFrame, context: SinkContext): Unit = {
    this.writer(dataFrame, context)
  }

  /**
   * Whether this sink participates in the constrained sync incremental mode.
   *
   * The older name referred to the initial lakehouse-only implementation. New sinks should
   * override this method; the default keeps existing lakehouse implementations compatible.
   */
  def supportsIncrementalSync: Boolean = supportsLakeIncremental

  /**
   * Legacy capability name retained for source compatibility.
   */
  @deprecated("override supportsIncrementalSync", "1.0.0")
  def supportsLakeIncremental: Boolean = false

  /**
   * Whether this sink can consume a top-level connection definition.
   */
  def supportsConnection: Boolean = this.supportedConnectionTypes.nonEmpty

  /**
   * Whether this sink can consume a top-level catalog definition.
   */
  def supportsCatalog: Boolean = this.supportedCatalogTypes.nonEmpty

  /**
   * Connection types accepted by this sink.
   */
  def supportedConnectionTypes: Set[String] = Set.empty

  /**
   * Catalog types accepted by this sink.
   */
  def supportedCatalogTypes: Set[String] = Set.empty
}

object DataSink {

  /**
   * Explicit opt-in for a `sync` full-refresh write to a sink that cannot guarantee an atomic
   * target replacement.
   */
  val AllowNonAtomicReplaceKey = "allowNonAtomicReplace"

  /**
   * identifiers of all available DataSink implementations, used to validate a pipeline before
   * Spark is started.
   */
  def identifiers: Set[String] = this.loadServices().keySet

  /**
   * all available sink implementations, keyed by identifier.
   */
  def services(): Map[String, DataSink] = this.loadServices()

  /**
   * capabilities of every sink implementation, used to reject unsupported references before a
   * Spark session is created.
   */
  def capabilities(): Map[String, ComponentCapabilities] = {
    this.loadServices().map { case (identifier, sink) =>
      identifier -> ComponentCapabilities(
        sink.supportsConnection,
        sink.supportsCatalog,
        sink.supportedConnectionTypes,
        sink.supportedCatalogTypes)
    }
  }

  private def loadServices(): Map[String, DataSink] = {
    val serviceLoader = ServiceLoader.load(classOf[DataSink])
    serviceLoader.iterator().asScala.map(s => s.identifier -> s).toMap
  }
}
