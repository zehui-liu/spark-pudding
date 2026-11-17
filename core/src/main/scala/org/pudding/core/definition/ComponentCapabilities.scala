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

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.catalog.ResolvedCatalog

/**
 * Declares which shared configuration sections a source or sink can consume. Validation uses
 * these flags so a connection or catalog reference is rejected instead of silently ignored by a
 * connector that does not support it.
 */
case class ComponentCapabilities(
  supportsConnection: Boolean = false,
  supportsCatalog: Boolean = false,
  connectionTypes: Set[String] = Set.empty,
  catalogTypes: Set[String] = Set.empty)

/**
 * Incremental metadata that must survive a transform when an incremental pipeline allows
 * transforms. The transform can use it to keep key, operation and ordering columns.
 */
case class IncrementalTransformContext(
  semantics: IncrementalSemantics,
  keyFields: Seq[String],
  opColumn: String,
  orderFields: Seq[String] = Seq.empty)

/**
 * Runtime context passed to a transform. In a batch pipeline the incremental context is empty.
 */
case class TransformContext(
  incremental: Option[IncrementalTransformContext] = None)

/**
 * Runtime context passed to a connection-aware source. `config` contains the source-specific
 * entries only; connection and catalog values stay in their typed objects so credentials can be
 * kept out of logs and merge precedence remains explicit.
 *
 * `executionMode` is set by BuildJob for every job and is authoritative over connector-level
 * streaming flags. None is used only for direct connector calls outside a job. `trigger` is the
 * optional job-level trigger and lets a streaming source reject trigger modes it cannot support.
 */
case class SourceContext(
  config: Option[Map[String, Any]],
  connection: Option[ResolvedConnection] = None,
  catalog: Option[ResolvedCatalog] = None,
  executionMode: Option[ExecutionMode] = None,
  trigger: Option[String] = None)

/**
 * Runtime context passed to a connection-aware sink.
 *
 * `trigger` is the optional job-level structured-streaming trigger and only applies when the
 * execution mode is streaming. `stateContract` is set only for a stateful streaming pipeline;
 * a sink with a checkpoint must verify it before starting the query.
 */
case class SinkContext(
  config: Option[Map[String, Any]],
  connection: Option[ResolvedConnection] = None,
  catalog: Option[ResolvedCatalog] = None,
  executionMode: Option[ExecutionMode] = None,
  trigger: Option[String] = None,
  stateContract: Option[String] = None)
