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

package org.pudding.core.configure

import scala.collection.immutable._

/**
 * job pipeline configure
 */
case class JobPipelineConf(
  sources: Seq[SourceConf],
  transforms: Option[Seq[TransformConf]],
  sinks: Seq[SinkConf],
  job: JobConf,
  connections: Seq[ConnectionConf] = Seq.empty,
  catalogs: Seq[CatalogConf] = Seq.empty)
