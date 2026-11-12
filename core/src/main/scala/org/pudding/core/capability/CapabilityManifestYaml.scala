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

package org.pudding.core.capability

import org.json4s._
import org.json4s.jackson.JsonMethods

import org.pudding.core.PuddingException

/**
 * Renders a capability manifest in the canonical YAML shape used at the distribution root.
 */
object CapabilityManifestYaml {

  def render(manifest: CapabilityManifest): String = {
    val jsonText = JsonMethods.compact(JsonMethods.render(this.toJson(manifest)))
    val json = io.circe.yaml.parser.parse(jsonText) match {
      case Right(value) => value
      case Left(error) =>
        throw new PuddingException(s"failed to render capability manifest: $error")
    }
    val yaml = io.circe.yaml.Printer.spaces2.pretty(json)
    if (yaml.endsWith("\n")) yaml else yaml + "\n"
  }

  private def toJson(manifest: CapabilityManifest): JObject = {
    JObject(List(
      JField("schemaVersion", JString(manifest.schemaVersion)),
      JField("productVersion", JString(manifest.productVersion)),
      JField("sparkVersion", JString(manifest.sparkVersion)),
      JField("scalaBinaryVersion", JString(manifest.scalaBinaryVersion)),
      JField("generatedAt", JString(manifest.generatedAt)),
      JField(
        "components",
        JArray(manifest.components.sortBy(c => (c.kind, c.`type`))
          .map(this.componentToJson).toList))
    ))
  }

  private def componentToJson(component: CapabilityComponent): JObject = {
    val required = List(
      JField("kind", JString(component.kind)),
      JField("type", JString(component.`type`)),
      JField("className", JString(component.className)),
      JField("module", JString(component.module)),
      JField("jar", JString(component.jar))
    )
    val capabilities = if (component.capabilities.connectionTypes.nonEmpty ||
      component.capabilities.catalogTypes.nonEmpty ||
      component.capabilities.executionModes.nonEmpty ||
      component.capabilities.streamingOutputModes.nonEmpty ||
      component.capabilities.streamingUpsert ||
      component.capabilities.syncFullRefresh ||
      component.capabilities.syncAtomicReplace ||
      component.capabilities.syncIncrementalSemantics.nonEmpty ||
      component.capabilities.idempotentSyncIncrementalSemantics.nonEmpty ||
      component.capabilities.syncIncrementalFencing ||
      component.capabilities.lakeSyncIncremental ||
      component.capabilities.preservesSyncIncrementalMetadata) {
      List(JField(
        "capabilities",
        JObject(List(
          JField(
            "connectionTypes",
            JArray(component.capabilities.connectionTypes.sorted.map(JString).toList)),
          JField(
            "catalogTypes",
            JArray(component.capabilities.catalogTypes.sorted.map(JString).toList)),
          JField(
            "executionModes",
            JArray(component.capabilities.executionModes.sorted.map(JString).toList)),
          JField(
            "streamingOutputModes",
            JArray(component.capabilities.streamingOutputModes.sorted
              .map(JString).toList)),
          JField("streamingUpsert", JBool(component.capabilities.streamingUpsert)),
          JField("syncFullRefresh", JBool(component.capabilities.syncFullRefresh)),
          JField("syncAtomicReplace", JBool(component.capabilities.syncAtomicReplace)),
          JField(
            "syncIncrementalSemantics",
            JArray(component.capabilities.syncIncrementalSemantics.sorted
              .map(JString).toList)),
          JField(
            "idempotentSyncIncrementalSemantics",
            JArray(component.capabilities.idempotentSyncIncrementalSemantics.sorted
              .map(JString).toList)),
          JField(
            "syncIncrementalFencing",
            JBool(component.capabilities.syncIncrementalFencing)),
          JField("lakeSyncIncremental", JBool(component.capabilities.lakeSyncIncremental)),
          JField(
            "preservesSyncIncrementalMetadata",
            JBool(component.capabilities.preservesSyncIncrementalMetadata))
        ))))
    } else {
      List.empty[JField]
    }
    val optional = List(
      component.description.map(value => JField("description", JString(value))),
      component.since.map(value => JField("since", JString(value))),
      component.deprecated.map(value => JField("deprecated", JBool(value))),
      component.replacement.map(value => JField("replacement", JString(value)))
    ).flatten

    JObject(required ++ capabilities ++ optional)
  }
}
