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

package org.pudding.core.capability

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.Locale
import java.util.jar.JarFile

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.pudding.core.PuddingException

/**
 * Source, sink, transform, connection and catalog capabilities exposed by a distribution.
 * The manifest is generated at build time and is the metadata source for the backend; it is not
 * maintained by hand.
 */
case class CapabilityComponent(
  kind: String,
  `type`: String,
  className: String,
  module: String,
  jar: String,
  capabilities: ComponentCapabilities = ComponentCapabilities(),
  description: Option[String] = None,
  since: Option[String] = None,
  deprecated: Option[Boolean] = None,
  replacement: Option[String] = None) {

  def key: String = kind + ":" + `type`
}

case class ComponentCapabilities(
  connectionTypes: Seq[String] = Seq.empty,
  catalogTypes: Seq[String] = Seq.empty,
  executionModes: Seq[String] = Seq.empty,
  streamingOutputModes: Seq[String] = Seq.empty,
  streamingUpsert: Boolean = false,
  syncFullRefresh: Boolean = false,
  syncAtomicReplace: Boolean = false,
  syncIncrementalSemantics: Seq[String] = Seq.empty,
  idempotentSyncIncrementalSemantics: Seq[String] = Seq.empty,
  syncIncrementalFencing: Boolean = false,
  lakeSyncIncremental: Boolean = false,
  preservesSyncIncrementalMetadata: Boolean = false)

case class CapabilityManifest(
  schemaVersion: String,
  productVersion: String,
  sparkVersion: String,
  scalaBinaryVersion: String,
  generatedAt: String,
  components: Seq[CapabilityComponent]) {

  def byKey: Map[String, CapabilityComponent] = this.components.map(c => c.key -> c).toMap

  /**
   * A stable digest of the capability set, excluding build timestamps. A backend can store this
   * to decide whether a new distribution actually changed its capabilities.
   */
  def capabilitySetHash: String = {
    val canonical = this.schemaVersion + "|" +
      this.components.sortBy(c => (c.kind, c.`type`)).map { component =>
      Seq(
        component.kind,
        component.`type`,
        component.className,
        component.module,
        component.capabilities.connectionTypes.sorted.mkString(","),
        component.capabilities.catalogTypes.sorted.mkString(","),
        component.capabilities.executionModes.sorted.mkString(","),
        component.capabilities.streamingOutputModes.sorted.mkString(","),
        component.capabilities.streamingUpsert.toString,
        component.capabilities.syncFullRefresh.toString,
        component.capabilities.syncAtomicReplace.toString,
        component.capabilities.syncIncrementalSemantics.sorted.mkString(","),
        component.capabilities.idempotentSyncIncrementalSemantics.sorted.mkString(","),
        component.capabilities.syncIncrementalFencing.toString,
        component.capabilities.lakeSyncIncremental.toString,
        component.capabilities.preservesSyncIncrementalMetadata.toString,
        component.description.getOrElse(""),
        component.since.getOrElse(""),
        component.deprecated.map(_.toString).getOrElse(""),
        component.replacement.getOrElse("")
      ).mkString("\u0001")
    }.mkString("\u0002")
    CapabilityManifest.sha256(canonical)
  }
}

case class CapabilityFragment(
  schemaVersion: String,
  components: Seq[CapabilityComponent])

case class CapabilityChange(
  before: CapabilityComponent,
  after: CapabilityComponent)

case class CapabilityDiff(
  added: Seq[CapabilityComponent],
  removed: Seq[CapabilityComponent],
  changed: Seq[CapabilityChange]) {

  def isEmpty: Boolean = this.added.isEmpty && this.removed.isEmpty && this.changed.isEmpty
}

object CapabilityKind {
  val Source = "source"
  val Sink = "sink"
  val Transform = "transform"
  val Connection = "connection"
  val Catalog = "catalog"

  val All: Set[String] = Set(Source, Sink, Transform, Connection, Catalog)
}

object CapabilityManifest {

  val SchemaVersion = "2.1"

  val RootFileName = "capabilities.yaml"

  val FragmentPath = "META-INF/spark-pudding/component.yaml"

  val AlternateFragmentPath = "META-INF/spark-pudding/component.yml"

  /**
   * Schema 2.0 manifests remain readable; 2.1 adds streaming output-mode declarations.
   */
  private val SupportedSchemaVersions = Set("2.0", SchemaVersion)

  /**
   * Schema 1.0 field names are rejected explicitly instead of being ignored, so a fragment that
   * copied an old manifest does not silently lose its incremental capabilities.
   */
  private val RetiredCapabilityFields = Map(
    "incrementalSemantics" -> "syncIncrementalSemantics",
    "idempotentIncrementalSemantics" -> "idempotentSyncIncrementalSemantics",
    "incrementalFencing" -> "syncIncrementalFencing",
    "fullRefresh" -> "syncFullRefresh",
    "lakeIncremental" -> "lakeSyncIncremental",
    "preservesIncrementalMetadata" -> "preservesSyncIncrementalMetadata")

  /**
   * Load the merged manifest from a distribution. The root manifest is mandatory: a missing file
   * is rejected instead of silently treating the distribution as empty.
   */
  def load(distributionRoot: Path): CapabilityManifest = {
    val root = distributionRoot.resolve(RootFileName)
    if (!Files.isRegularFile(root)) {
      throw new PuddingException(
        s"capability manifest is missing: $root; refusing to use this distribution")
    }
    val base = this.parseManifest(
      new String(Files.readAllBytes(root), StandardCharsets.UTF_8), root.toString)
    val fragments = this.scanFragments(distributionRoot)
    val merged = this.merge(base, fragments, root.toString)
    this.validate(merged, root.toString)
    merged
  }

  /**
   * Verify that the root manifest exists, merges cleanly with jar fragments and that every
   * component's jar is present in the distribution.
   */
  def verifyDistribution(distributionRoot: Path): CapabilityManifest = {
    val manifest = this.load(distributionRoot)
    val jars = this.listJars(distributionRoot.resolve("libs"))
      .map(_.getFileName.toString).toSet
    val missing = manifest.components.map(_.jar).distinct.filterNot(jars.contains).sorted
    if (missing.nonEmpty) {
      throw new PuddingException(
        s"capability manifest references missing jars: ${missing.mkString(", ")}")
    }
    manifest
  }

  def parseManifest(text: String, source: String): CapabilityManifest = {
    val root = this.parseYaml(text, source)
    val obj = this.asObject(root, source)
    val schemaVersion = this.requiredString(obj, "schemaVersion", source)
    this.checkSchema(schemaVersion, source)
    val manifest = CapabilityManifest(
      schemaVersion = schemaVersion,
      productVersion = this.requiredString(obj, "productVersion", source),
      sparkVersion = this.requiredString(obj, "sparkVersion", source),
      scalaBinaryVersion = this.requiredString(obj, "scalaBinaryVersion", source),
      generatedAt = this.requiredString(obj, "generatedAt", source),
      components = this.components(obj, source))
    this.validate(manifest, source)
    manifest
  }

  def parseFragment(text: String, source: String): CapabilityFragment = {
    val root = this.parseYaml(text, source)
    val obj = this.asObject(root, source)
    val schemaVersion = this.requiredString(obj, "schemaVersion", source)
    this.checkSchema(schemaVersion, source)
    val fragment = CapabilityFragment(
      schemaVersion = schemaVersion,
      components = this.components(obj, source))
    if (fragment.components.isEmpty) {
      throw new PuddingException(s"capability fragment in $source has no components")
    }
    this.validateComponents(fragment.components, source)
    fragment
  }

  def diff(previous: CapabilityManifest, current: CapabilityManifest): CapabilityDiff = {
    val previousByKey = previous.byKey
    val currentByKey = current.byKey
    val added = currentByKey.keySet.diff(previousByKey.keySet).toSeq.sorted
      .map(currentByKey)
    val removed = previousByKey.keySet.diff(currentByKey.keySet).toSeq.sorted
      .map(previousByKey)
    val changed = previousByKey.keySet.intersect(currentByKey.keySet).toSeq
      .filter(key => previousByKey(key) != currentByKey(key)).sorted
      .map(key => CapabilityChange(previousByKey(key), currentByKey(key)))
    CapabilityDiff(added, removed, changed)
  }

  private[capability] def validate(
      manifest: CapabilityManifest,
      source: String): Unit = {
    this.checkSchema(manifest.schemaVersion, source)
    Seq(
      "productVersion" -> manifest.productVersion,
      "sparkVersion" -> manifest.sparkVersion,
      "scalaBinaryVersion" -> manifest.scalaBinaryVersion,
      "generatedAt" -> manifest.generatedAt).foreach { case (key, value) =>
      if (value == null || value.trim.isEmpty) {
        throw new PuddingException(s"capability manifest in $source has an empty $key")
      }
    }
    if (manifest.components.isEmpty) {
      throw new PuddingException(s"capability manifest in $source has no components")
    }
    this.validateComponents(manifest.components, source)
  }

  private def validateComponents(
      components: Seq[CapabilityComponent],
      source: String): Unit = {
    components.foreach { component =>
      if (!CapabilityKind.All.contains(component.kind)) {
        throw new PuddingException(
          s"capability component in $source has an unsupported kind: ${component.kind}")
      }
      Seq(
        "type" -> component.`type`,
        "className" -> component.className,
        "module" -> component.module,
        "jar" -> component.jar).foreach { case (key, value) =>
        if (value == null || value.trim.isEmpty) {
          throw new PuddingException(
            s"capability component ${component.kind}:${component.`type`} in $source " +
              s"has an empty $key")
        }
      }
    }

    val duplicated = components.groupBy(_.key).collect {
      case (key, occurrences) if occurrences.size > 1 => key
    }.toSeq.sorted
    if (duplicated.nonEmpty) {
      throw new PuddingException(
        s"capability manifest in $source contains duplicate components: " +
          duplicated.mkString(", "))
    }
  }

  private def checkSchema(schemaVersion: String, source: String): Unit = {
    if (!SupportedSchemaVersions.contains(schemaVersion)) {
      throw new PuddingException(
        s"unsupported capability manifest schemaVersion in $source: $schemaVersion, " +
          s"supported: ${SupportedSchemaVersions.toSeq.sorted.mkString(", ")}")
    }
  }

  private def merge(
      base: CapabilityManifest,
      fragments: Seq[(String, CapabilityFragment)],
      rootSource: String): CapabilityManifest = {
    val entries = base.components.map(component => (component.key, (component, rootSource))) ++
      fragments.flatMap { case (source, fragment) =>
        fragment.components.map(component => (component.key, (component, source)))
      }

    val duplicates = entries.groupBy(_._1).collect {
      case (key, values) if values.size > 1 =>
        key -> values.map(_._2._2).distinct.sorted.mkString(", ")
    }.toSeq.sortBy(_._1)
    if (duplicates.nonEmpty) {
      throw new PuddingException(
        "capability component is defined more than once: " +
          duplicates.map { case (key, sources) => s"$key ($sources)" }.mkString(", "))
    }

    base.copy(components = entries.map(_._2._1).sortBy(c => (c.kind, c.`type`)))
  }

  private def scanFragments(
      distributionRoot: Path): Seq[(String, CapabilityFragment)] = {
    val libs = distributionRoot.resolve("libs")
    if (!Files.isDirectory(libs)) {
      Seq.empty
    } else {
      val stream = Files.walk(libs)
      try {
        stream.iterator().asScala
          .filter(path => Files.isRegularFile(path) &&
            path.getFileName.toString.toLowerCase(Locale.ROOT).endsWith(".jar"))
          .flatMap(path => this.readFragment(path).toSeq)
          .toVector
      } finally {
        stream.close()
      }
    }
  }

  private def readFragment(jar: Path): Option[(String, CapabilityFragment)] = {
    val jarFile = new JarFile(jar.toFile)
    try {
      val entry = Option(jarFile.getEntry(FragmentPath))
        .orElse(Option(jarFile.getEntry(AlternateFragmentPath)))
      entry.map { value =>
        val input = jarFile.getInputStream(value)
        val text = try {
          scala.io.Source.fromInputStream(input, StandardCharsets.UTF_8.name()).mkString
        } finally {
          input.close()
        }
        val fragment = this.parseFragment(text, jar.toString)
        val mismatched = fragment.components.filterNot(_.jar == jar.getFileName.toString)
        if (mismatched.nonEmpty) {
          throw new PuddingException(
            s"capability fragment in $jar declares jars that do not match the containing jar: " +
              mismatched.map(_.jar).distinct.sorted.mkString(", "))
        }
        jar.toString -> fragment
      }
    } finally {
      jarFile.close()
    }
  }

  private def listJars(libs: Path): Seq[Path] = {
    if (!Files.isDirectory(libs)) {
      Seq.empty
    } else {
      val stream = Files.walk(libs)
      try {
        stream.iterator().asScala
          .filter(path => Files.isRegularFile(path) &&
            path.getFileName.toString.toLowerCase(Locale.ROOT).endsWith(".jar"))
          .toVector
      } finally {
        stream.close()
      }
    }
  }

  private def components(obj: JObject, source: String): Seq[CapabilityComponent] = {
    this.requiredArray(obj, "components", source).map(value => this.component(value, source))
  }

  private def component(value: JValue, source: String): CapabilityComponent = {
    val obj = this.asObject(value, source)
    CapabilityComponent(
      kind = this.requiredString(obj, "kind", source),
      `type` = this.requiredString(obj, "type", source),
      className = this.requiredString(obj, "className", source),
      module = this.requiredString(obj, "module", source),
      jar = this.requiredString(obj, "jar", source),
      capabilities = this.capabilities(obj \ "capabilities", source),
      description = this.optionalString(obj, "description", source),
      since = this.optionalString(obj, "since", source),
      deprecated = this.optionalBoolean(obj, "deprecated", source),
      replacement = this.optionalString(obj, "replacement", source))
  }

  private def capabilities(value: JValue, source: String): ComponentCapabilities = value match {
    case JNothing => ComponentCapabilities()
    case obj: JObject =>
      this.rejectRetiredCapabilityFields(obj, source)
      ComponentCapabilities(
        connectionTypes = this.stringArray(obj \ "connectionTypes", "connectionTypes", source),
        catalogTypes = this.stringArray(obj \ "catalogTypes", "catalogTypes", source),
        executionModes = this.stringArray(obj \ "executionModes", "executionModes", source),
        streamingOutputModes = this.stringArray(
          obj \ "streamingOutputModes", "streamingOutputModes", source),
        streamingUpsert = this.optionalBoolean(
          obj \ "streamingUpsert", "streamingUpsert", source),
        syncFullRefresh =
          this.optionalBoolean(obj \ "syncFullRefresh", "syncFullRefresh", source),
        syncAtomicReplace =
          this.optionalBoolean(obj \ "syncAtomicReplace", "syncAtomicReplace", source),
        syncIncrementalSemantics = this.stringArray(
          obj \ "syncIncrementalSemantics", "syncIncrementalSemantics", source),
        idempotentSyncIncrementalSemantics = this.stringArray(
          obj \ "idempotentSyncIncrementalSemantics",
          "idempotentSyncIncrementalSemantics", source),
        syncIncrementalFencing = this.optionalBoolean(
          obj \ "syncIncrementalFencing", "syncIncrementalFencing", source),
        lakeSyncIncremental = this.optionalBoolean(
          obj \ "lakeSyncIncremental", "lakeSyncIncremental", source),
        preservesSyncIncrementalMetadata = this.optionalBoolean(
          obj \ "preservesSyncIncrementalMetadata",
          "preservesSyncIncrementalMetadata", source))
    case _ =>
      throw new PuddingException(s"capabilities in $source must be a map")
  }

  private def rejectRetiredCapabilityFields(obj: JObject, source: String): Unit = {
    val retired = CapabilityManifest.RetiredCapabilityFields.collect {
      case (field, replacement) if (obj \ field) != JNothing =>
        s"$field (use $replacement)"
    }.toSeq.sorted
    if (retired.nonEmpty) {
      throw new PuddingException(
        s"capability manifest in $source uses retired schema 1.0 capability fields: " +
          s"${retired.mkString(", ")}; rewrite the manifest for schemaVersion " +
          CapabilityManifest.SchemaVersion)
    }
  }

  private def optionalBoolean(value: JValue, key: String, source: String): Boolean = value match {
    case JNothing => false
    case JBool(result) => result
    case _ =>
      throw new PuddingException(s"capabilities.$key in $source must be a boolean")
  }

  private def stringArray(value: JValue, key: String, source: String): Seq[String] = value match {
    case JNothing => Seq.empty
    case JArray(values) =>
      values.map {
        case JString(text) if text.trim.nonEmpty => text.trim
        case _ =>
          throw new PuddingException(
            s"capabilities.$key in $source must contain non-empty strings")
      }
    case _ =>
      throw new PuddingException(s"capabilities.$key in $source must be a list")
  }

  private def parseYaml(text: String, source: String): JValue = {
    val jsonText = io.circe.yaml.parser.parse(text) match {
      case Right(json) => json.toString()
      case Left(error) =>
        throw new PuddingException(s"invalid capability YAML in $source: $error")
    }
    try {
      parse(jsonText)
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"failed to parse capability YAML in $source: ${error.getMessage}")
    }
  }

  private def asObject(value: JValue, source: String): JObject = value match {
    case obj: JObject => obj
    case _ => throw new PuddingException(s"capability manifest in $source must be a map")
  }

  private def requiredString(obj: JObject, key: String, source: String): String = {
    obj \ key match {
      case JString(value) if value.trim.nonEmpty => value.trim
      case JString(_) =>
        throw new PuddingException(s"capability manifest in $source has an empty $key")
      case JNothing =>
        throw new PuddingException(s"capability manifest in $source is missing $key")
      case _ =>
        throw new PuddingException(s"capability manifest in $source has a non-string $key")
    }
  }

  private def optionalString(
      obj: JObject,
      key: String,
      source: String): Option[String] = {
    obj \ key match {
      case JNothing | JNull => None
      case JString(value) => Option(value.trim).filter(_.nonEmpty)
      case _ =>
        throw new PuddingException(s"capability manifest in $source has a non-string $key")
    }
  }

  private def optionalBoolean(
      obj: JObject,
      key: String,
      source: String): Option[Boolean] = {
    obj \ key match {
      case JNothing | JNull => None
      case JBool(value) => Some(value)
      case _ =>
        throw new PuddingException(s"capability manifest in $source has a non-boolean $key")
    }
  }

  private def requiredArray(
      obj: JObject,
      key: String,
      source: String): Seq[JValue] = {
    obj \ key match {
      case JArray(values) => values
      case JNothing =>
        throw new PuddingException(s"capability manifest in $source is missing $key")
      case _ =>
        throw new PuddingException(s"capability manifest in $source has a non-list $key")
    }
  }

  def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x").mkString
  }
}
