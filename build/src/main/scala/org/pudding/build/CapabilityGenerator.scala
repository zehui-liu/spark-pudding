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

package org.pudding.build

import java.io.{File, InputStream}
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.ServiceLoader
import java.util.jar.JarFile

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.pudding.core.PuddingException
import org.pudding.core.capability._
import org.pudding.core.catalog.CatalogProvider
import org.pudding.core.connection.ConnectionProvider
import org.pudding.core.definition.{
  DataSink,
  DataSource,
  DataTransform,
  IncrementalSource
}

/**
 * Build-time generator for the distribution capability manifest.
 *
 * The generator loads every Component SPI with an isolated URLClassLoader, asks each component
 * for its identifier and capabilities, and writes the root capabilities.yaml. Jar fragments from
 * third-party connectors are not part of the generator input: they are merged by the backend at
 * scan time.
 */
object CapabilityGenerator {

  private val ServicePaths = Seq(
    "META-INF/services/org.pudding.core.definition.DataSource",
    "META-INF/services/org.pudding.core.definition.DataSink",
    "META-INF/services/org.pudding.core.definition.DataTransform",
    "META-INF/services/org.pudding.core.connection.ConnectionProvider",
    "META-INF/services/org.pudding.core.catalog.CatalogProvider")

  private case class ModuleJar(module: String, jar: Path)

  private case class Origin(module: String, jar: String)

  def main(args: Array[String]): Unit = {
    if (args == null || args.length != 5) {
      throw new PuddingException(
        "usage: CapabilityGenerator <repository-root> <version> <spark-version> " +
          "<scala-binary-version> <output-file>")
    }

    val repositoryRoot = Paths.get(args(0)).toAbsolutePath.normalize()
    val version = args(1)
    val sparkVersion = args(2)
    val scalaBinaryVersion = args(3)
    val output = Paths.get(args(4)).toAbsolutePath.normalize()

    val moduleJars = this.discoverModuleJars(repositoryRoot, version)
    val origins = this.scanOrigins(moduleJars)
    val connectorJars = moduleJars.filterNot(_.module == "core").map(_.jar)
    val loader = new URLClassLoader(
      connectorJars.map(_.toUri.toURL).toArray,
      Thread.currentThread().getContextClassLoader)
    try {
      val components = this.components(loader, origins)
      val manifest = CapabilityManifest(
        schemaVersion = CapabilityManifest.SchemaVersion,
        productVersion = version,
        sparkVersion = sparkVersion,
        scalaBinaryVersion = scalaBinaryVersion,
        generatedAt = Instant.now().toString,
        components = components.sortBy(component => (component.kind, component.`type`)))
      val yaml = CapabilityManifestYaml.render(manifest)
      // Round-trip through the reader so duplicate keys and malformed entries fail the build.
      CapabilityManifest.parseManifest(yaml, output.toString)
      Option(output.getParent).foreach(parent => Files.createDirectories(parent))
      Files.write(output, yaml.getBytes(StandardCharsets.UTF_8))
      // scalastyle:off println
      println(
        s"generated capability manifest: ${components.size} components -> $output")
      // scalastyle:on println
    } finally {
      loader.close()
    }
  }

  private def discoverModuleJars(
      repositoryRoot: Path,
      version: String): Seq[ModuleJar] = {
    val coreJar = repositoryRoot.resolve("core").resolve("target")
      .resolve(s"pudding-core-$version.jar")
    this.requireJar(coreJar)
    val core = Seq(ModuleJar("core", coreJar))

    val connectors = Seq("connector", "provider", "runtime", "source", "sink", "transform")
      .flatMap { group =>
        val groupDirectory = repositoryRoot.resolve(group).toFile
        if (!groupDirectory.isDirectory) {
          Seq.empty
        } else {
          Option(groupDirectory.listFiles()).getOrElse(Array.empty[File])
            .filter(directory => directory.isDirectory &&
              new File(directory, "pom.xml").isFile)
            .sortBy(_.getName)
            .map { directory =>
              val artifact = directory.getName
              val jar = directory.toPath.resolve("target")
                .resolve(s"$artifact-$version.jar")
              this.requireJar(jar)
              ModuleJar(artifact, jar)
            }
            .toSeq
        }
      }

    core ++ connectors
  }

  private def requireJar(jar: Path): Unit = {
    if (!Files.isRegularFile(jar)) {
      throw new PuddingException(
        s"module jar is missing: $jar; run the full reactor build before the build module")
    }
  }

  private def scanOrigins(moduleJars: Seq[ModuleJar]): Map[String, Origin] = {
    val origins = mutable.Map[String, Origin]()
    moduleJars.foreach { moduleJar =>
      val jarFile = new JarFile(moduleJar.jar.toFile)
      try {
        ServicePaths.foreach { servicePath =>
          Option(jarFile.getEntry(servicePath)).foreach { entry =>
            val input = jarFile.getInputStream(entry)
            val lines = try readLines(input) finally input.close()
            lines.foreach { className =>
              origins.get(className) match {
                case Some(existing) if existing.jar != moduleJar.jar.getFileName.toString =>
                  throw new PuddingException(
                    s"component class $className is registered in both " +
                      s"${existing.jar} and ${moduleJar.jar.getFileName}")
                case _ =>
                  origins += className ->
                    Origin(moduleJar.module, moduleJar.jar.getFileName.toString)
              }
            }
          }
        }
      } finally {
        jarFile.close()
      }
    }
    origins.toMap
  }

  private def readLines(input: InputStream): Seq[String] = {
    scala.io.Source.fromInputStream(input, StandardCharsets.UTF_8.name())
      .getLines()
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .toVector
  }

  private def components(
      loader: ClassLoader,
      origins: Map[String, Origin]): Seq[CapabilityComponent] = {
    val sources = this.services(classOf[DataSource], loader).map { source =>
      this.component(
        CapabilityKind.Source,
        source.identifier,
        source.getClass,
        ComponentCapabilities(
          connectionTypes = source.supportedConnectionTypes.toSeq.sorted,
          catalogTypes = source.supportedCatalogTypes.toSeq.sorted,
          syncIncrementalSemantics = source match {
            case incremental: IncrementalSource =>
              incremental.supportedIncrementalSemantics.map(_.name).toSeq.sorted
            case _ => Seq.empty
          },
          executionModes = source.supportedExecutionModes.map(_.name).toSeq.sorted,
          lakeSyncIncremental = source.supportsLakeIncremental),
        origins)
    }
    val sinks = this.services(classOf[DataSink], loader).map { sink =>
      this.component(
        CapabilityKind.Sink,
        sink.identifier,
        sink.getClass,
        ComponentCapabilities(
          connectionTypes = sink.supportedConnectionTypes.toSeq.sorted,
          catalogTypes = sink.supportedCatalogTypes.toSeq.sorted,
          syncIncrementalSemantics =
            sink.supportedIncrementalSemantics.map(_.name).toSeq.sorted,
          idempotentSyncIncrementalSemantics = sink.supportedIncrementalSemantics
            .filter(sink.isIdempotentIncremental)
            .map(_.name).toSeq.sorted,
          syncIncrementalFencing = sink.supportsIncrementalFencing,
          executionModes = sink.supportedExecutionModes.map(_.name).toSeq.sorted,
          streamingOutputModes = sink.supportedStreamingOutputModes.toSeq.sorted,
          streamingUpsert = sink.supportsStreamingUpsert,
          syncFullRefresh = sink.supportsFullRefresh,
          syncAtomicReplace = sink.supportsAtomicReplace,
          lakeSyncIncremental = sink.supportsLakeIncremental),
        origins)
    }
    val transforms = this.services(classOf[DataTransform], loader).map { transform =>
      this.component(
        CapabilityKind.Transform,
        transform.identifier,
        transform.getClass,
        ComponentCapabilities(
          executionModes = transform.supportedExecutionModes.map(_.name).toSeq.sorted,
          preservesSyncIncrementalMetadata = transform.preservesIncrementalMetadata),
        origins)
    }
    val connections = this.services(classOf[ConnectionProvider], loader).map { provider =>
      this.component(
        CapabilityKind.Connection,
        provider.identifier,
        provider.getClass,
        ComponentCapabilities(),
        origins)
    }
    val catalogs = this.services(classOf[CatalogProvider], loader).map { provider =>
      this.component(
        CapabilityKind.Catalog,
        provider.identifier,
        provider.getClass,
        ComponentCapabilities(),
        origins)
    }
    sources ++ sinks ++ transforms ++ connections ++ catalogs
  }

  private def component(
      kind: String,
      componentType: String,
      componentClass: Class[_],
      capabilities: ComponentCapabilities,
      origins: Map[String, Origin]): CapabilityComponent = {
    val origin = origins.getOrElse(componentClass.getName,
      throw new PuddingException(
        s"can not determine the jar for component ${componentClass.getName}"))
    CapabilityComponent(
      kind = kind,
      `type` = componentType,
      className = componentClass.getName,
      module = origin.module,
      jar = origin.jar,
      capabilities = capabilities)
  }

  private def services[A](
      serviceClass: Class[A],
      loader: ClassLoader): Seq[A] = {
    try {
      ServiceLoader.load(serviceClass, loader).iterator().asScala.toVector
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"failed to load ${serviceClass.getName} implementations: " +
            Option(error.getMessage).getOrElse(error.getClass.getSimpleName),
          error)
    }
  }
}
