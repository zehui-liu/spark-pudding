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
import java.util.jar.{JarEntry, JarOutputStream}

import scala.collection.JavaConverters._

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for manifest parsing, strict loading, fragment merging and capability diffs.
 */
class CapabilityManifestTest extends AnyFunSuite with Matchers {

  private val FilterComponent =
    """
      |  - kind: transform
      |    type: filter-transform
      |    className: org.pudding.transform.FilterTransform
      |    module: basic-transform
      |    jar: basic-transform-1.0.0.jar
      |    capabilities:
      |      executionModes: [batch, streaming]
      |      preservesSyncIncrementalMetadata: true
      |""".stripMargin

  private val FileSourceComponent =
    """
      |  - kind: source
      |    type: file-common-source
      |    className: org.pudding.source.FileSource
      |    module: file-common-source
      |    jar: file-common-source-1.0.0.jar
      |    capabilities:
      |      connectionTypes: []
      |      catalogTypes: []
      |      executionModes: [batch, streaming]
      |      syncIncrementalSemantics: [append]
      |      lakeSyncIncremental: true
      |""".stripMargin

  private val HudiSinkComponent =
    """
      |  - kind: sink
      |    type: hudi-sink
      |    className: org.pudding.sink.HudiSink
      |    module: hudi-sink
      |    jar: hudi-sink-1.0.0.jar
      |    capabilities:
      |      executionModes: [batch, streaming]
      |      syncFullRefresh: true
      |      syncAtomicReplace: true
      |      syncIncrementalSemantics: [append, cdc, upsert]
      |      idempotentSyncIncrementalSemantics: [append, cdc, upsert]
      |      syncIncrementalFencing: true
      |      lakeSyncIncremental: true
      |""".stripMargin

  private def rootManifest(components: String): String = {
    s"""
       |schemaVersion: "2.0"
       |productVersion: "1.0.0"
       |sparkVersion: "3.4.0"
       |scalaBinaryVersion: "2.12"
       |generatedAt: "2026-10-25T00:00:00Z"
       |components:
       |$components
       |""".stripMargin
  }

  private def withDistribution(testBody: Path => Unit): Unit = {
    val root = Files.createTempDirectory("pudding-capability-test")
    try {
      Files.createDirectories(root.resolve("libs"))
      testBody(root)
    } finally {
      val stream = Files.walk(root)
      try stream.iterator().asScala.toVector.reverse.foreach(path => Files.deleteIfExists(path))
      finally stream.close()
    }
  }

  private def writeManifest(root: Path, text: String): Unit = {
    Files.write(
      root.resolve(CapabilityManifest.RootFileName),
      text.getBytes(StandardCharsets.UTF_8))
  }

  private def createJar(path: Path, fragment: Option[String] = None): Unit = {
    val output = new JarOutputStream(Files.newOutputStream(path))
    try {
      val manifest = new JarEntry("META-INF/MANIFEST.MF")
      output.putNextEntry(manifest)
      output.write("Manifest-Version: 1.0\n".getBytes(StandardCharsets.UTF_8))
      output.closeEntry()
      fragment.foreach { text =>
        val entry = new JarEntry(CapabilityManifest.FragmentPath)
        output.putNextEntry(entry)
        output.write(text.getBytes(StandardCharsets.UTF_8))
        output.closeEntry()
      }
    } finally {
      output.close()
    }
  }

  test("parse and render a manifest") {
    val text = rootManifest(FileSourceComponent + HudiSinkComponent + FilterComponent)
    val manifest = CapabilityManifest.parseManifest(text, "<test>")

    manifest.schemaVersion shouldBe "2.0"
    manifest.productVersion shouldBe "1.0.0"
    manifest.components.map(_.key).toSet shouldBe
      Set("source:file-common-source", "sink:hudi-sink", "transform:filter-transform")
    manifest.capabilitySetHash.length shouldBe 64
    val sourceCapabilities = manifest.components
      .find(_.key == "source:file-common-source").get.capabilities
    sourceCapabilities.syncIncrementalSemantics shouldBe Seq("append")
    sourceCapabilities.executionModes shouldBe Seq("batch", "streaming")
    sourceCapabilities.lakeSyncIncremental shouldBe true
    val sinkCapabilities = manifest.components
      .find(_.key == "sink:hudi-sink").get.capabilities
    sinkCapabilities.syncIncrementalSemantics shouldBe Seq("append", "cdc", "upsert")
    sinkCapabilities.idempotentSyncIncrementalSemantics shouldBe Seq("append", "cdc", "upsert")
    sinkCapabilities.syncIncrementalFencing shouldBe true
    sinkCapabilities.syncFullRefresh shouldBe true
    sinkCapabilities.syncAtomicReplace shouldBe true
    manifest.components.find(_.key == "transform:filter-transform").get
      .capabilities.preservesSyncIncrementalMetadata shouldBe true

    val rendered = CapabilityManifestYaml.render(manifest)
    rendered should include ("schemaVersion:")
    rendered should include ("file-common-source")
    rendered should include ("syncIncrementalSemantics:")
    rendered should include ("idempotentSyncIncrementalSemantics:")
    rendered should include ("syncIncrementalFencing:")
    rendered should include ("syncAtomicReplace:")
    rendered should include ("executionModes:")
    rendered should include ("preservesSyncIncrementalMetadata:")
    val parsed = CapabilityManifest.parseManifest(rendered, "<rendered>")
    parsed.components.sortBy(_.key) shouldBe manifest.components.sortBy(_.key)
  }

  test("parse schema 2.1 streaming output capabilities") {
    val text =
      """
        |schemaVersion: "2.1"
        |productVersion: "1.0.0"
        |sparkVersion: "3.4.0"
        |scalaBinaryVersion: "2.12"
        |generatedAt: "2026-10-25T00:00:00Z"
        |components:
        |  - kind: sink
        |    type: iceberg-sink
        |    className: org.pudding.sink.IcebergSink
        |    module: iceberg-sink
        |    jar: iceberg-sink-1.0.0.jar
        |    capabilities:
        |      executionModes: [batch, streaming]
        |      streamingOutputModes: [append, update]
        |      streamingUpsert: true
        |""".stripMargin
    val manifest = CapabilityManifest.parseManifest(text, "<2.1>")
    manifest.schemaVersion shouldBe "2.1"
    val capabilities = manifest.components.head.capabilities
    capabilities.streamingOutputModes shouldBe Seq("append", "update")
    capabilities.streamingUpsert shouldBe true

    val rendered = CapabilityManifestYaml.render(manifest)
    rendered should include ("streamingOutputModes:")
    rendered should include ("streamingUpsert:")
    CapabilityManifest.parseManifest(rendered, "<2.1-rendered>") shouldBe manifest
  }

  test("hash is stable across component order and jar versions") {
    val first = CapabilityManifest.parseManifest(
      rootManifest(FileSourceComponent + FilterComponent), "<first>")
    val second = CapabilityManifest.parseManifest(
      rootManifest(FilterComponent + FileSourceComponent), "<second>")

    first.capabilitySetHash shouldBe second.capabilitySetHash

    val nextVersion = CapabilityManifest.parseManifest(
      rootManifest(FilterComponent + FileSourceComponent
        .replace("1.0.0", "1.0.1")), "<next-version>")
    nextVersion.capabilitySetHash shouldBe first.capabilitySetHash
  }

  test("reject a distribution without a root manifest by default") {
    withDistribution { root =>
      val error = intercept[PuddingException] {
        CapabilityManifest.load(root)
      }
      error.getMessage should include ("capability manifest is missing")
    }
  }

  test("reject duplicate components") {
    val error = intercept[PuddingException] {
      CapabilityManifest.parseManifest(
        rootManifest(FileSourceComponent + FileSourceComponent), "<duplicate>")
    }
    error.getMessage should include ("duplicate components")
  }

  test("reject the retired schema 1.0 without migration") {
    val text = rootManifest(FileSourceComponent).replace("2.0", "1.0")
    val error = intercept[PuddingException] {
      CapabilityManifest.parseManifest(text, "<schema>")
    }
    error.getMessage should include ("unsupported capability manifest schemaVersion")
    error.getMessage should include ("supported: 2.0")
  }

  test("reject retired schema 1.0 capability fields even with schemaVersion 2.0") {
    val text = rootManifest(FileSourceComponent)
      .replace("syncIncrementalSemantics", "incrementalSemantics")
    val error = intercept[PuddingException] {
      CapabilityManifest.parseManifest(text, "<retired-fields>")
    }
    error.getMessage should include ("retired schema 1.0 capability fields")
    error.getMessage should include ("incrementalSemantics")
  }

  test("merge a third-party fragment from a jar") {
    withDistribution { root =>
      writeManifest(root, rootManifest(FileSourceComponent))
      createJar(root.resolve("libs/file-common-source-1.0.0.jar"))
      val fragment =
        """
          |schemaVersion: "2.0"
          |components:
          |  - kind: source
          |    type: hbase-source
          |    className: com.example.HbaseSource
          |    module: hbase-source
          |    jar: hbase-source-1.0.jar
          |    capabilities:
          |      connectionTypes: [hbase]
          |""".stripMargin
      Files.createDirectories(root.resolve("libs/extensions"))
      createJar(root.resolve("libs/extensions/hbase-source-1.0.jar"), Some(fragment))

      val manifest = CapabilityManifest.verifyDistribution(root)

      manifest.components.map(_.key).toSet shouldBe
        Set("source:file-common-source", "source:hbase-source")
      manifest.components.find(_.key == "source:hbase-source").get
        .capabilities.connectionTypes shouldBe Seq("hbase")
    }
  }

  test("reject a fragment that duplicates a built-in component") {
    withDistribution { root =>
      writeManifest(root, rootManifest(FileSourceComponent))
      createJar(root.resolve("libs/file-common-source-1.0.0.jar"))
      val fragment =
        """
          |schemaVersion: "2.0"
          |components:
          |  - kind: source
          |    type: file-common-source
          |    className: com.example.OtherFileSource
          |    module: other
          |    jar: file-common-source-1.0.0.jar
          |""".stripMargin
      createJar(root.resolve("libs/file-common-source-1.0.0.jar"), Some(fragment))

      val error = intercept[PuddingException] {
        CapabilityManifest.load(root)
      }
      error.getMessage should include ("defined more than once")
    }
  }

  test("reject a fragment that declares another jar") {
    withDistribution { root =>
      writeManifest(root, rootManifest(FileSourceComponent))
      createJar(root.resolve("libs/file-common-source-1.0.0.jar"))
      val fragment =
        """
          |schemaVersion: "2.0"
          |components:
          |  - kind: source
          |    type: hbase-source
          |    className: com.example.HbaseSource
          |    module: hbase-source
          |    jar: another.jar
          |""".stripMargin
      createJar(root.resolve("libs/hbase-source-1.0.jar"), Some(fragment))

      val error = intercept[PuddingException] {
        CapabilityManifest.load(root)
      }
      error.getMessage should include ("do not match the containing jar")
    }
  }

  test("verify that every component jar exists") {
    withDistribution { root =>
      writeManifest(root, rootManifest(FileSourceComponent))
      createJar(root.resolve("libs/file-common-source-1.0.0.jar"))

      CapabilityManifest.verifyDistribution(root).components.size shouldBe 1

      Files.delete(root.resolve("libs/file-common-source-1.0.0.jar"))
      val error = intercept[PuddingException] {
        CapabilityManifest.verifyDistribution(root)
      }
      error.getMessage should include ("missing jars")
    }
  }

  test("diff reports added, removed and changed components") {
    val previous = CapabilityManifest.parseManifest(
      rootManifest(FileSourceComponent + FilterComponent), "<previous>")
    val changedSource =
      """
        |  - kind: source
        |    type: file-common-source
        |    className: org.pudding.source.FileSource
        |    module: file-common-source
        |    jar: file-common-source-1.0.1.jar
        |    capabilities:
        |      connectionTypes: [local]
        |      catalogTypes: []
        |""".stripMargin
    val addedComponent =
      """
        |  - kind: sink
        |    type: console-sink
        |    className: org.pudding.sink.ConsoleSink
        |    module: console-sink
        |    jar: console-sink-1.0.1.jar
        |""".stripMargin
    val current = CapabilityManifest.parseManifest(
      rootManifest(changedSource + addedComponent), "<current>")

    val diff = CapabilityManifest.diff(previous, current)

    diff.added.map(_.key) shouldBe Seq("sink:console-sink")
    diff.removed.map(_.key) shouldBe Seq("transform:filter-transform")
    diff.changed.map(_.after.key) shouldBe Seq("source:file-common-source")
    diff.isEmpty shouldBe false
  }
}
