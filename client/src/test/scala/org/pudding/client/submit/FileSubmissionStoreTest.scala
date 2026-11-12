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

package org.pudding.client.submit

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileSystems, Files, LinkOption, Path}
import java.time.Instant

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Tests the atomic, owner-only file submission store.
 */
class FileSubmissionStoreTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val createdDirectories = scala.collection.mutable.ArrayBuffer.empty[Path]

  private val createdAt = Instant.parse("2026-10-23T10:00:00Z")

  private val posix =
    FileSystems.getDefault.supportedFileAttributeViews.contains("posix")

  override def afterEach(): Unit = {
    createdDirectories.reverse.foreach(deleteRecursively)
    createdDirectories.clear()
  }

  test("saves, finds and lists records in creation order") {
    val store = newStore()
    val running = record(
      "submission-1",
      ApplicationState.RUNNING,
      createdAt.plusSeconds(5),
      applicationId = "app-1")
    val failed = record(
      "submission-2",
      ApplicationState.FAILED,
      createdAt.plusSeconds(10),
      applicationId = "app-2",
      error = "driver failed")

    store.save(failed)
    store.save(running)

    store.find("submission-1").get shouldBe running
    store.find("submission-2").get shouldBe failed
    store.find("missing").isPresent shouldBe false
    store.list().asScala.map(_.getSubmissionId) shouldBe List("submission-1", "submission-2")
    store.list().asScala.head.getFinishedAt.isPresent shouldBe false
    store.list().asScala.last.getFinishedAt.isPresent shouldBe true

    val files = listFiles(store)
    files.map(_.getFileName.toString) shouldBe
      List("submission-1.properties", "submission-2.properties")
    if (posix) {
      Files.getPosixFilePermissions(store.getDirectory) shouldBe
        PosixFilePermissions.fromString("rwx------")
      files.foreach { file =>
        Files.getPosixFilePermissions(file) shouldBe
          PosixFilePermissions.fromString("rw-------")
      }
    }
  }

  test("overwrites a record atomically without temporary files") {
    val store = newStore()
    val pending = record("submission-3", ApplicationState.PENDING, createdAt)
    store.save(pending)
    store.save(pending.withStatus(
      ApplicationState.RUNNING, "app-3", createdAt.plusSeconds(5)))

    val loaded = store.find("submission-3").get
    loaded.getState shouldBe ApplicationState.RUNNING
    loaded.getApplicationId.get shouldBe "app-3"
    listFiles(store).map(_.getFileName.toString) shouldBe
      List("submission-3.properties")
    if (posix) {
      Files.getPosixFilePermissions(
        store.getDirectory.resolve("submission-3.properties")) shouldBe
        PosixFilePermissions.fromString("rw-------")
    }
  }

  test("round-trips Kubernetes job Secret metadata") {
    val store = newStore()
    val record = SubmissionRecord
      .builder("submission-secret", "kubernetes", ApplicationState.FINISHED, createdAt)
      .appName("store test")
      .master("k8s://https://cluster:443")
      .deployMode("cluster")
      .namespace("data-platform")
      .kubeconfig("/private/kubeconfig")
      .kubernetesJobSecret("pudding-job-submission-secret")
      .kubernetesJobSecretKey("pipeline.yaml")
      .kubernetesJobSecretOwned(true)
      .updatedAt(createdAt.plusSeconds(5))
      .build()

    store.save(record)

    val loaded = store.find("submission-secret").get
    loaded shouldBe record
    loaded.getKubernetesJobSecret.get shouldBe "pudding-job-submission-secret"
    loaded.getKubernetesJobSecretKey.get shouldBe "pipeline.yaml"
    loaded.isKubernetesJobSecretOwned shouldBe true
  }

  test("loads legacy records without Kubernetes job Secret metadata") {
    val store = newStore()
    val contents =
      "format.version=1\n" +
        "submission.id=submission-legacy\n" +
        "target=kubernetes\n" +
        "application.name=legacy app\n" +
        "master=k8s\\://https\\://cluster\\:443\n" +
        "deploy.mode=cluster\n" +
        "kubernetes.namespace=pudding\n" +
        "state=PENDING\n" +
        "created.at=2026-10-23T10:00:00Z\n"
    Files.write(
      store.getDirectory.resolve("submission-legacy.properties"),
      contents.getBytes(StandardCharsets.UTF_8))

    val loaded = store.find("submission-legacy").get

    loaded.getState shouldBe ApplicationState.PENDING
    loaded.getKubernetesJobSecret.isPresent shouldBe false
    loaded.getKubernetesJobSecretKey.isPresent shouldBe false
    loaded.isKubernetesJobSecretOwned shouldBe false
    store.list().asScala.map(_.getSubmissionId) shouldBe List("submission-legacy")
  }

  test("rejects invalid Kubernetes job Secret ownership values") {
    val store = newStore()
    val contents =
      "format.version=1\n" +
        "submission.id=submission-invalid\n" +
        "target=kubernetes\n" +
        "application.name=store test\n" +
        "state=PENDING\n" +
        "created.at=2026-10-23T10:00:00Z\n" +
        "kubernetes.job.secret=pudding-job-submission-invalid\n" +
        "kubernetes.job.secret.owned=sometimes\n"
    Files.write(
      store.getDirectory.resolve("submission-invalid.properties"),
      contents.getBytes(StandardCharsets.UTF_8))

    val error = intercept[SubmissionException] {
      store.find("submission-invalid")
    }
    error.getMessage should include ("kubernetes.job.secret.owned")
    error.getMessage should not include ("sometimes")
  }

  test("delete is idempotent") {
    val store = newStore()
    store.save(record("submission-4", ApplicationState.FINISHED, createdAt))

    store.delete("submission-4")
    store.find("submission-4").isPresent shouldBe false
    store.delete("submission-4")
  }

  test("rejects unsafe submission ids") {
    val store = newStore()

    an[SubmissionException] should be thrownBy store.find("../escape")
    an[SubmissionException] should be thrownBy store.delete("with/slash")
    an[SubmissionException] should be thrownBy store.find("")
  }

  test("corrupt records produce safe errors without file content") {
    val store = newStore()
    val file = store.getDirectory.resolve("corrupt.properties")
    Files.write(
      file,
      "format.version=1\nDO-NOT-LEAK\nsubmission.id=\\uZZZZ\n".getBytes(StandardCharsets.UTF_8))

    val findError = intercept[SubmissionException] {
      store.find("corrupt")
    }
    findError.getMessage should include ("corrupt.properties")
    findError.getMessage should not include ("DO-NOT-LEAK")

    val listError = intercept[SubmissionException] {
      store.list()
    }
    listError.getMessage should not include ("DO-NOT-LEAK")
  }

  test("rejects record files whose id does not match the file name") {
    val store = newStore()
    store.save(record("submission-5", ApplicationState.RUNNING, createdAt))
    Files.copy(
      store.getDirectory.resolve("submission-5.properties"),
      store.getDirectory.resolve("submission-6.properties"))

    val error = intercept[SubmissionException] {
      store.find("submission-6")
    }
    error.getMessage should include ("submission-6.properties")
  }

  test("rejects symbolic links inside the store") {
    assume(posix)
    val store = newStore()
    val outside = Files.createTempFile("pudding-store-outside-", ".properties")
    createdDirectories += outside
    Files.write(outside, "secret-content".getBytes(StandardCharsets.UTF_8))
    Files.createSymbolicLink(
      store.getDirectory.resolve("linked.properties"), outside)

    val error = intercept[SubmissionException] {
      store.find("linked")
    }
    error.getMessage should include ("symbolic links")
    error.getMessage should not include ("secret-content")
  }

  test("rejects a symbolic link as the store directory") {
    assume(posix)
    val base = newBaseDirectory()
    val target = Files.createDirectory(base.resolve("target"))
    val link = base.resolve("link")
    Files.createSymbolicLink(link, target)

    val error = intercept[SubmissionException] {
      new FileSubmissionStore(link)
    }
    error.getMessage should include ("symbolic link")
  }

  test("tightens permissions on an existing store directory") {
    assume(posix)
    val base = newBaseDirectory()
    val directory = Files.createDirectory(base.resolve("store"))
    Files.setPosixFilePermissions(
      directory, PosixFilePermissions.fromString("rwxr-xr-x"))

    val store = new FileSubmissionStore(directory)

    store.getDirectory shouldBe directory.toAbsolutePath.normalize()
    Files.getPosixFilePermissions(store.getDirectory) shouldBe
      PosixFilePermissions.fromString("rwx------")
  }

  private def newStore(): FileSubmissionStore = {
    val base = newBaseDirectory()
    new FileSubmissionStore(base.resolve("store"))
  }

  private def newBaseDirectory(): Path = {
    val base = Files.createTempDirectory("pudding-store-test-")
    createdDirectories += base
    base
  }

  private def record(
      id: String,
      state: ApplicationState,
      updatedAt: Instant,
      applicationId: String = null,
      error: String = null): SubmissionRecord = {
    SubmissionRecord
      .builder(id, "kubernetes", state, createdAt)
      .appName("store test")
      .master("k8s://https://cluster:443")
      .deployMode("cluster")
      .namespace("pudding")
      .kubeconfig("/private/kubeconfig")
      .applicationId(applicationId)
      .error(error)
      .updatedAt(updatedAt)
      .build()
  }

  private def listFiles(store: FileSubmissionStore): List[Path] = {
    val stream = Files.list(store.getDirectory)
    try {
      stream.iterator().asScala.toList.sortBy(_.getFileName.toString)
    } finally {
      stream.close()
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (path != null && Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toList
          .sortBy(_.getNameCount)
          .reverse
          .foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}
