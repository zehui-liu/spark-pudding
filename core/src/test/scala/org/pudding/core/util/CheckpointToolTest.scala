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

package org.pudding.core.util

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the checkpoint maintenance CLI.
 */
class CheckpointToolTest extends AnyFunSuite with Matchers {

  private final case class Result(code: Int, out: String, err: String)

  private def run(args: String*): Result = {
    val out = new ByteArrayOutputStream()
    val err = new ByteArrayOutputStream()
    val code = CheckpointTool.run(
      args.toArray,
      new PrintStream(out, true, StandardCharsets.UTF_8.name()),
      new PrintStream(err, true, StandardCharsets.UTF_8.name()))
    Result(
      code,
      out.toString(StandardCharsets.UTF_8.name()),
      err.toString(StandardCharsets.UTF_8.name()))
  }

  private def checkpoint(root: Path, name: String): Path = {
    val path = Files.createDirectories(root.resolve(name))
    Files.createDirectories(path.resolve("commits"))
    Files.write(path.resolve("commits/1"), "data".getBytes(StandardCharsets.UTF_8))
    Files.createDirectories(path.resolve("offsets"))
    Files.write(path.resolve("offsets/1"), "{}".getBytes(StandardCharsets.UTF_8))
    Files.write(path.resolve("metadata"), "{}".getBytes(StandardCharsets.UTF_8))
    path
  }

  private def deleteTree(root: Path): Unit = {
    val stream = Files.walk(root)
    try stream.iterator().asScala.toVector.reverse.foreach(path => Files.deleteIfExists(path))
    finally stream.close()
  }

  private def setTreeModified(root: Path, time: FileTime): Unit = {
    val stream = Files.walk(root)
    try {
      stream.iterator().asScala.toVector.reverse
        .foreach(path => Files.setLastModifiedTime(path, time))
    } finally stream.close()
  }

  test("show a checkpoint") {
    val root = Files.createTempDirectory("pudding-checkpoint-show")
    try {
      val path = checkpoint(root, "job-a")
      val result = run("show", path.toString)

      result.code shouldBe 0
      result.out should include ("exists: true")
      result.out should include ("type: directory")
      result.out should include ("checkpoint: true")
      result.out should include ("sizeBytes:")
    } finally {
      deleteTree(root)
    }
  }

  test("delete one checkpoint, with a dry run") {
    val root = Files.createTempDirectory("pudding-checkpoint-delete")
    try {
      val path = checkpoint(root, "job-a")

      val dryRun = run("delete", path.toString, "--dry-run")
      dryRun.code shouldBe 0
      dryRun.out should include ("would delete:")
      Files.exists(path) shouldBe true

      val deleted = run("delete", path.toString)
      deleted.code shouldBe 0
      deleted.out should include ("deleted:")
      Files.exists(path) shouldBe false
    } finally {
      deleteTree(root)
    }
  }

  test("refuse to delete a directory that is not a Spark checkpoint") {
    val root = Files.createTempDirectory("pudding-checkpoint-delete-noncheckpoint")
    try {
      val data = Files.createDirectories(root.resolve("warehouse"))
      Files.write(data.resolve("part.parquet"), "data".getBytes(StandardCharsets.UTF_8))

      val result = run("delete", data.toString)

      result.code shouldBe 1
      result.err should include ("not a Spark checkpoint")
      Files.exists(data.resolve("part.parquet")) shouldBe true
    } finally {
      deleteTree(root)
    }
  }

  test("cleanup ignores non-checkpoint directories under its root") {
    val root = Files.createTempDirectory("pudding-checkpoint-cleanup-safety")
    try {
      val oldCheckpoint = checkpoint(root, "old")
      val data = Files.createDirectories(root.resolve("warehouse"))
      Files.write(data.resolve("part.parquet"), "data".getBytes(StandardCharsets.UTF_8))
      val old = FileTime.fromMillis(System.currentTimeMillis() - 2L * 60L * 60L * 1000L)
      setTreeModified(oldCheckpoint, old)
      setTreeModified(data, old)

      val result = run("cleanup", root.toString, "--older-than", "1h")

      result.code shouldBe 0
      Files.exists(oldCheckpoint) shouldBe false
      Files.exists(data.resolve("part.parquet")) shouldBe true
    } finally {
      deleteTree(root)
    }
  }

  test("cleanup only removes checkpoints older than the threshold") {
    val root = Files.createTempDirectory("pudding-checkpoint-cleanup")
    try {
      val oldCheckpoint = checkpoint(root, "old")
      val newCheckpoint = checkpoint(root, "new")
      val old = FileTime.fromMillis(System.currentTimeMillis() - 2L * 60L * 60L * 1000L)
      setTreeModified(oldCheckpoint, old)

      val dryRun = run("cleanup", root.toString, "--older-than", "1h", "--dry-run")
      dryRun.code shouldBe 0
      dryRun.out should include (oldCheckpoint.toString)
      dryRun.out should include ("kept: 1")
      Files.exists(oldCheckpoint) shouldBe true

      val cleaned = run("cleanup", root.toString, "--older-than=1h")
      cleaned.code shouldBe 0
      cleaned.out should include ("deleted:")
      Files.exists(oldCheckpoint) shouldBe false
      Files.exists(newCheckpoint) shouldBe true
    } finally {
      deleteTree(root)
    }
  }

  test("keep a checkpoint whose root is old but whose latest file is recent") {
    val root = Files.createTempDirectory("pudding-checkpoint-active")
    try {
      val path = checkpoint(root, "active")
      val old = FileTime.fromMillis(System.currentTimeMillis() - 2L * 60L * 60L * 1000L)
      Files.setLastModifiedTime(path, old)
      Files.setLastModifiedTime(path.resolve("commits"), old)
      Files.setLastModifiedTime(path.resolve("offsets"), old)
      Files.setLastModifiedTime(path.resolve("metadata"), old)

      val result = run("cleanup", root.toString, "--older-than", "1h")

      result.code shouldBe 0
      result.out should include ("kept: 1")
      Files.exists(path) shouldBe true
    } finally {
      deleteTree(root)
    }
  }

  test("ignore directory modification times when computing checkpoint age") {
    val root = Files.createTempDirectory("pudding-checkpoint-directory-age")
    try {
      val path = checkpoint(root, "old")
      val old = FileTime.fromMillis(System.currentTimeMillis() - 2L * 60L * 60L * 1000L)
      setTreeModified(path, old)
      // Object stores report the current time for directories. Only file times are an
      // activity marker, so this checkpoint must still be considered expired.
      val now = FileTime.fromMillis(System.currentTimeMillis())
      Files.setLastModifiedTime(path, now)
      Files.setLastModifiedTime(path.resolve("commits"), now)
      Files.setLastModifiedTime(path.resolve("offsets"), now)

      val result = run("cleanup", root.toString, "--older-than", "1h")

      result.code shouldBe 0
      result.out should include ("deleted:")
      Files.exists(path) shouldBe false
    } finally {
      deleteTree(root)
    }
  }

  test("refuse a checkpoint as the cleanup root") {
    val root = Files.createTempDirectory("pudding-checkpoint-root")
    try {
      val path = checkpoint(root, "job-a")
      val result = run("cleanup", path.toString, "--older-than", "1h")

      result.code shouldBe 1
      result.err should include ("cleanup root is itself a checkpoint")
    } finally {
      deleteTree(root)
    }
  }

  test("status returns checkpoint age and commit counts as JSON") {
    val root = Files.createTempDirectory("pudding-checkpoint-status")
    try {
      val path = checkpoint(root, "job-a")
      val result = run("status", path.toString, "--json")

      result.code shouldBe 0
      result.out should include ("\"checkpoint\":true")
      result.out should include ("\"ageSeconds\":")
      result.out should include ("\"commits\":1")
      result.out should include ("\"lastCommit\":\"1\"")
    } finally {
      deleteTree(root)
    }
  }

  test("cleanup can retain only the newest checkpoint count") {
    val root = Files.createTempDirectory("pudding-checkpoint-keep-last")
    try {
      val old = checkpoint(root, "old")
      val middle = checkpoint(root, "middle")
      val recent = checkpoint(root, "recent")
      val now = System.currentTimeMillis()
      setTreeModified(old, FileTime.fromMillis(now - 3L * 60L * 60L * 1000L))
      setTreeModified(middle, FileTime.fromMillis(now - 2L * 60L * 60L * 1000L))
      setTreeModified(recent, FileTime.fromMillis(now - 1L * 60L * 60L * 1000L))

      val result = run("cleanup", root.toString, "--keep-last", "2")

      result.code shouldBe 0
      result.out should include ("kept: 2")
      Files.exists(old) shouldBe false
      Files.exists(middle) shouldBe true
      Files.exists(recent) shouldBe true
    } finally {
      deleteTree(root)
    }
  }

  test("load additional filesystem configuration from a properties file") {
    val root = Files.createTempDirectory("pudding-checkpoint-conf")
    try {
      val path = checkpoint(root, "job-a")
      val conf = root.resolve("checkpoint.properties")
      Files.write(
        conf,
        "fs.defaultFS=file:///\n".getBytes(StandardCharsets.UTF_8))
      val result = run(
        "show", path.toString, "--conf-file", conf.toString)

      result.code shouldBe 0
      result.out should include ("checkpoint: true")
    } finally {
      deleteTree(root)
    }
  }

  test("reject unsupported filesystem config keys and invalid directories") {
    val root = Files.createTempDirectory("pudding-checkpoint-invalid-conf")
    try {
      val conf = root.resolve("checkpoint.properties")
      Files.write(conf, "password=hidden\n".getBytes(StandardCharsets.UTF_8))
      val unsupported = run("show", root.toString, "--conf-file", conf.toString)
      unsupported.code shouldBe 2
      unsupported.err should include ("only accepts fs.* keys")

      val missing = run(
        "show", root.toString, "--hadoop-conf", root.resolve("missing").toString)
      missing.code shouldBe 2
      missing.err should include ("not a directory")
    } finally {
      deleteTree(root)
    }
  }

  test("reject invalid commands, durations and missing options") {
    run("cleanup", "/tmp", "--older-than", "7").code shouldBe 2
    run("cleanup", "/tmp", "--older-than", "0d").code shouldBe 2
    run("cleanup", "/tmp").code shouldBe 2
    run("delete").code shouldBe 2
    run("show", "/tmp", "--dry-run").code shouldBe 2
    run("unknown").code shouldBe 2
  }
}
