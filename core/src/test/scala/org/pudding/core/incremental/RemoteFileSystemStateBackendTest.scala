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

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException
import org.pudding.core.definition.IncrementalSemantics
import org.pudding.core.util.CheckpointTool

/**
 * Opt-in tests for HDFS and S3A state directories.
 *
 * Set `PUDDING_TEST_STATE_URI` to a writable base URI, for example
 * `hdfs://namenode:8020/tmp/pudding-test` or `s3a://bucket/pudding-test`. Set
 * `PUDDING_TEST_STATE_CONF` to a directory containing `core-site.xml`/`hdfs-site.xml` when the
 * deployment needs explicit endpoints or credentials. A unique subdirectory is created and
 * removed for every test.
 */
class RemoteFileSystemStateBackendTest extends AnyFunSuite with Matchers {

  private val enabled =
    sys.env.get("PUDDING_TEST_STATE_URI").exists(_.trim.nonEmpty)

  private val baseUri =
    sys.env.getOrElse("PUDDING_TEST_STATE_URI", "")

  private val plan = IncrementalPlanDescriptor.create(
    sourceType = "test-source",
    sourceId = "source",
    sinkType = "test-sink",
    sinkId = "sink",
    semantics = IncrementalSemantics.Append,
    keyFields = Seq.empty,
    opColumn = "__op",
    sourceConfig = None,
    sinkConfig = None,
    stateBackend = "filesystem")

  test("persist, commit and list state on a remote filesystem") {
    assume(this.enabled, "PUDDING_TEST_STATE_URI is not set")
    this.withRemotePath { path =>
      val store = this.store(path)
      store.acquire("source")
      try {
        store.begin("source", None, "10", "txn-1", this.plan)
        store.commit("source", "txn-1")
        store.load("source").committedCursor shouldBe Some("10")
        store.listSourceIds() shouldBe Seq("source")

        store.begin("source", Some("10"), "20", "txn-2", this.plan)
        store.commit("source", "txn-2")
        store.load("source").committedCursor shouldBe Some("20")
        store.reset("source", Some("15"))
        store.load("source").committedCursor shouldBe Some("15")
      } finally {
        store.release("source") shouldBe true
      }
    }
  }

  test("recover a crashed writer on a remote filesystem after an explicit unlock") {
    assume(this.enabled, "PUDDING_TEST_STATE_URI is not set")
    this.withRemotePath { path =>
      val first = this.store(path)
      first.acquire("source")
      first.begin("source", None, "42", "txn-remote", this.plan)
      first.close()

      val second = this.store(path)
      intercept[PuddingException] {
        second.acquire("source")
      }.getMessage should include ("locked")

      second.forceUnlock("source") shouldBe true
      val info = second.acquire("source")
      info.fencingToken shouldBe 2L
      try {
        second.load("source").pending.map(_.transactionId) shouldBe Some("txn-remote")
        second.commit("source", "txn-remote")
        second.load("source").committedCursor shouldBe Some("42")
      } finally {
        second.release("source") shouldBe true
      }
    }
  }

  test("clear state on a remote filesystem") {
    assume(this.enabled, "PUDDING_TEST_STATE_URI is not set")
    this.withRemotePath { path =>
      val store = this.store(path)
      store.acquire("source")
      store.begin("source", None, "10", "txn-1", this.plan)
      store.commit("source", "txn-1")
      store.clear("source")
      store.load("source").isEmpty shouldBe true
      store.listSourceIds() shouldBe empty
      store.release("source") shouldBe true
    }
  }

  test("inspect and delete a checkpoint on a remote filesystem") {
    assume(this.enabled, "PUDDING_TEST_STATE_URI is not set")
    this.withRemotePath { path =>
      val fileSystem = path.getFileSystem(this.configuration())
      fileSystem.mkdirs(new Path(path, "commits"))
      val commit = fileSystem.create(new Path(path, "commits/1"))
      try commit.write("batch".getBytes(StandardCharsets.UTF_8))
      finally commit.close()
      fileSystem.mkdirs(new Path(path, "offsets"))
      val metadata = fileSystem.create(new Path(path, "metadata"))
      try metadata.write("{}".getBytes(StandardCharsets.UTF_8))
      finally metadata.close()

      val out = new ByteArrayOutputStream()
      val err = new ByteArrayOutputStream()
      val outStream = new PrintStream(out, true, StandardCharsets.UTF_8.name())
      val errStream = new PrintStream(err, true, StandardCharsets.UTF_8.name())
      val hadoopConf = sys.env.get("PUDDING_TEST_STATE_CONF")
        .map(_.trim).filter(_.nonEmpty)
        .map(value => Array("--hadoop-conf", value))
        .getOrElse(Array.empty[String])
      try {
        val show = CheckpointTool.run(
          Array("show", path.toString) ++ hadoopConf, outStream, errStream)
        show shouldBe 0
        out.toString(StandardCharsets.UTF_8.name()) should include ("checkpoint: true")

        out.reset()
        val status = CheckpointTool.run(
          Array("status", path.toString, "--json") ++ hadoopConf, outStream, errStream)
        status shouldBe 0
        val json = out.toString(StandardCharsets.UTF_8.name())
        json should include ("\"checkpoint\":true")
        json should include ("\"commits\":1")

        val deleted = CheckpointTool.run(
          Array("delete", path.toString) ++ hadoopConf, outStream, errStream)
        deleted shouldBe 0
        fileSystem.exists(path) shouldBe false
      } finally {
        outStream.close()
        errStream.close()
      }
    }
  }

  private def store(path: Path): IncrementalStateStore = {
    new IncrementalStateStore(
      new FileSystemStateBackend(path.toString, this.configuration()))
  }

  private def configuration(): Configuration = {
    val conf = new Configuration()
    sys.env.get("PUDDING_TEST_STATE_CONF")
      .map(_.trim).filter(_.nonEmpty).foreach { value =>
        val directory = new File(value)
        Seq("core-site.xml", "hdfs-site.xml", "s3a-site.xml")
          .map(name => new File(directory, name))
          .filter(_.isFile)
          .foreach(file => conf.addResource(file.toURI.toURL))
      }
    conf
  }

  private def withRemotePath(body: Path => Unit): Unit = {
    val base = new Path(this.baseUri)
    val path = new Path(base, "run-" + UUID.randomUUID().toString.replace("-", ""))
    val fileSystem = base.getFileSystem(this.configuration())
    try {
      body(path)
    } finally {
      Try(fileSystem.delete(path, true))
      Try(fileSystem.close())
    }
  }
}
