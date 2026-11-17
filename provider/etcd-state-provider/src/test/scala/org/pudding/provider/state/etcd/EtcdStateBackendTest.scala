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

package org.pudding.provider.state.etcd

import java.time.Instant
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException
import org.pudding.core.definition.{
  IncrementalSemantics,
  IncrementalSpec,
  IncrementalStateConf
}
import org.pudding.core.incremental.{
  ContainerFactory,
  ContainerTestSupport,
  IncrementalPlanDescriptor,
  IncrementalState,
  IncrementalStateBackendProvider,
  IncrementalStateStore
}

/**
 * Integration tests for the etcd backend. Set `PUDDING_TEST_ETCD_ENDPOINT` (for example
 * `http://127.0.0.1:2379`) or `PUDDING_TESTCONTAINERS=true` to run them.
 */
class EtcdStateBackendTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var container: Option[GenericContainer[_]] = None

  private var endpoint = ""

  override def beforeAll(): Unit = {
    sys.env.get("PUDDING_TEST_ETCD_ENDPOINT").map(_.trim).filter(_.nonEmpty) match {
      case Some(value) => this.endpoint = value
      case None if ContainerTestSupport.enabled =>
        this.container = ContainerTestSupport.start("etcd") {
          ContainerFactory.container("quay.io/coreos/etcd:v3.5.15")
            .withExposedPorts(2379)
            .withCommand(
              "/usr/local/bin/etcd",
              "--advertise-client-urls",
              "http://0.0.0.0:2379",
              "--listen-client-urls",
              "http://0.0.0.0:2379")
            .waitingFor(Wait.forHttp("/version").forStatusCode(200))
        }
        this.container.foreach { value =>
          this.endpoint = s"http://${value.getHost}:${value.getMappedPort(2379)}"
        }
      case None =>
    }
  }

  override def afterAll(): Unit = {
    this.container.foreach(_.stop())
  }

  private def enabled: Boolean = this.endpoint.nonEmpty

  private val plan = IncrementalPlanDescriptor.create(
    sourceType = "test-source",
    sourceId = "source",
    sinkType = "test-sink",
    sinkId = "sink",
    semantics = IncrementalSemantics.Upsert,
    keyFields = Seq("id"),
    opColumn = "__op",
    sourceConfig = None,
    sinkConfig = None,
    stateBackend = "etcd")

  private def prefix(): String =
    "pudding/test/" + UUID.randomUUID().toString.replace("-", "") + "/"

  private def backend(prefix: String, ttlMillis: Long = 0L): EtcdStateBackend =
    new EtcdStateBackend(Map(
      "endpoint" -> this.endpoint,
      "prefix" -> prefix,
      "lockTtlMs" -> ttlMillis.toString,
      "timeoutMs" -> "5000"))

  test("register the provider through the service loader") {
    IncrementalStateBackendProvider.providers().keySet should contain ("etcd")
  }

  test("validate backend options before connecting") {
    val prefix = intercept[PuddingException] {
      new EtcdStateBackend(Map("prefix" -> "中文/"))
    }
    prefix.getMessage should include ("prefix")
    val timeout = intercept[PuddingException] {
      new EtcdStateBackend(Map("timeoutMs" -> "soon"))
    }
    timeout.getMessage should include ("timeoutMs")
    val ttl = intercept[PuddingException] {
      new EtcdStateBackend(Map("lockTtlMs" -> "forever"))
    }
    ttl.getMessage should include ("lockTtlMs")
    intercept[PuddingException] {
      new EtcdStateBackend(Map("timeoutMs" -> "0"))
    }.getMessage should include ("timeoutMs")
    intercept[PuddingException] {
      new EtcdStateBackend(Map("endpoint" -> ",,"))
    }.getMessage should include ("at least one")
    val credentials = intercept[PuddingException] {
      new EtcdStateBackend(Map("endpoint" -> "http://user:secret@localhost:2379"))
    }
    credentials.getMessage should include ("must not contain credentials")
  }

  test("select etcd through an incremental state configuration") {
    assume(this.enabled, "etcd endpoint or PUDDING_TESTCONTAINERS is not configured")
    val spec = IncrementalSpec(
      semantics = IncrementalSemantics.Append,
      state = Some(IncrementalStateConf(
        "etcd",
        Map(
          "endpoint" -> this.endpoint,
          "prefix" -> this.prefix(),
          "lockTtlMs" -> "60000"))))
    val store = IncrementalStateStore.open(spec, new Configuration())
    store.location should startWith ("etcd://")
    store.acquire("source")
    try {
      store.begin("source", None, "1", "txn-config", this.plan)
      store.commit("source", "txn-config")
      store.load("source").committedCursor shouldBe Some("1")
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("persist, commit, reset and list state") {
    assume(this.enabled, "etcd endpoint or PUDDING_TESTCONTAINERS is not configured")
    val store = new IncrementalStateStore(this.backend(this.prefix()))
    store.acquire("source")
    try {
      store.load("source").isEmpty shouldBe true
      store.begin("source", None, "10", "txn-1", this.plan)
      store.load("source").pending.map(_.toCursor) shouldBe Some("10")
      store.commit("source", "txn-1")
      store.load("source").committedCursor shouldBe Some("10")
      store.listSourceIds() shouldBe Seq("source")

      store.reset("source", Some("9"))
      store.load("source").committedCursor shouldBe Some("9")
      store.load("source").pending shouldBe None
    } finally {
      store.release("source") shouldBe true
    }
  }

  test("allow only one writer and take over an expired lock") {
    assume(this.enabled, "etcd endpoint or PUDDING_TESTCONTAINERS is not configured")
    val prefix = this.prefix()
    val first = new IncrementalStateStore(this.backend(prefix, 1000L))
    val second = new IncrementalStateStore(this.backend(prefix, 1000L))

    first.acquire("source")
    first.begin("source", None, "10", "txn-1", this.plan)
    first.commit("source", "txn-1")
    intercept[PuddingException] {
      second.acquire("source")
    }.getMessage should include ("locked")
    first.close()

    Thread.sleep(1200L)
    val info = second.acquire("source")
    info.fencingToken shouldBe 2L
    try {
      second.load("source").committedCursor shouldBe Some("10")
    } finally {
      second.release("source") shouldBe true
    }
  }

  test("condition state writes on the lock token") {
    assume(this.enabled, "etcd endpoint or PUDDING_TESTCONTAINERS is not configured")
    val backend = new EtcdStateBackend(Map(
      "endpoint" -> this.endpoint,
      "prefix" -> this.prefix())) {
      def currentToken(sourceId: String): String =
        readLock(sourceId).map(_.token).getOrElse("")
      def writeState(state: IncrementalState, token: String): Unit =
        persistState(state, token)
    }
    backend.acquire("source")
    try {
      val token = backend.currentToken("source")
      token should not be empty
      val state = IncrementalState(
        sourceId = "source",
        committedCursor = Some("1"),
        pending = None,
        updatedAt = Instant.now().toString)
      val error = intercept[PuddingException] {
        backend.writeState(state, "replaced-token")
      }
      error.getMessage should include ("lost")
      backend.writeState(state, token)
      backend.load("source").committedCursor shouldBe Some("1")
    } finally {
      backend.release("source") shouldBe true
    }
  }

  test("renew the lock lease while the writer is active") {
    assume(this.enabled, "etcd endpoint or PUDDING_TESTCONTAINERS is not configured")
    val prefix = this.prefix()
    val first = this.backend(prefix, 2000L)
    first.acquire("source")
    try {
      first.lockInfo("source").get.expiresAt should not be empty
      Thread.sleep(3000L)
      intercept[PuddingException] {
        this.backend(prefix, 2000L).acquire("source")
      }.getMessage should include ("locked")
      first.ensureLock("source")
    } finally {
      first.release("source") shouldBe true
    }
  }

  test("clear state and release the lock") {
    assume(this.enabled, "etcd endpoint or PUDDING_TESTCONTAINERS is not configured")
    val store = new IncrementalStateStore(this.backend(this.prefix()))
    store.acquire("source")
    store.begin("source", None, "10", "txn-1", this.plan)
    store.commit("source", "txn-1")
    store.clear("source")
    store.load("source").isEmpty shouldBe true
    store.listSourceIds() shouldBe empty
    store.release("source") shouldBe true
  }
}
