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

package org.pudding.provider.state.redis

import java.time.Instant
import java.util.UUID

import redis.clients.jedis.Jedis
import org.apache.hadoop.conf.Configuration
import org.testcontainers.containers.GenericContainer
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
  IncrementalStateCodec,
  IncrementalStateStore
}

/**
 * Integration tests for the Redis backend. Set `PUDDING_TEST_REDIS_HOST` (and optionally
 * `PUDDING_TEST_REDIS_PORT`) or `PUDDING_TESTCONTAINERS=true` to run them.
 */
class RedisStateBackendTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var container: Option[GenericContainer[_]] = None

  private var host = ""

  private var port = ""

  override def beforeAll(): Unit = {
    sys.env.get("PUDDING_TEST_REDIS_HOST").map(_.trim).filter(_.nonEmpty) match {
      case Some(value) =>
        this.host = value
        this.port = sys.env.getOrElse("PUDDING_TEST_REDIS_PORT", "6379")
      case None if ContainerTestSupport.enabled =>
        this.container = ContainerTestSupport.start("redis") {
          ContainerFactory.container("redis:7-alpine").withExposedPorts(6379)
        }
        this.container.foreach { value =>
          this.host = value.getHost
          this.port = value.getMappedPort(6379).toString
        }
      case None =>
    }
  }

  override def afterAll(): Unit = {
    this.container.foreach(_.stop())
  }

  private def enabled: Boolean = this.host.nonEmpty

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
    stateBackend = "redis")

  private def prefix(): String =
    "pudding:test:" + UUID.randomUUID().toString.replace("-", "") + ":"

  private def exerciseStore(store: IncrementalStateStore): Unit = {
    store.acquire("source")
    try {
      store.begin("source", None, "10", "txn-deployment", this.plan)
      store.commit("source", "txn-deployment")
      store.load("source").committedCursor shouldBe Some("10")
      store.listSourceIds() shouldBe Seq("source")
    } finally {
      store.release("source") shouldBe true
    }
  }

  private def backend(prefix: String, ttlMillis: Long = 0L): RedisStateBackend =
    new RedisStateBackend(Map(
      "host" -> host,
      "port" -> port,
      "prefix" -> prefix,
      "lockTtlMs" -> ttlMillis.toString,
      "timeoutMs" -> "5000"))

  test("register the provider through the service loader") {
    IncrementalStateBackendProvider.providers().keySet should contain ("redis")
  }

  test("validate backend options before connecting") {
    val missing = intercept[PuddingException] {
      new RedisStateBackend(Map.empty)
    }
    missing.getMessage should include ("host")
    val ttl = intercept[PuddingException] {
      new RedisStateBackend(Map("host" -> "localhost", "lockTtlMs" -> "-1"))
    }
    ttl.getMessage should include ("lockTtlMs")
    intercept[PuddingException] {
      new RedisStateBackend(Map("host" -> "localhost", "timeoutMs" -> "0"))
    }.getMessage should include ("timeoutMs")
    intercept[PuddingException] {
      new RedisStateBackend(Map("host" -> "localhost", "port" -> "0"))
    }.getMessage should include ("port")
    intercept[PuddingException] {
      new RedisStateBackend(Map("mode" -> "unknown", "host" -> "localhost"))
    }.getMessage should include ("unsupported redis state mode")
    intercept[PuddingException] {
      new RedisStateBackend(Map("mode" -> "sentinel", "sentinels" -> "localhost:26379"))
    }.getMessage should include ("masterName")
    intercept[PuddingException] {
      new RedisStateBackend(Map("mode" -> "sentinel", "masterName" -> "master"))
    }.getMessage should include ("sentinels")
    intercept[PuddingException] {
      new RedisStateBackend(Map(
        "mode" -> "cluster",
        "clusterNodes" -> "localhost:6379",
        "database" -> "1"))
    }.getMessage should include ("non-zero database")
    intercept[PuddingException] {
      new RedisStateBackend(Map("host" -> "localhost", "prefix" -> "bad{prefix"))
    }.getMessage should include ("hash tag")
    intercept[PuddingException] {
      new RedisStateBackend(Map("host" -> "user:secret@localhost"))
    }.getMessage should include ("without credentials")
  }

  test("select Redis through an incremental state configuration") {
    assume(this.enabled, "Redis endpoint or PUDDING_TESTCONTAINERS is not configured")
    val spec = IncrementalSpec(
      semantics = IncrementalSemantics.Append,
      state = Some(IncrementalStateConf(
        "redis",
        Map(
          "host" -> this.host,
          "port" -> this.port,
          "prefix" -> this.prefix(),
          "lockTtlMs" -> "60000"))))
    val store = IncrementalStateStore.open(spec, new Configuration())
    store.location should startWith ("redis://")
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
    assume(this.enabled, "Redis endpoint or PUDDING_TESTCONTAINERS is not configured")
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

  test("allow only one writer and take over after expiry") {
    assume(this.enabled, "Redis endpoint or PUDDING_TESTCONTAINERS is not configured")
    val prefix = this.prefix()
    val first = new IncrementalStateStore(this.backend(prefix, 60000L))
    val second = new IncrementalStateStore(this.backend(prefix, 60000L))

    first.acquire("source")
    first.begin("source", None, "10", "txn-1", this.plan)
    first.commit("source", "txn-1")
    val error = intercept[PuddingException] {
      second.acquire("source")
    }
    error.getMessage should include ("locked")
    first.close()

    // Simulate the lock key expiring after the first process disappeared.
    val jedis = new Jedis(this.host, this.port.toInt)
    try {
      val tag = IncrementalStateCodec.sha256("source").take(16)
      jedis.del(prefix + s"{h$tag}:source:lock")
    } finally {
      jedis.close()
    }

    val info = second.acquire("source")
    info.fencingToken shouldBe 2L
    try {
      second.load("source").committedCursor shouldBe Some("10")
    } finally {
      second.release("source") shouldBe true
    }
  }

  test("condition state writes on the lock token") {
    assume(this.enabled, "Redis endpoint or PUDDING_TESTCONTAINERS is not configured")
    val backend = new RedisStateBackend(Map(
      "host" -> this.host,
      "port" -> this.port,
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
    assume(this.enabled, "Redis endpoint or PUDDING_TESTCONTAINERS is not configured")
    val prefix = this.prefix()
    val first = this.backend(prefix, 2000L)
    first.acquire("source")
    try {
      first.lockInfo("source").get.expiresAt should not be empty
      Thread.sleep(3000L)
      val second = this.backend(prefix, 2000L)
      val error = intercept[PuddingException] {
        second.acquire("source")
      }
      error.getMessage should include ("locked")
      first.ensureLock("source")
    } finally {
      first.release("source") shouldBe true
    }
  }

  test("use a Redis Sentinel deployment when configured") {
    val sentinels = sys.env.get("PUDDING_TEST_REDIS_SENTINEL_ENDPOINTS")
      .map(_.trim).filter(_.nonEmpty)
    val master = sys.env.get("PUDDING_TEST_REDIS_SENTINEL_MASTER")
      .map(_.trim).filter(_.nonEmpty)
    assume(sentinels.isDefined && master.isDefined,
      "Redis Sentinel endpoint or master name is not configured")
    val store = new IncrementalStateStore(new RedisStateBackend(Map(
      "mode" -> "sentinel",
      "sentinels" -> sentinels.get,
      "masterName" -> master.get,
      "prefix" -> this.prefix(),
      "lockTtlMs" -> "60000",
      "timeoutMs" -> "5000")))
    this.exerciseStore(store)
  }

  test("use a Redis Cluster deployment when configured") {
    val nodes = sys.env.get("PUDDING_TEST_REDIS_CLUSTER_NODES")
      .map(_.trim).filter(_.nonEmpty)
    assume(nodes.isDefined, "Redis Cluster nodes are not configured")
    val store = new IncrementalStateStore(new RedisStateBackend(Map(
      "mode" -> "cluster",
      "clusterNodes" -> nodes.get,
      "prefix" -> this.prefix(),
      "lockTtlMs" -> "60000",
      "timeoutMs" -> "5000")))
    this.exerciseStore(store)
  }

  test("clear state and release the lock") {
    assume(this.enabled, "Redis endpoint or PUDDING_TESTCONTAINERS is not configured")
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
