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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import redis.clients.jedis.{
  DefaultJedisClientConfig,
  HostAndPort,
  Jedis,
  JedisCluster,
  JedisPool,
  JedisPoolConfig,
  JedisSentinelPool,
  ScanParams
}
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.util.Pool

import org.pudding.core.PuddingException
import org.pudding.core.incremental.{
  AbstractIncrementalStateBackend,
  IncrementalLockRecord,
  IncrementalState,
  IncrementalStateBackend,
  IncrementalStateBackendProvider,
  IncrementalStateCodec,
  LockDocument
}

/**
 * Redis state backend for single-node, Sentinel and Cluster deployments.
 *
 * Keys use a hash tag derived from the source id (`{h<hash>}:<source>:state` and `:lock`) so the
 * two keys of a source share a cluster slot. State writes, renewals and deletes are Lua scripts
 * that compare the private lock token inside Redis. This gives the same fencing behavior in
 * single-node, Sentinel and Cluster modes without relying on `WATCH`/`MULTI`, which JedisCluster
 * does not support.
 *
 * Options: `mode` (`single`, `sentinel` or `cluster`), `host`, `port` (6379), `masterName` and
 * `sentinels` for Sentinel, `clusterNodes` for Cluster, `user`, `password`, `database` (0),
 * `ssl` (false for single/cluster), `sentinelUser`/`sentinelPassword` (falling back to the data
 * credentials), `timeoutMs` (5000, positive), `prefix` (`pudding:incremental:`; must not contain
 * `{` or `}`), and `lockTtlMs` (0, disabled). The location shown by the state tool never
 * contains credentials.
 */
class RedisStateBackend(options: Map[String, String])
    extends AbstractIncrementalStateBackend(RedisStateBackend.lockTtlMillis(options)) {

  private val mode = this.options.get("mode")
    .map(_.trim.toLowerCase(java.util.Locale.ROOT)).filter(_.nonEmpty)
    .getOrElse("single")

  private val host = this.options.get("host").map(_.trim).filter(_.nonEmpty).map { value =>
    this.validateHost(value, "host")
    value
  }

  private val port = {
    val value = this.intOption("port", 6379)
    if (value <= 0 || value > 65535) {
      throw new PuddingException("port must be between 1 and 65535")
    }
    value
  }

  private val database = this.intOption("database", 0)

  private val timeoutMs = {
    val value = this.intOption("timeoutMs", 5000)
    if (value <= 0) {
      throw new PuddingException("timeoutMs must be positive")
    }
    value
  }

  private val user = this.options.get("user").map(_.trim).filter(_.nonEmpty)

  private val password = this.options.get("password")

  private val prefix = {
    val value = this.options.get("prefix")
      .map(_.trim).filter(_.nonEmpty).getOrElse("pudding:incremental:")
    if (value.contains('{') || value.contains('}')) {
      throw new PuddingException(
        "redis state prefix can not contain '{' or '}' because keys use a hash tag")
    }
    value
  }

  private val client: RedisClient = {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(4)
    poolConfig.setMaxIdle(2)
    poolConfig.setMinIdle(0)
    val clientConfig = this.clientConfig()
    this.mode match {
      case "single" =>
        val target = this.host.getOrElse {
          throw new PuddingException("redis incremental state backend requires option 'host'")
        }
        new PooledRedisClient(
          new JedisPool(poolConfig, new HostAndPort(target, this.port), clientConfig))
      case "sentinel" =>
        val master = this.required("masterName")
        val sentinels = this.hostPorts("sentinels", this.required("sentinels"))
          .map { case (nodeHost, nodePort) => new HostAndPort(nodeHost, nodePort) }
          .toSet.asJava
        new PooledRedisClient(new JedisSentinelPool(
          master,
          sentinels,
          poolConfig,
          this.clientConfig(),
          this.sentinelClientConfig()))
      case "cluster" =>
        if (this.database != 0) {
          throw new PuddingException("redis cluster does not support a non-zero database")
        }
        val nodes = this.hostPorts("clusterNodes", this.required("clusterNodes"))
          .map { case (nodeHost, nodePort) => new HostAndPort(nodeHost, nodePort) }
        new ClusterRedisClient(new JedisCluster(
          nodes.toSet.asJava,
          clientConfig,
          5,
          poolConfig))
      case other =>
        throw new PuddingException(
          s"unsupported redis state mode: $other, supported: single, sentinel, cluster")
    }
  }

  override def location: String = {
    this.mode match {
      case "single" => s"redis://${this.host.getOrElse("unknown")}:$port/$database"
      case "sentinel" =>
        val sentinels = this.safeNodeString("sentinels")
        s"redis-sentinel://${this.options.getOrElse("masterName", "unknown")}@$sentinels/$database"
      case "cluster" =>
        s"redis-cluster://${this.safeNodeString("clusterNodes")}"
      case _ => "redis://unknown"
    }
  }

  override protected def readState(sourceId: String): Option[IncrementalState] = {
    this.client.get(this.stateKey(sourceId)).map { text =>
      IncrementalStateCodec.decode(sourceId, text)
    }
  }

  override protected def persistState(state: IncrementalState, token: String): Unit = {
    val result = this.client.eval(
      RedisStateBackend.PersistStateScript,
      Seq(this.lockKey(state.sourceId), this.stateKey(state.sourceId)),
      Seq(token, IncrementalStateCodec.encode(state)))
    if (result != 1L) {
      throw this.lostLock(state.sourceId)
    }
  }

  override protected def readLock(sourceId: String): Option[IncrementalLockRecord] = {
    val key = this.lockKey(sourceId)
    this.client.get(key).map(text => LockDocument.decode(text, key).record)
  }

  override protected def createLock(
      sourceId: String,
      record: IncrementalLockRecord): Boolean = {
    val document = LockDocument(
      token = record.token,
      owner = record.info.owner,
      host = record.info.host,
      pid = record.info.pid,
      acquiredAt = record.info.acquiredAt,
      fencingToken = record.info.fencingToken,
      expiresAt = record.info.expiresAt)
    this.client.setNxPx(
      this.lockKey(sourceId),
      LockDocument.encode(document),
      this.lockTtlMillis)
  }

  override protected def renewLock(
      sourceId: String,
      token: String,
      expiresAt: Option[String]): Boolean = {
    if (this.lockTtlMillis <= 0L) {
      false
    } else {
      val key = this.lockKey(sourceId)
      this.client.get(key) match {
        case Some(raw) =>
          val document = LockDocument.decode(raw, key)
          if (document.token != token) {
            false
          } else {
            this.client.eval(
              RedisStateBackend.RenewLockScript,
              Seq(key),
              Seq(
                token,
                LockDocument.encode(document.copy(expiresAt = expiresAt)),
                this.lockTtlMillis.toString)) == 1L
          }
        case None => false
      }
    }
  }

  override protected def deleteLockIfToken(sourceId: String, token: String): Boolean = {
    this.client.eval(
      RedisStateBackend.DeleteLockScript,
      Seq(this.lockKey(sourceId)),
      Seq(token)) == 1L
  }

  override protected def deleteLock(sourceId: String): Boolean = {
    this.client.del(this.lockKey(sourceId)) > 0L
  }

  override protected def listBackendSourceIds(): Seq[String] = {
    val suffix = ":state"
    val sources = this.client.keysByPattern(this.prefix + "*" + suffix).flatMap { key =>
      if (!key.startsWith(this.prefix) || !key.endsWith(suffix)) {
        None
      } else {
        val close = key.indexOf('}', this.prefix.length)
        if (close < 0 || close + 1 >= key.length || key.charAt(close + 1) != ':') {
          None
        } else {
          val start = close + 2
          val end = key.length - suffix.length
          if (end > start) Some(key.substring(start, end)) else None
        }
      }
    }.distinct
    sources.filter { sourceId =>
      this.client.get(this.stateKey(sourceId)) match {
        case Some(text) =>
          try {
            !IncrementalStateCodec.decode(sourceId, text).isEmpty
          } catch {
            case NonFatal(_) => true
          }
        case None => false
      }
    }
  }

  override def close(): Unit = {
    try this.client.close()
    finally super.close()
  }

  private def asLong(value: Any): Long = value match {
    case number: java.lang.Long => number.longValue()
    case number: java.lang.Integer => number.longValue()
    case other =>
      throw new PuddingException(
        s"redis incremental state script returned " +
          Option(other).map(_.getClass.getSimpleName).getOrElse("null"))
  }

  private def scan(jedis: Jedis, pattern: String): Seq[String] = {
    val params = new ScanParams().`match`(pattern).count(500)
    val keys = new ListBuffer[String]()
    var cursor = ScanParams.SCAN_POINTER_START
    do {
      val page = jedis.scan(cursor, params)
      keys ++= page.getResult.asScala
      cursor = page.getCursor
    } while (cursor != ScanParams.SCAN_POINTER_START)
    keys.toSeq
  }

  private def clientConfig(): DefaultJedisClientConfig = {
    val builder = DefaultJedisClientConfig.builder()
      .connectionTimeoutMillis(this.timeoutMs)
      .socketTimeoutMillis(this.timeoutMs)
      .database(this.database)
      .ssl(this.booleanOption("ssl", default = false))
    this.user.foreach(builder.user)
    this.password.foreach(builder.password)
    builder.build()
  }

  /**
   * Sentinel connections use their own credentials because Sentinel may be protected by a
   * different ACL user or password than the data nodes. Fall back to the data credentials so a
   * deployment that shares one account keeps working.
   */
  private def sentinelClientConfig(): DefaultJedisClientConfig = {
    val builder = DefaultJedisClientConfig.builder()
      .connectionTimeoutMillis(this.timeoutMs)
      .socketTimeoutMillis(this.timeoutMs)
      .database(0)
      .ssl(this.booleanOption("ssl", default = false))
    this.options.get("sentinelUser").map(_.trim).filter(_.nonEmpty)
      .orElse(this.user).foreach(builder.user)
    this.options.get("sentinelPassword").filter(_.nonEmpty)
      .orElse(this.password).foreach(builder.password)
    builder.build()
  }

  private def stateKey(sourceId: String): String = this.keyBase(sourceId) + ":state"

  private def lockKey(sourceId: String): String = this.keyBase(sourceId) + ":lock"

  private def keyBase(sourceId: String): String = {
    val source = this.requireSourceId(sourceId)
    val tag = IncrementalStateCodec.sha256(source).take(16)
    s"$prefix{h$tag}:$source"
  }

  private def requireSourceId(sourceId: String): String = {
    Option(sourceId).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("source id can not be empty for incremental state")
    }
  }

  private def required(key: String): String = {
    this.options.get(key).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"redis incremental state backend requires option '$key'")
    }
  }

  private def hostPorts(key: String, value: String): Seq[(String, Int)] = {
    value.split(",").map(_.trim).filter(_.nonEmpty).map { item =>
      val parts = item.split(":")
      if (parts.length != 2) {
        throw new PuddingException(s"$key entries must use host:port, but got '$item'")
      }
      this.validateHost(parts(0), key)
      val parsedPort = try parts(1).toInt catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"$key has an invalid port in '$item'")
      }
      if (parsedPort <= 0 || parsedPort > 65535) {
        throw new PuddingException(s"$key has an invalid port in '$item'")
      }
      parts(0) -> parsedPort
    }.toSeq
  }

  private def safeNodeString(key: String): String = {
    this.options.get(key)
      .map(value => this.hostPorts(key, value).map {
        case (nodeHost, nodePort) => s"$nodeHost:$nodePort"
      }.mkString(","))
      .getOrElse("")
  }

  private def validateHost(value: String, key: String): Unit = {
    val host = Option(value).map(_.trim).getOrElse("")
    if (host.isEmpty ||
      host.exists(character =>
        character == '@' || character == '/' || character == '?' || character == '#' ||
          character.isWhitespace)) {
      throw new PuddingException(
        s"redis $key must be a plain host name without credentials or a URL")
    }
  }

  private def intOption(key: String, default: Int): Int = {
    this.options.get(key).map(_.trim).filter(_.nonEmpty).map { value =>
      try {
        val parsed = value.toInt
        if (parsed < 0) {
          throw new PuddingException(s"$key can not be negative")
        }
        parsed
      } catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"$key must be an integer")
      }
    }.getOrElse(default)
  }

  private def booleanOption(key: String, default: Boolean): Boolean = {
    this.options.get(key).map(_.trim).filter(_.nonEmpty).map { value =>
      value.toLowerCase(java.util.Locale.ROOT) match {
        case "true" => true
        case "false" => false
        case _ => throw new PuddingException(s"$key must be true or false")
      }
    }.getOrElse(default)
  }

  private trait RedisClient {

    def get(key: String): Option[String]

    def setNxPx(key: String, value: String, ttlMillis: Long): Boolean

    def del(key: String): Long

    def eval(script: String, keys: Seq[String], args: Seq[String]): Long

    def keysByPattern(pattern: String): Seq[String]

    def close(): Unit
  }

  private class PooledRedisClient(pool: Pool[Jedis]) extends RedisClient {

    override def get(key: String): Option[String] = this.withJedis { jedis =>
      Option(jedis.get(key))
    }

    override def setNxPx(key: String, value: String, ttlMillis: Long): Boolean = {
      this.withJedis { jedis =>
        val params = SetParams.setParams().nx()
        if (ttlMillis > 0L) {
          params.px(ttlMillis)
        }
        "OK" == jedis.set(key, value, params)
      }
    }

    override def del(key: String): Long = this.withJedis(_.del(key))

    override def eval(script: String, keys: Seq[String], args: Seq[String]): Long = {
      this.withJedis { jedis =>
        RedisStateBackend.this.asLong(jedis.eval(script, keys.asJava, args.asJava))
      }
    }

    override def keysByPattern(pattern: String): Seq[String] = this.withJedis { jedis =>
      RedisStateBackend.this.scan(jedis, pattern)
    }

    override def close(): Unit = pool.close()

    private def withJedis[T](body: Jedis => T): T = {
      val jedis = pool.getResource
      try {
        body(jedis)
      } catch {
        case error: JedisException =>
          throw new PuddingException(
            "redis incremental state backend operation failed: " +
              error.getClass.getSimpleName, error)
      } finally {
        jedis.close()
      }
    }
  }

  private class ClusterRedisClient(cluster: JedisCluster) extends RedisClient {

    override def get(key: String): Option[String] = this.guard(Option(cluster.get(key)))

    override def setNxPx(key: String, value: String, ttlMillis: Long): Boolean = {
      this.guard {
        val params = SetParams.setParams().nx()
        if (ttlMillis > 0L) {
          params.px(ttlMillis)
        }
        "OK" == cluster.set(key, value, params)
      }
    }

    override def del(key: String): Long = this.guard(cluster.del(key))

    override def eval(script: String, keys: Seq[String], args: Seq[String]): Long = {
      this.guard(RedisStateBackend.this.asLong(
        cluster.eval(script, keys.asJava, args.asJava)))
    }

    override def keysByPattern(pattern: String): Seq[String] = this.guard {
      val keys = new ListBuffer[String]()
      cluster.getClusterNodes.values().asScala.foreach { node =>
        val jedis = node.getResource
        try keys ++= RedisStateBackend.this.scan(jedis, pattern)
        finally jedis.close()
      }
      keys.toSeq.distinct
    }

    override def close(): Unit = cluster.close()

    private def guard[T](body: => T): T = {
      try {
        body
      } catch {
        case error: JedisException =>
          throw new PuddingException(
            "redis incremental state backend operation failed: " +
              error.getClass.getSimpleName, error)
      }
    }
  }
}

object RedisStateBackend {

  private val PersistStateScript =
    """local raw = redis.call('GET', KEYS[1])
      |if not raw then return 0 end
      |local ok, doc = pcall(cjson.decode, raw)
      |if not ok or doc['token'] ~= ARGV[1] then return 0 end
      |redis.call('SET', KEYS[2], ARGV[2])
      |return 1""".stripMargin

  private val RenewLockScript =
    """local raw = redis.call('GET', KEYS[1])
      |if not raw then return 0 end
      |local ok, doc = pcall(cjson.decode, raw)
      |if not ok or doc['token'] ~= ARGV[1] then return 0 end
      |redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
      |return 1""".stripMargin

  private val DeleteLockScript =
    """local raw = redis.call('GET', KEYS[1])
      |if not raw then return 0 end
      |local ok, doc = pcall(cjson.decode, raw)
      |if not ok or doc['token'] ~= ARGV[1] then return 0 end
      |redis.call('DEL', KEYS[1])
      |return 1""".stripMargin

  def lockTtlMillis(options: Map[String, String]): Long = {
    options.get("lockTtlMs")
      .map(_.trim).filter(_.nonEmpty).map { value =>
        try {
          val parsed = value.toLong
          if (parsed < 0L) {
            throw new PuddingException("lockTtlMs can not be negative")
          }
          parsed
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException("lockTtlMs must be a number of milliseconds")
        }
      }.getOrElse(0L)
  }
}

/**
 * Built-in provider for the Redis state backend.
 */
class RedisStateBackendProvider extends IncrementalStateBackendProvider {

  override def identifier: String = "redis"

  override def create(
      options: Map[String, String],
      hadoopConf: Configuration): IncrementalStateBackend =
    new RedisStateBackend(options)
}
