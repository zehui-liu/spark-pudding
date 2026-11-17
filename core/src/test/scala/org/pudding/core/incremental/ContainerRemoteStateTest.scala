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

package org.pudding.core.incremental

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.containers.wait.strategy.Wait

import org.pudding.core.definition.IncrementalSemantics
import org.pudding.core.util.{CheckpointTool, StreamingCheckpoint}

/**
 * Container-backed HDFS and S3A tests for incremental state and streaming checkpoints. They are
 * enabled by `PUDDING_TESTCONTAINERS=true` and are intended for CI; local HDFS/S3 endpoints can
 * still be supplied through [[RemoteFileSystemStateBackendTest]] environment variables.
 */
class ContainerRemoteStateTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val hdfsImage =
    sys.env.getOrElse("PUDDING_TEST_HDFS_IMAGE", "apache/hadoop:3.3.6")

  // MinIO no longer serves anonymous pulls from Docker Hub or quay.io. Chainguard's
  // rebuilt image is anonymously pullable; pin the multi-arch digest for reproducibility.
  private val minioImage =
    sys.env.getOrElse(
      "PUDDING_TEST_MINIO_IMAGE",
      "cgr.dev/chainguard/minio@sha256:59953701f8f8d0de81aab456f0b0b6c0a1804119415559bf7ede79d944108879")

  private val minioUser = "pudding"

  private val minioPassword = "pudding-secret"

  private val minioBucket = "pudding-state"

  private var network: Network = _

  private var hdfsNamenode: GenericContainer[_] = _

  private var hdfsDatanode: GenericContainer[_] = _

  private var minio: GenericContainer[_] = _

  private var hdfsUri: String = ""

  private var s3Uri: String = ""

  private var s3Configuration: Configuration = _

  @volatile private var minioAttempted = false

  private val skipS3Container =
    sys.env.get("PUDDING_TESTCONTAINERS_SKIP_S3").exists(_.trim.equalsIgnoreCase("true"))

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

  override def beforeAll(): Unit = {
    if (ContainerTestSupport.enabled) {
      this.startHdfs()
    }
  }

  override def afterAll(): Unit = {
    Option(this.hdfsDatanode).foreach(container => Try(container.stop()))
    Option(this.hdfsNamenode).foreach(container => Try(container.stop()))
    Option(this.minio).foreach(container => Try(container.stop()))
    Option(this.network).foreach(value => Try(value.close()))
  }

  test("persist and recover a pending transaction on HDFS") {
    assume(this.hdfsUri.nonEmpty, "HDFS container is not available")
    this.withPath(this.hdfsUri, this.hadoopConfiguration()) { path =>
      this.exercise(path, this.hadoopConfiguration())
    }
  }

  test("persist and recover a pending transaction on S3A") {
    this.ensureMinio()
    assume(this.s3Uri.nonEmpty, "MinIO container is not available")
    this.withPath(this.s3Uri, this.s3Configuration) { path =>
      this.exercise(path, this.s3Configuration)
    }
  }

  test("inspect and delete a checkpoint on S3A") {
    this.ensureMinio()
    assume(this.s3Uri.nonEmpty, "MinIO container is not available")
    this.withPath(this.s3Uri, this.s3Configuration) { path =>
      val fileSystem = path.getFileSystem(this.s3Configuration)
      val commit = new Path(path, "commits/1")
      fileSystem.mkdirs(commit)
      val output = fileSystem.create(new Path(commit, "0"))
      try output.write("batch".getBytes(StandardCharsets.UTF_8))
      finally output.close()
      fileSystem.mkdirs(new Path(path, "offsets"))
      val metadata = fileSystem.create(new Path(path, "metadata"))
      try metadata.write("{}".getBytes(StandardCharsets.UTF_8))
      finally metadata.close()

      val properties = Files.createTempFile("pudding-s3a-checkpoint", ".properties")
      val text =
        s"""fs.s3a.endpoint=${this.s3Configuration.get("fs.s3a.endpoint")}
           |fs.s3a.access.key=${this.s3Configuration.get("fs.s3a.access.key")}
           |fs.s3a.secret.key=${this.s3Configuration.get("fs.s3a.secret.key")}
           |fs.s3a.path.style.access=true
           |fs.s3a.connection.ssl.enabled=false
           |fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
           |fs.s3a.endpoint.region=us-east-1
           |""".stripMargin
      Files.write(properties, text.getBytes(StandardCharsets.UTF_8))
      val stdout = new ByteArrayOutputStream()
      val stderr = new ByteArrayOutputStream()
      try {
        val show = CheckpointTool.run(
          Array("show", path.toString, "--conf-file", properties.toString),
          new PrintStream(stdout, true, StandardCharsets.UTF_8.name()),
          new PrintStream(stderr, true, StandardCharsets.UTF_8.name()))
        show shouldBe 0
        stdout.toString(StandardCharsets.UTF_8.name()) should include ("checkpoint: true")

        val deleted = CheckpointTool.run(
          Array("delete", path.toString, "--conf-file", properties.toString),
          new PrintStream(stdout, true, StandardCharsets.UTF_8.name()),
          new PrintStream(stderr, true, StandardCharsets.UTF_8.name()))
        deleted shouldBe 0
        fileSystem.exists(path) shouldBe false
      } finally {
        Files.deleteIfExists(properties)
      }
    }
  }

  test("restart from and delete a successful checkpoint on S3A") {
    this.ensureMinio()
    assume(this.s3Uri.nonEmpty, "MinIO container is not available")
    this.withPath(this.s3Uri, this.s3Configuration) { base =>
      val sparkSession = this.sparkSessionWithS3()
      val input = Files.createTempDirectory("pudding-s3-checkpoint-input")
      val deleteInput = Files.createTempDirectory("pudding-s3-checkpoint-delete-input")
      val output = Files.createTempDirectory("pudding-s3-checkpoint-counts")
      try {
        this.writeText(input.resolve("a.txt"), "a")
        this.writeText(input.resolve("b.txt"), "b")
        val counts = output.resolve("restart.counts")
        val checkpoint = new Path(base, "restart-checkpoint")
        StreamingCheckpoint.awaitTermination(
          this.startTextQuery(sparkSession, input, checkpoint, counts),
          Map("checkpointRetention" -> "keep"),
          Some("available-now"),
          Some(checkpoint.toString))
        this.countLines(counts) shouldBe 2

        this.writeText(input.resolve("c.txt"), "c")
        StreamingCheckpoint.awaitTermination(
          this.startTextQuery(sparkSession, input, checkpoint, counts),
          Map("checkpointRetention" -> "keep"),
          Some("available-now"),
          Some(checkpoint.toString))
        this.countLines(counts) shouldBe 3
        checkpoint.getFileSystem(this.s3Configuration).exists(checkpoint) shouldBe true

        this.writeText(deleteInput.resolve("only.txt"), "only")
        val deleteCounts = output.resolve("delete.counts")
        val deleteCheckpoint = new Path(base, "delete-checkpoint")
        StreamingCheckpoint.awaitTermination(
          this.startTextQuery(sparkSession, deleteInput, deleteCheckpoint, deleteCounts),
          Map("checkpointRetention" -> "delete-on-success"),
          Some("available-now"),
          Some(deleteCheckpoint.toString))
        deleteCheckpoint.getFileSystem(this.s3Configuration).exists(deleteCheckpoint) shouldBe false
      } finally {
        sparkSession.stop()
        this.deleteLocal(input)
        this.deleteLocal(deleteInput)
        this.deleteLocal(output)
      }
    }
  }

  private def sparkSessionWithS3(): SparkSession = {
    val builder = SparkSession.builder()
      .appName("pudding s3 checkpoint restart")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      // delete-on-success needs the final zero-row progress update to prove completion.
      .config("spark.sql.streaming.noDataProgressEventInterval", "0")
    Seq(
      "fs.s3a.endpoint",
      "fs.s3a.access.key",
      "fs.s3a.secret.key",
      "fs.s3a.aws.credentials.provider",
      "fs.s3a.endpoint.region").foreach { key =>
      builder.config(s"spark.hadoop.$key", this.s3Configuration.get(key))
    }
    builder
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    val session = builder.getOrCreate()
    Seq(
      "fs.s3a.endpoint",
      "fs.s3a.access.key",
      "fs.s3a.secret.key",
      "fs.s3a.aws.credentials.provider",
      "fs.s3a.endpoint.region").foreach { key =>
      session.sparkContext.hadoopConfiguration.set(key, this.s3Configuration.get(key))
    }
    session.sparkContext.hadoopConfiguration.setBoolean(
      "fs.s3a.path.style.access", true)
    session.sparkContext.hadoopConfiguration.setBoolean(
      "fs.s3a.connection.ssl.enabled", false)
    session
  }

  private def startTextQuery(
      sparkSession: SparkSession,
      input: java.nio.file.Path,
      checkpoint: Path,
      counts: java.nio.file.Path): StreamingQuery = {
    sparkSession.readStream
      .format("text")
      .option("path", input.toString)
      .load()
      .writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, _: Long) =>
        Files.write(
          counts,
          s"${batch.count()}\n".getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND)
        ()
      }
      .option("checkpointLocation", checkpoint.toString)
      .outputMode("append")
      .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
      .start()
  }

  private def writeText(path: java.nio.file.Path, value: String): Unit = {
    Files.write(path, value.getBytes(StandardCharsets.UTF_8))
  }

  private def countLines(path: java.nio.file.Path): Long = {
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.map(_.trim.toLong).sum
  }

  private def deleteLocal(path: java.nio.file.Path): Unit = {
    val stream = Files.walk(path)
    try {
      stream.iterator().asScala.toVector.reverse.foreach(value => Files.deleteIfExists(value))
    } finally {
      stream.close()
    }
  }

  private def exercise(path: Path, conf: Configuration): Unit = {
    val first = new IncrementalStateStore(
      new FileSystemStateBackend(path.toString, conf))
    first.acquire("source")
    first.begin("source", None, "42", "txn-container", this.plan)
    first.close()

    val second = new IncrementalStateStore(
      new FileSystemStateBackend(path.toString, conf))
    intercept[org.pudding.core.PuddingException] {
      second.acquire("source")
    }.getMessage should include ("locked")

    second.forceUnlock("source") shouldBe true
    val info = second.acquire("source")
    info.fencingToken shouldBe 2L
    try {
      second.load("source").pending.map(_.transactionId) shouldBe Some("txn-container")
      second.commit("source", "txn-container")
      second.load("source").committedCursor shouldBe Some("42")
      second.listSourceIds() shouldBe Seq("source")
    } finally {
      second.release("source") shouldBe true
    }
  }

  private def withPath(uri: String, conf: Configuration)(body: Path => Unit): Unit = {
    val root = new Path(uri, "run-" + UUID.randomUUID().toString.replace("-", ""))
    val fileSystem = root.getFileSystem(conf)
    try {
      body(root)
    } finally {
      Try(fileSystem.delete(root, true))
      Try(fileSystem.close())
    }
  }

  private def hadoopConfiguration(): Configuration = {
    val conf = new Configuration()
    conf.setBoolean("dfs.client.use.datanode.hostname", true)
    conf
  }

  private def freePort(): Int = {
    val socket = new java.net.ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  private def startHdfs(): Unit = {
    try {
      this.network = Network.newNetwork()
      val namenode = ContainerFactory.container(this.hdfsImage)
        .withNetwork(this.network)
        .withNetworkAliases("namenode")
        .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-root/dfs/name")
        .withEnv("HADOOP_USER_NAME", "hadoop")
        .withEnv("CORE-SITE.XML_fs.defaultFS", "hdfs://namenode:8020")
        .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", "namenode:8020")
        .withExposedPorts(8020)
        .withCommand("hdfs", "namenode")
        .waitingFor(Wait.forLogMessage(".*NameNode RPC up at.*", 1))
      this.hdfsNamenode = namenode
      namenode.start()

      // The DataNode streams blocks to clients on macOS through an advertised host address, so
      // bind a free host port and advertise 127.0.0.1 instead of hard-coding 9866.
      val datanodePort = this.freePort()
      val datanode = ContainerFactory.container(this.hdfsImage)
        .withNetwork(this.network)
        .withEnv("HADOOP_USER_NAME", "hadoop")
        .withEnv("CORE-SITE.XML_fs.defaultFS", "hdfs://namenode:8020")
        .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-address", "namenode:8020")
        .withEnv("HDFS-SITE.XML_dfs.replication", "1")
        .withEnv("HDFS-SITE.XML_dfs.datanode.address", s"0.0.0.0:$datanodePort")
        .withEnv("HDFS-SITE.XML_dfs.datanode.hostname", "127.0.0.1")
        .withExposedPorts(datanodePort)
        .withCommand("hdfs", "datanode")
        .waitingFor(Wait.forLogMessage(".*successfully registered with NN.*", 1))
      datanode.setPortBindings(
        java.util.Arrays.asList(s"$datanodePort:$datanodePort"))
      this.hdfsDatanode = datanode
      datanode.start()
      namenode.execInContainer(
        "bash",
        "-c",
        "hdfs dfs -mkdir -p /tmp && hdfs dfs -chmod 1777 /tmp")
      this.hdfsUri = s"hdfs://${namenode.getHost}:${namenode.getMappedPort(8020)}/tmp/"
    } catch {
      case NonFatal(error) =>
        Option(this.hdfsDatanode).foreach(container => Try(container.stop()))
        Option(this.hdfsNamenode).foreach(container => Try(container.stop()))
        this.hdfsDatanode = null
        this.hdfsNamenode = null
        if (ContainerTestSupport.required) {
          throw new IllegalStateException("HDFS container is required but failed to start", error)
        }
    }
  }

  private def ensureMinio(): Unit = this.synchronized {
    if (!this.minioAttempted && ContainerTestSupport.enabled && !this.skipS3Container) {
      this.minioAttempted = true
      this.startMinio()
    }
  }

  private def startMinio(): Unit = {
    try {
      Option(this.network).getOrElse {
        this.network = Network.newNetwork()
        this.network
      }
      val container = ContainerFactory.container(this.minioImage)
        .withNetwork(this.network)
        .withEnv("MINIO_ROOT_USER", this.minioUser)
        .withEnv("MINIO_ROOT_PASSWORD", this.minioPassword)
        .withExposedPorts(9000)
        .withCommand("server", "/data")
        .waitingFor(Wait.forHttp("/minio/health/live").forStatusCode(200))
      container.start()
      this.minio = container
      val endpoint = s"http://${container.getHost}:${container.getMappedPort(9000)}"
      val s3 = AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
        .withCredentials(new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(this.minioUser, this.minioPassword)))
        .withPathStyleAccessEnabled(true)
        .build()
      try {
        if (!s3.doesBucketExistV2(this.minioBucket)) {
          s3.createBucket(this.minioBucket)
        }
      } finally {
        s3.shutdown()
      }
      val conf = new Configuration()
      conf.set("fs.s3a.endpoint", endpoint)
      conf.set("fs.s3a.access.key", this.minioUser)
      conf.set("fs.s3a.secret.key", this.minioPassword)
      conf.setBoolean("fs.s3a.path.style.access", true)
      conf.setBoolean("fs.s3a.connection.ssl.enabled", false)
      conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      conf.set("fs.s3a.endpoint.region", "us-east-1")
      this.s3Configuration = conf
      this.s3Uri = s"s3a://${this.minioBucket}/pudding-test"
    } catch {
      case NonFatal(error) =>
        Option(this.minio).foreach(value => Try(value.stop()))
        this.minio = null
        if (ContainerTestSupport.required) {
          throw new IllegalStateException("MinIO container is required but failed to start", error)
        }
    }
  }
}
