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

package org.pudding.integration

import java.nio.file.Files
import java.util.UUID

import scala.util.Try
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql.SparkSession
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Shared MinIO/S3 fixture for lakehouse data-path regression tests.
 *
 * The suite runs when `PUDDING_TESTCONTAINERS=true`, or against a disposable external endpoint
 * supplied through `PUDDING_TEST_S3_ENDPOINT` (with optional `PUDDING_TEST_S3_ACCESS_KEY` and
 * `PUDDING_TEST_S3_SECRET_KEY`). It creates one uniquely named bucket; external endpoints must
 * permit bucket creation. The bucket is not deleted on an external endpoint.
 */
private[integration] object ObjectStoreTestSupport {

  private val endpointVariable = "PUDDING_TEST_S3_ENDPOINT"

  private val accessKeyVariable = "PUDDING_TEST_S3_ACCESS_KEY"

  private val secretKeyVariable = "PUDDING_TEST_S3_SECRET_KEY"

  private val imageVariable = "PUDDING_TEST_MINIO_IMAGE"

  private val externalEndpoint =
    sys.env.get(endpointVariable).map(_.trim).filter(_.nonEmpty)

  private val containersEnabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  val enabled: Boolean = externalEndpoint.isDefined || containersEnabled

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true")) ||
      externalEndpoint.isDefined

  private val defaultAccessKey = "pudding"

  private val defaultSecretKey = "pudding-secret"

  private val defaultImage =
    "minio/minio:RELEASE.2023-09-04T19-57-37Z"

  final case class Context(
      endpoint: String,
      bucket: String,
      accessKey: String,
      secretKey: String,
      spark: SparkSession,
      container: Option[GenericContainer[_]]) {

    def s3a(relative: String): String =
      s"s3a://$bucket/${relative.stripPrefix("/")}"

    def close(): Unit = {
      try this.spark.close()
      catch {
        case NonFatal(_) => ()
      }
      this.container.foreach { value =>
        try value.stop()
        catch {
          case NonFatal(_) => ()
        }
      }
    }
  }

  def open(appName: String): Option[Context] = {
    if (!this.enabled) {
      None
    } else {
      var container: GenericContainer[_] = null
      var spark: SparkSession = null
      try {
        val endpoint = this.externalEndpoint.getOrElse {
          val minio = new GenericContainer(DockerImageName.parse(
            sys.env.getOrElse(imageVariable, this.defaultImage)))
          val accessKey = sys.env.getOrElse(accessKeyVariable, this.defaultAccessKey)
          val secretKey = sys.env.getOrElse(secretKeyVariable, this.defaultSecretKey)
          minio.withEnv("MINIO_ROOT_USER", accessKey)
          minio.withEnv("MINIO_ROOT_PASSWORD", secretKey)
          minio.withExposedPorts(9000)
          minio.withCommand("server", "/data")
          minio.waitingFor(Wait.forHttp("/minio/health/live").forStatusCode(200))
          container = minio
          container.start()
          s"http://${container.getHost}:${container.getMappedPort(9000)}"
        }
        val accessKey = sys.env.getOrElse(accessKeyVariable, this.defaultAccessKey)
        val secretKey = sys.env.getOrElse(secretKeyVariable, this.defaultSecretKey)
        val bucket = "pudding-lakehouse-" + UUID.randomUUID().toString.replace("-", "")
        this.createBucket(endpoint, bucket, accessKey, secretKey)
        spark = this.sparkSession(appName, endpoint, accessKey, secretKey, bucket)
        Some(Context(endpoint, bucket, accessKey, secretKey, spark, Option(container)))
      } catch {
        case NonFatal(error) =>
          if (spark != null) {
            try spark.close()
            catch {
              case NonFatal(_) => ()
            }
          }
          Option(container).foreach(value =>
            try value.stop()
            catch {
              case NonFatal(_) => ()
            })
          if (this.required) {
            throw new IllegalStateException(
              "object-store lakehouse test environment is required but unavailable: " +
                error.getClass.getSimpleName,
              error)
          }
          None
      }
    }
  }

  private def createBucket(
      endpoint: String,
      bucket: String,
      accessKey: String,
      secretKey: String): Unit = {
    val s3 = AmazonS3ClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
      .withCredentials(new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(accessKey, secretKey)))
      .withPathStyleAccessEnabled(true)
      .build()
    try {
      if (!s3.doesBucketExistV2(bucket)) {
        s3.createBucket(bucket)
      }
    } finally {
      s3.shutdown()
    }
  }

  private def sparkSession(
      appName: String,
      endpoint: String,
      accessKey: String,
      secretKey: String,
      bucket: String): SparkSession = {
    val ssl = endpoint.toLowerCase.startsWith("https://")
    val config = Map(
      "spark.ui.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "2",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.sql.extensions" ->
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
      "spark.sql.catalog.paimon" -> "org.apache.paimon.spark.SparkCatalog",
      "spark.sql.catalog.paimon.warehouse" -> s"s3a://$bucket/paimon-warehouse",
      "spark.hadoop.fs.s3a.endpoint" -> endpoint,
      "spark.hadoop.fs.s3a.access.key" -> accessKey,
      "spark.hadoop.fs.s3a.secret.key" -> secretKey,
      "spark.hadoop.fs.s3a.path.style.access" -> "true",
      "spark.hadoop.fs.s3a.connection.ssl.enabled" -> ssl.toString,
      "spark.hadoop.fs.s3a.endpoint.region" -> "us-east-1",
      "spark.hadoop.fs.s3a.aws.credentials.provider" ->
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    val builder = SparkSession.builder().appName(appName).master("local[2]")
    config.foldLeft(builder) { case (next, (key, value)) => next.config(key, value) }
      .getOrCreate()
  }

  def localInput(lines: Seq[String]): java.nio.file.Path = {
    val directory = Files.createTempDirectory("lakehouse-object-store-input")
    Files.write(
      directory.resolve("batch.json"),
      lines.mkString("\n").getBytes(java.nio.charset.StandardCharsets.UTF_8))
    directory
  }

  def deleteLocal(path: java.nio.file.Path): Unit = {
    if (path != null && Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        import scala.collection.JavaConverters._
        stream.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(value => Try(Files.deleteIfExists(value)))
      } finally {
        stream.close()
      }
    }
  }
}
