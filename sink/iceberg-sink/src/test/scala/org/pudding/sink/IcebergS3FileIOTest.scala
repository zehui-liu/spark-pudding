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

package org.pudding.sink

import java.net.URI
import java.util.UUID

import scala.util.control.NonFatal

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Opt-in MinIO coverage for Iceberg's S3FileIO data layer. This is separate from the Hadoop S3A
 * runtime used for streaming checkpoints; the test validates that the bundled AWS SDK can create
 * S3FileIO clients and write table data against an S3-compatible endpoint.
 *
 * Set `PUDDING_TESTCONTAINERS=true` to start MinIO with Testcontainers, or provide
 * `PUDDING_TEST_S3_ENDPOINT` (plus optional `_ACCESS_KEY`, `_SECRET_KEY`) for an existing
 * disposable endpoint. The endpoint must allow creating the generated test bucket.
 */
class IcebergS3FileIOTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val externalEndpoint =
    sys.env.get("PUDDING_TEST_S3_ENDPOINT").map(_.trim).filter(_.nonEmpty)

  private val enabled =
    externalEndpoint.isDefined ||
      sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image =
    sys.env.getOrElse("PUDDING_TEST_MINIO_IMAGE", "minio/minio:RELEASE.2023-09-04T19-57-37Z")

  private val user =
    sys.env.getOrElse("PUDDING_TEST_S3_ACCESS_KEY", "pudding")

  private val password =
    sys.env.getOrElse("PUDDING_TEST_S3_SECRET_KEY", "pudding-secret")

  private var container: GenericContainer[_] = _

  private var sparkSession: SparkSession = _

  private var bucket: String = _

  private var endpointValue: String = _

  override def beforeAll(): Unit = {
    if (this.enabled) {
      try {
        this.endpointValue = this.externalEndpoint.getOrElse {
          val minio = new GenericContainer(DockerImageName.parse(this.image))
          minio.withEnv("MINIO_ROOT_USER", this.user)
          minio.withEnv("MINIO_ROOT_PASSWORD", this.password)
          minio.withExposedPorts(9000)
          minio.withCommand("server", "/data")
          minio.waitingFor(Wait.forHttp("/minio/health/live").forStatusCode(200))
          this.container = minio
          this.container.start()
          this.containerEndpoint
        }
        this.bucket = "pudding-iceberg-" + UUID.randomUUID().toString.replace("-", "")
        val s3 = S3Client.builder()
          .endpointOverride(URI.create(this.endpointValue))
          .region(Region.US_EAST_1)
          .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(this.user, this.password)))
          .forcePathStyle(true)
          .build()
        try {
          s3.createBucket(CreateBucketRequest.builder().bucket(this.bucket).build())
        } finally {
          s3.close()
        }
        System.setProperty("aws.region", "us-east-1")
        this.sparkSession = SparkSession.builder()
          .appName("test for iceberg s3 file io")
          .master("local[2]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .config("spark.sql.catalog.s3_catalog", "org.apache.iceberg.spark.SparkCatalog")
          .config("spark.sql.catalog.s3_catalog.type", "jdbc")
          .config(
            "spark.sql.catalog.s3_catalog.uri",
            "jdbc:h2:mem:iceberg_s3_catalog;DB_CLOSE_DELAY=-1")
          .config("spark.sql.catalog.s3_catalog.jdbc.user", "sa")
          .config("spark.sql.catalog.s3_catalog.jdbc.password", "")
          .config("spark.sql.catalog.s3_catalog.jdbc.schema-version", "V1")
          .config(
            "spark.sql.catalog.s3_catalog.warehouse",
            s"s3://${this.bucket}/warehouse")
          .config(
            "spark.sql.catalog.s3_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
          .config("spark.sql.catalog.s3_catalog.s3.endpoint", this.endpointValue)
          .config("spark.sql.catalog.s3_catalog.s3.access-key-id", this.user)
          .config("spark.sql.catalog.s3_catalog.s3.secret-access-key", this.password)
          .config("spark.sql.catalog.s3_catalog.s3.path-style-access", "true")
          .getOrCreate()
        this.sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS s3_catalog.db")
      } catch {
        case NonFatal(error) =>
          if (this.sparkSession != null) {
            try this.sparkSession.close()
            catch {
              case NonFatal(_) => ()
            }
            this.sparkSession = null
          }
          this.stopContainer()
          if (this.required) {
            throw new IllegalStateException(
              "MinIO is required but the Iceberg S3FileIO test failed to start: " +
                error.getClass.getSimpleName,
              error)
          }
      }
    }
  }

  override def afterAll(): Unit = {
    if (this.sparkSession != null) {
      this.sparkSession.close()
    }
    this.stopContainer()
  }

  test("write and read an Iceberg table through S3FileIO") {
    assume(this.sparkSession != null, "S3-compatible endpoint is not available")
    val table = "s3_catalog.db.s3_orders"
    this.sparkSession.sql(s"DROP TABLE IF EXISTS $table")
    val session = this.sparkSession
    import session.implicits._

    new IcebergSink().writer(
      Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
      Some(Map("table" -> table, "mode" -> "overwrite")))

    val rows = this.sparkSession.table(table)
    rows.count() shouldBe 2
    rows.select("id", "name").collect()
      .map(row => (row.getInt(0), row.getString(1))).toSet shouldBe
      Set((1, "tom"), (2, "jerry"))
  }

  private def containerEndpoint: String =
    s"http://${this.container.getHost}:${this.container.getMappedPort(9000)}"

  private def stopContainer(): Unit = {
    if (this.container != null) {
      try this.container.stop()
      catch {
        case NonFatal(_) => ()
      }
      this.container = null
    }
  }
}
