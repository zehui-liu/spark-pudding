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

package org.pudding.source.cdc

import java.nio.file.{Files, Path}
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.utility.DockerImageName

import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.SourceContext

/**
 * End-to-end MongoDB change-stream test against a single-node replica set.
 */
class MongodbCdcSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val enabled =
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))

  private val required =
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))

  private val image =
    sys.env.getOrElse("PUDDING_TEST_MONGO_IMAGE", "mongo:7.0")

  private var container: MongoDBContainer = _

  private var mongo: MongoClient = _

  private var spark: SparkSession = _

  private var checkpoint: Path = _

  private val rows = new CopyOnWriteArrayList[(String, String, String)]()

  override def beforeAll(): Unit = {
    if (this.enabled) {
      try {
        this.container = new MongoDBContainer(DockerImageName.parse(this.image))
        this.container.start()
        this.mongo = MongoClients.create(this.container.getReplicaSetUrl)
        val collection = this.collection
        collection.insertOne(new Document("_id", "1").append("name", "tom").append("value", 1))
        collection.insertOne(new Document("_id", "2").append("name", "jerry").append("value", 2))
        this.spark = SparkSession.builder()
          .appName("mongodb-cdc-source-test")
          .master("local[2]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()
        this.checkpoint = Files.createTempDirectory("mongodb-cdc-checkpoint")
      } catch {
        case NonFatal(error) =>
          this.closeAll()
          if (this.required) {
            throw new IllegalStateException(
              "MongoDB container is required but the CDC source test failed to start: " +
                error.getClass.getSimpleName,
              error)
          }
      }
    }
  }

  override def afterAll(): Unit = {
    this.closeAll()
  }

  test("snapshot, changes and checkpoint restart") {
    assume(this.container != null && this.spark != null, "MongoDB container is not available")
    this.rows.clear()
    var query = this.startQuery()
    try {
      this.waitForOps(Set("I"), 2)
      this.rows.asScala.filter(_._1 == "I").map(row => row._2 -> row._3).toSet shouldBe
        Set("1" -> "tom", "2" -> "jerry")

      val collection = this.collection
      collection.insertOne(new Document("_id", "3").append("name", "spark").append("value", 3))
      collection.updateOne(
        new Document("_id", "1"),
        new Document("$set", new Document("name", "tom_v2")))
      collection.deleteOne(new Document("_id", "2"))
      this.waitForOps(Set("I", "U", "D"), 4)
      this.rows.asScala.exists(row =>
        row._1 == "U" && row._2 == "1" && row._3 == "tom_v2") shouldBe true
      this.rows.asScala.exists(row => row._1 == "D" && row._2 == "2") shouldBe true
    } finally {
      this.stop(query)
    }

    query = this.startQuery()
    try {
      this.collection.insertOne(
        new Document("_id", "4").append("name", "restart").append("value", 4))
      this.waitUntil(60, "document after checkpoint restart") {
        this.rows.asScala.exists(row =>
          row._1 == "I" && row._2 == "4" && row._3 == "restart")
      }
    } finally {
      this.stop(query)
    }
  }

  private def collection: MongoCollection[Document] = {
    this.mongo.getDatabase("inventory").getCollection("orders")
  }

  private def startQuery(): StreamingQuery = {
    val source = new MongodbCdcSource()
    val config = Map[String, Any](
      "uri" -> this.container.getReplicaSetUrl,
      "database" -> "inventory",
      "collection" -> "orders",
      "schema" -> "_id string, name string, value int",
      "maxRecordsPerBatch" -> 2)
    val frame = source.initData(
      this.spark,
      SourceContext(Some(config), executionMode = Some(ExecutionMode.Streaming)))
    frame.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("500 milliseconds"))
      .option("checkpointLocation", this.checkpoint.toAbsolutePath.toString)
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.collect().foreach { row =>
          this.rows.add((
            row.getAs[String]("__op"),
            row.getAs[String]("_id"),
            row.getAs[String]("name")))
        }
        ()
      }
      .start()
  }

  private def stop(query: StreamingQuery): Unit = {
    if (query != null) {
      try query.stop()
      catch {
        case NonFatal(_) => ()
      }
      try query.awaitTermination(30)
      catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
  }

  private def waitForOps(expected: Set[String], minimum: Int): Unit = {
    this.waitUntil(90, s"CDC operations $expected") {
      expected.subsetOf(this.rows.asScala.map(_._1).toSet) && this.rows.size() >= minimum
    }
  }

  private def waitUntil(seconds: Int, description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds.toLong)
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(250L)
    }
    condition shouldBe true
  }

  private def closeAll(): Unit = {
    if (this.spark != null) {
      try this.spark.close()
      catch {
        case NonFatal(_) => ()
      }
      this.spark = null
    }
    if (this.mongo != null) {
      try this.mongo.close()
      catch {
        case NonFatal(_) => ()
      }
      this.mongo = null
    }
    if (this.checkpoint != null && Files.exists(this.checkpoint)) {
      try {
        val stream = Files.walk(this.checkpoint)
        try {
          stream.iterator().asScala.toVector
            .sortBy(_.getNameCount)(Ordering.Int.reverse)
            .foreach(value => Try(Files.deleteIfExists(value)))
        } finally {
          stream.close()
        }
      } catch {
        case NonFatal(_) => ()
      }
      this.checkpoint = null
    }
    if (this.container != null) {
      try this.container.stop()
      catch {
        case NonFatal(_) => ()
      }
      this.container = null
    }
  }
}
