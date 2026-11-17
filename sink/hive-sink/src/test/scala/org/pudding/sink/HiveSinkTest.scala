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

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.definition.SinkContext
import org.pudding.core.{CommonTestUtils, PuddingException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Writes Hive tables through `hive-sink`, including save modes and partitioning.
 */
class HiveSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for hive sink")

  private val database = "pudding_hive_sink_test"

  var sparkSession: SparkSession = _

  private var hiveSetupError: Option[Throwable] = None

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession

    try {
      sparkSession.sql(s"drop database if exists $database cascade")
      sparkSession.sql(s"create database $database")
    } catch {
      case e: Throwable => hiveSetupError = Some(e)
    }
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      Try(sparkSession.sql(s"drop database if exists $database cascade"))
    }
    utils.closeSparkSession()
  }

  private def withHive(testBody: => Any): Unit = {
    hiveSetupError match {
      case Some(error) => cancel(s"local hive metastore is not available: ${error.getMessage}")
      case None => testBody
    }
  }

  private def freshTable(tableName: String): String = {
    sparkSession.sql(s"drop table if exists $database.$tableName")
    s"$database.$tableName"
  }

  test("write dataframe to hive table") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val table = freshTable("sink_basic")
      val dataFrame = Seq((1, "tom"), (2, "jerry")).toDF("id", "name")

      new HiveSink().writer(dataFrame, Some(Map("table" -> table, "mode" -> "overwrite")))

      val result = sparkSession.table(table)
      result.count() shouldBe 2
      result.select("id", "name").collect()
        .map(row => (row.getInt(0), row.getString(1)))
        .toSet shouldBe Set((1, "tom"), (2, "jerry"))
    }
  }

  test("write to a hive table through a catalog reference") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val table = freshTable("sink_catalog")
      val catalog = ResolvedCatalog(
        name = "hive_prod",
        `type` = "hive",
        sparkCatalogName = "spark_catalog",
        sessionCatalog = true,
        sparkConf = Map.empty,
        properties = Map.empty)

      new HiveSink().writer(
        Seq((1, "tom"), (2, "jerry")).toDF("id", "name"),
        SinkContext(
          Some(Map("table" -> table, "mode" -> "overwrite")),
          catalog = Some(catalog)))

      sparkSession.table(table).count() shouldBe 2
    }
  }

  test("append to hive table") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val table = freshTable("sink_append")
      val sink = new HiveSink()

      sink.writer(
        Seq((1, "tom")).toDF("id", "name"),
        Some(Map("table" -> table, "mode" -> "overwrite"))
      )
      sink.writer(
        Seq((2, "jerry")).toDF("id", "name"),
        Some(Map("table" -> table, "mode" -> "append"))
      )

      sparkSession.table(table).count() shouldBe 2
    }
  }

  test("write partitioned hive table") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val table = freshTable("sink_partitioned")
      val dataFrame = Seq(
        (1, "tom", "2024-01-01"),
        (2, "jerry", "2024-01-02"),
        (3, "spark", "2024-01-01")
      ).toDF("id", "name", "dt")

      new HiveSink().writer(dataFrame, Some(Map(
        "table" -> table,
        "mode" -> "overwrite",
        "partitionBy" -> Seq("dt")
      )))

      sparkSession.sql(s"show partitions $table").count() shouldBe 2
      sparkSession.table(table).count() shouldBe 3
    }
  }

  test("null config throws PuddingException") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val dataFrame = Seq((1, "tom")).toDF("id", "name")

      intercept[PuddingException] {
        new HiveSink().writer(dataFrame, None)
      }
    }
  }

  test("missing table config throws PuddingException") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val dataFrame = Seq((1, "tom")).toDF("id", "name")

      intercept[PuddingException] {
        new HiveSink().writer(dataFrame, Some(Map("mode" -> "overwrite")))
      }
    }
  }

  test("unsupported mode throws PuddingException") {
    withHive {
      val session = sparkSession
      import session.implicits._
      val dataFrame = Seq((1, "tom")).toDF("id", "name")

      intercept[PuddingException] {
        new HiveSink().writer(
          dataFrame,
          Some(Map("table" -> freshTable("sink_bad_mode"), "mode" -> "bad"))
        )
      }
    }
  }
}
