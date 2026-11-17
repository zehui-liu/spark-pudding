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

package org.pudding.source

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.definition.SourceContext
import org.pudding.core.{CommonTestUtils, PuddingException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Reads Hive tables through `hive-source`, including filters and schema projection.
 */
class HiveSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for hive source")

  private val database = "pudding_hive_source_test"

  private val table = s"$database.source_test"

  var sparkSession: SparkSession = _

  private var hiveSetupError: Option[Throwable] = None

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession

    try {
      sparkSession.sql(s"drop database if exists $database cascade")
      sparkSession.sql(s"create database $database")

      val session = sparkSession
      import session.implicits._
      Seq((1, "tom", 20), (2, "jerry", 25), (3, "spark", 30))
        .toDF("id", "name", "age")
        .write.mode("overwrite")
        .saveAsTable(table)
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

  test("read hive table") {
    withHive {
      val dataFrame = new HiveSource().initData(sparkSession, Some(Map("table" -> table)))
      dataFrame.count() shouldBe 3
      dataFrame.select("id", "name", "age").collect()
        .map(row => (row.getInt(0), row.getString(1), row.getInt(2)))
        .toSet shouldBe Set((1, "tom", 20), (2, "jerry", 25), (3, "spark", 30))
    }
  }

  test("read hive table through a catalog reference") {
    withHive {
      val catalog = ResolvedCatalog(
        name = "hive_prod",
        `type` = "hive",
        sparkCatalogName = "spark_catalog",
        sessionCatalog = true,
        sparkConf = Map.empty,
        properties = Map.empty)
      val dataFrame = new HiveSource().initData(
        sparkSession,
        SourceContext(Some(Map("table" -> table)), catalog = Some(catalog)))

      dataFrame.count() shouldBe 3
    }
  }

  test("reject a non-hive catalog reference") {
    withHive {
      val catalog = ResolvedCatalog(
        name = "lake",
        `type` = "iceberg",
        sparkCatalogName = "lake",
        sessionCatalog = false,
        sparkConf = Map.empty,
        properties = Map.empty)
      intercept[PuddingException] {
        new HiveSource().initData(
          sparkSession,
          SourceContext(Some(Map("table" -> table)), catalog = Some(catalog)))
      }
    }
  }

  test("filter hive table by conditionExpr") {
    withHive {
      val config = Some(Map("table" -> table, "conditionExpr" -> "age > 21"))
      val dataFrame = new HiveSource().initData(sparkSession, config)
      dataFrame.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(2, 3)
    }
  }

  test("force convert schema") {
    withHive {
      val config = Some(Map(
        "table" -> table,
        "forceConvertSchema" -> true,
        "schema" -> Map("id" -> "string", "age" -> "double")
      ))
      val dataFrame = new HiveSource().initData(sparkSession, config)

      dataFrame.schema.fieldNames.toSet shouldBe Set("id", "age")
      dataFrame.schema("id").dataType shouldBe StringType
      dataFrame.schema("age").dataType shouldBe DoubleType
      dataFrame.count() shouldBe 3
    }
  }

  test("null config throws PuddingException") {
    withHive {
      intercept[PuddingException] {
        new HiveSource().initData(sparkSession, None)
      }
    }
  }

  test("missing table config throws PuddingException") {
    withHive {
      intercept[PuddingException] {
        new HiveSource().initData(sparkSession, Some(Map.empty[String, Any]))
      }
    }
  }

  test("force convert schema without schema config throws PuddingException") {
    withHive {
      intercept[PuddingException] {
        new HiveSource().initData(
          sparkSession,
          Some(Map("table" -> table, "forceConvertSchema" -> true))
        )
      }
    }
  }

  test("accept a string boolean and reject invalid config types") {
    withHive {
      val dataFrame = new HiveSource().initData(
        sparkSession,
        Some(Map(
          "table" -> table,
          "forceConvertSchema" -> "TRUE",
          "schema" -> Map("id" -> "string"))))
      dataFrame.schema.fieldNames.toSeq shouldBe Seq("id")

      intercept[PuddingException] {
        new HiveSource().initData(
          sparkSession,
          Some(Map("table" -> table, "forceConvertSchema" -> "yes")))
      }
      intercept[PuddingException] {
        new HiveSource().initData(
          sparkSession,
          Some(Map("table" -> 1)))
      }
      intercept[PuddingException] {
        new HiveSource().initData(
          sparkSession,
          Some(Map(
            "table" -> table,
            "forceConvertSchema" -> true,
            "schema" -> "id:string")))
      }
      intercept[PuddingException] {
        new HiveSource().initData(
          sparkSession,
          Some(Map(
            "table" -> table,
            "conditionExpr" -> 42)))
      }
    }
  }
}
