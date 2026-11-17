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

package org.pudding.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * test for SqlTransform
 */
class SqlTransformTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("test for sql transform")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.close()
    }
  }

  private def orders(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1001, 1, 10.0),
      (1002, 2, 20.0),
      (1003, 1, 30.0),
      (1004, 3, 99.0)
    ).toDF("order_id", "user_id", "amount")
  }

  private def users(): DataFrame = {
    val session = sparkSession
    import session.implicits._
    Seq(
      (1, "tom", "beijing", 20),
      (2, "jerry", "shanghai", 25),
      (3, "kid", "beijing", 16)
    ).toDF("user_id", "name", "city", "age")
  }

  test("execute sql for a single dependency using tempView") {
    val config = Some(Map(
      "tempView" -> "orders_view",
      "sql" -> "select order_id, amount from orders_view where amount >= 10 order by order_id"
    ))

    val result = new SqlTransform().handle(Map("source-1" -> orders()), config)

    result.collect().map(row => (row.getInt(0), row.getDouble(1))).toSeq shouldBe
      Seq((1001, 10.0), (1002, 20.0), (1003, 30.0), (1004, 99.0))
  }

  test("join and aggregate multiple dependencies using tempViews") {
    val config = Some(Map(
      "tempViews" -> Map(
        "source-1" -> "orders",
        "source-2" -> "users"
      ),
      "sql" ->
        """
          |select u.city, count(*) as order_cnt, sum(o.amount) as total_amount
          |from orders o
          |join users u on o.user_id = u.user_id
          |where u.age >= 18 and o.amount >= 10
          |group by u.city
          |order by u.city
          |""".stripMargin
    ))

    val result = new SqlTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      config
    )

    result.collect().map(row => (row.getString(0), row.getLong(1), row.getDouble(2))).toSeq shouldBe
      Seq(("beijing", 2L, 40.0), ("shanghai", 1L, 20.0))
  }

  test("run a stateless SQL projection on a streaming dependency") {
    val streaming = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()
    val result = new SqlTransform().handle(
      Map("source-1" -> streaming),
      Some(Map(
        "sql" -> "select value from source_1 where value >= 0")))

    result.isStreaming shouldBe true
    result.schema.fieldNames.toSeq shouldBe Seq("value")
  }

  test("derive temp view names from dependency ids") {
    val config = Some(Map(
      "sql" ->
        """
          |select count(*) as cnt
          |from source_1 o
          |join source_2 u on o.user_id = u.user_id
          |""".stripMargin
    ))

    val result = new SqlTransform().handle(
      Map("source-1" -> orders(), "source-2" -> users()),
      config
    )

    result.collect().head.getLong(0) shouldBe 4L
  }

  test("apply numPartitions to the sql result") {
    val config = Some(Map(
      "tempView" -> "orders_partition_view",
      "sql" -> "select * from orders_partition_view",
      "numPartitions" -> BigInt(3)
    ))

    val result = new SqlTransform().handle(Map("source1" -> orders()), config)

    result.rdd.getNumPartitions shouldBe 3
  }

  test("numPartitions supports java integer") {
    val config = Some(Map(
      "tempView" -> "orders_java_integer_view",
      "sql" -> "select * from orders_java_integer_view",
      "numPartitions" -> java.lang.Integer.valueOf(2)
    ))

    val result = new SqlTransform().handle(Map("source1" -> orders()), config)

    result.rdd.getNumPartitions shouldBe 2
  }

  test("tempView with multiple dependencies throws PuddingException") {
    val config = Some(Map(
      "tempView" -> "orders",
      "sql" -> "select * from orders"
    ))

    val error = intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders(), "source-2" -> users()), config)
    }
    error.getMessage should include("tempViews")
  }

  test("tempViews must cover every dependency") {
    val config = Some(Map(
      "tempViews" -> Map("source-1" -> "orders"),
      "sql" -> "select * from orders"
    ))

    val error = intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders(), "source-2" -> users()), config)
    }
    error.getMessage should include("missing: source-2")
  }

  test("tempViews can not contain unknown dependencies") {
    val config = Some(Map(
      "tempViews" -> Map(
        "source-1" -> "orders",
        "source-2" -> "users",
        "source-3" -> "extra"
      ),
      "sql" -> "select * from orders"
    ))

    val error = intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders(), "source-2" -> users()), config)
    }
    error.getMessage should include("unknown dependencies: source-3")
  }

  test("tempView and tempViews can not be used together") {
    val config = Some(Map(
      "tempView" -> "orders",
      "tempViews" -> Map("source-1" -> "orders_view"),
      "sql" -> "select * from orders_view"
    ))

    intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders()), config)
    }
  }

  test("duplicated temp view names throw PuddingException") {
    val config = Some(Map(
      "tempViews" -> Map(
        "source-1" -> "same_view",
        "source-2" -> "same_view"
      ),
      "sql" -> "select * from same_view"
    ))

    val error = intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders(), "source-2" -> users()), config)
    }
    error.getMessage should include("same_view")
  }

  test("invalid temp view name throws PuddingException") {
    val config = Some(Map(
      "tempView" -> "bad-view-name",
      "sql" -> "select * from `bad-view-name`"
    ))

    val error = intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders()), config)
    }
    error.getMessage should include("invalid temp view name")
  }

  test("empty sql throws PuddingException") {
    val config = Some(Map(
      "tempView" -> "orders_view",
      "sql" -> " "
    ))

    val error = intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders()), config)
    }
    error.getMessage should include("sql can not be empty")
  }

  test("missing sql throws PuddingException") {
    intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders()), Some(Map("tempView" -> "orders_view")))
    }
  }

  test("empty dataframes throws PuddingException") {
    val config = Some(Map(
      "tempView" -> "orders_view",
      "sql" -> "select 1"
    ))

    intercept[PuddingException] {
      new SqlTransform().handle(Map.empty, config)
    }
  }

  test("non-positive numPartitions throws PuddingException") {
    val config = Some(Map(
      "tempView" -> "orders_view",
      "sql" -> "select * from orders_view",
      "numPartitions" -> 0
    ))

    intercept[PuddingException] {
      new SqlTransform().handle(Map("source-1" -> orders()), config)
    }
  }
}
