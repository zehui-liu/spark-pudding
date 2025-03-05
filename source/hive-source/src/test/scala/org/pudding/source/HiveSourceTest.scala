/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

import org.apache.spark.sql.SparkSession
import org.pudding.core.CommonTestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * test for HiveSource
 */
class HiveSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll{

  private val utils = new CommonTestUtils("test for hive source")

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession

    sparkSession.sql("use default")


    val createTableSql = """create table test (
                           |    a int,
                           |    b string
                           |)""".stripMargin
    sparkSession.sql(createTableSql)

    val insertTableSql = """insert into
                           |  test
                           |select
                           |  1 as a,
                           |  "dd" as b""".stripMargin
    sparkSession.sql(insertTableSql)

    val queryTableSql = "select * from default.test"
    sparkSession.sql(queryTableSql).show()

  }

  override def afterAll(): Unit = {
    utils.closeSparkSession()
  }

  private val mockHiveSourceConfig = Some(Map(
    "table" -> ""
  ))


  test("link hive metastore") {

    sparkSession.sql("show databases").show()



  }



}
