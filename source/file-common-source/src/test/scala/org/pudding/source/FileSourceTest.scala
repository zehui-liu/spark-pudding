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

import java.sql.Date

/**
 * test for HdfsSource
 */
class FileSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for file source")

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession
  }

  override def afterAll(): Unit = {
    utils.closeSparkSession()
  }

  private val mockCsvConfig = Some(Map(
    "path" -> getClass.getClassLoader.getResource("csv/").getFile,
    "fileType" -> "csv",
    "forceConvertSchema" -> true,
    "options" -> Map("header" -> "true"),
    "schema" -> Map(
      "a1" -> "string",
      "a2" -> "double",
      "a3" -> "int",
      "a4" -> "date"
    )
  ))

  test("test csv file") {
    val dataFrame = new FileSource().initData(sparkSession, mockCsvConfig)
    assert(dataFrame.count() == 3)

    val schema = dataFrame.schema
    val cfg = mockCsvConfig.get
    assert(schema.size == cfg("schema").asInstanceOf[Map[String, String]].size)
    assert(schema.toDDL == "`a1` STRING,`a2` DOUBLE,`a3` INT,`a4` DATE")

    val rows = dataFrame.collect()
    assert(rows.head.get(0) == "zhangSan")
    assert(rows.head.get(1) == 3.1415926)
    assert(rows.head.get(2) == 1)
    assert(rows.head.get(3) == Date.valueOf("2024-11-21"))

  }

  private val mockTextConfig = Some(Map(
    "path" -> getClass.getClassLoader.getResource("text/").getFile,
    "fileType" -> "text",
    "forceConvertSchema" -> true,
    "options" -> Map(
      "encoding" -> "UTF-8"
    ),
    "schema" -> Map(
      "a1" -> "string",
      "a2" -> "double",
      "a3" -> "int",
      "a4" -> "date"
    )
  ))

  test("test text file") {
    val dataFrame = new FileSource().initData(sparkSession, mockTextConfig)
    val schema = dataFrame.schema
    val cfg = mockTextConfig.get

    assert(schema.size == cfg("schema").asInstanceOf[Map[String, String]].size)
    assert(schema.toDDL == "`a1` STRING,`a2` DOUBLE,`a3` INT,`a4` DATE")

    val rows = dataFrame.collect()
    assert(rows.head.get(0) == "zhangSan")
    assert(rows.head.get(1) == 3.1415926)
    assert(rows.head.get(2) == 1)
    assert(rows.head.get(3) == Date.valueOf("2024-11-21"))
  }

  private val mockJsonConfig = Some(Map(
    "path" -> getClass.getClassLoader.getResource("json/").getFile,
    "fileType" -> "json",
    "forceConvertSchema" -> true,
    "options" -> Map(
      "encoding" -> "UTF-8"
    ),
    "schema" -> Map(
      "a1" -> "string",
      "a2" -> "double",
      "a3" -> "int",
      "a4" -> "date"
    )
  ))

  test("test json file") {
    val dataFrame = new FileSource().initData(sparkSession, mockJsonConfig)

    val schema = dataFrame.schema
    val cfg = mockTextConfig.get

    assert(schema.size == cfg("schema").asInstanceOf[Map[String, String]].size)
    assert(schema.toDDL == "`a1` STRING,`a2` DOUBLE,`a3` INT,`a4` DATE")

    val rows = dataFrame.collect()
    assert(rows.head.get(0) == "zhangSan")
    assert(rows.head.get(1) == 3.1415926)
    assert(rows.head.get(2) == 1)
    assert(rows.head.get(3) == Date.valueOf("2024-11-21"))
  }

  private val mockOrcConfig = Some(Map(
    "path" -> getClass.getClassLoader.getResource("orc/").getFile,
    "fileType" -> "orc",
    "forceConvertSchema" -> true,
    "options" -> Map(
      "encoding" -> "UTF-8"
    ),
    "schema" -> Map(
      "a1" -> "string",
      "a2" -> "double",
      "a3" -> "int",
      "a4" -> "date"
    )
  ))

  test("test orc file") {
    val dataFrame = new FileSource().initData(sparkSession, mockOrcConfig)
    val schema = dataFrame.schema
    val cfg = mockTextConfig.get

    assert(schema.size == cfg("schema").asInstanceOf[Map[String, String]].size)
    assert(schema.toDDL == "`a1` STRING,`a2` DOUBLE,`a3` INT,`a4` DATE")

    val rows = dataFrame.collect()
    assert(rows.head.get(0) == "zhangSan")
    assert(rows.head.get(1) == 3.1415926)
    assert(rows.head.get(2) == 1)
    assert(rows.head.get(3) == Date.valueOf("2024-11-21"))
  }

  private val mockParquetConfig = Some(Map(
    "path" -> getClass.getClassLoader.getResource("parquet/").getFile,
    "fileType" -> "parquet",
    "forceConvertSchema" -> true,
    "options" -> Map(
      "encoding" -> "UTF-8"
    ),
    "schema" -> Map(
      "a1" -> "string",
      "a2" -> "double",
      "a3" -> "int",
      "a4" -> "date"
    )
  ))

  test("test parquet file") {
    val dataFrame = new FileSource().initData(sparkSession, mockParquetConfig)
    val schema = dataFrame.schema
    val cfg = mockTextConfig.get

    assert(schema.size == cfg("schema").asInstanceOf[Map[String, String]].size)
    assert(schema.toDDL == "`a1` STRING,`a2` DOUBLE,`a3` INT,`a4` DATE")

    val rows = dataFrame.collect()
    assert(rows.head.get(0) == "zhangSan")
    assert(rows.head.get(1) == 3.1415926)
    assert(rows.head.get(2) == 1)
    assert(rows.head.get(3) == Date.valueOf("2024-11-21"))
  }


}
