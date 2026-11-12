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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.Date

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.pudding.core.CommonTestUtils
import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.SourceContext

/**
 * Reads files through `file-common-source`, including batch, streaming and schema conversion.
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
    assert(schema.toDDL.replace("`", "") == "a1 STRING,a2 DOUBLE,a3 INT,a4 DATE")

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
    assert(schema.toDDL.replace("`", "") == "a1 STRING,a2 DOUBLE,a3 INT,a4 DATE")

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
    assert(schema.toDDL.replace("`", "") == "a1 STRING,a2 DOUBLE,a3 INT,a4 DATE")

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
    assert(schema.toDDL.replace("`", "") == "a1 STRING,a2 DOUBLE,a3 INT,a4 DATE")

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
    assert(schema.toDDL.replace("`", "") == "a1 STRING,a2 DOUBLE,a3 INT,a4 DATE")

    val rows = dataFrame.collect()
    assert(rows.head.get(0) == "zhangSan")
    assert(rows.head.get(1) == 3.1415926)
    assert(rows.head.get(2) == 1)
    assert(rows.head.get(3) == Date.valueOf("2024-11-21"))
  }

  private def withTempDirectory(test: Path => Unit): Unit = {
    val directory = Files.createTempDirectory("pudding-file-source-")
    try {
      test(directory)
    } finally {
      deleteRecursively(directory)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val children = Files.list(path)
      try {
        children.iterator().asScala.foreach(deleteRecursively)
      } finally {
        children.close()
      }
    }
    Files.deleteIfExists(path)
  }

  private def collectStream(dataFrame: DataFrame, queryName: String): Seq[Row] = {
    val query = dataFrame.writeStream
      .format("memory")
      .queryName(queryName)
      .start()
    try {
      query.processAllAvailable()
      sparkSession.sql(s"select * from $queryName").collect().toSeq
    } finally {
      query.stop()
    }
  }

  test("accept boolean strings for streamRead and forceConvertSchema") {
    val config = mockCsvConfig.get ++ Map(
      "streamRead" -> "false",
      "forceConvertSchema" -> "true")

    val dataFrame = new FileSource().initData(sparkSession, Some(config))

    dataFrame.isStreaming shouldBe false
    dataFrame.schema.fieldNames.toSet shouldBe Set("a1", "a2", "a3", "a4")
  }

  test("explicit batch execution overrides connector streamRead") {
    val config = mockCsvConfig.get + ("streamRead" -> true)

    val dataFrame = new FileSource().initData(
      sparkSession,
      SourceContext(Some(config), executionMode = Some(ExecutionMode.Batch)))

    dataFrame.isStreaming shouldBe false
    dataFrame.count() shouldBe 3
  }

  test("reject invalid file source configuration early") {
    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(Map(
        "path" -> getClass.getClassLoader.getResource("csv/").getFile,
        "fileType" -> "csv",
        "streamRead" -> "sometimes")))
    }

    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(Map(
        "path" -> "",
        "fileType" -> "csv")))
    }

    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(Map(
        "path" -> getClass.getClassLoader.getResource("csv/").getFile,
        "fileType" -> "xml")))
    }

    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(Map(
        "path" -> getClass.getClassLoader.getResource("csv/").getFile,
        "fileType" -> "csv",
        "schema" -> "not-a-map")))
    }

    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(mockTextConfig.get +
        ("textDelimiter" -> "")))
    }

    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(Map(
        "path" -> 1,
        "fileType" -> "csv")))
    }

    intercept[PuddingException] {
      new FileSource().initData(sparkSession, Some(Map(
        "path" -> getClass.getClassLoader.getResource("csv/").getFile,
        "fileType" -> true)))
    }
  }

  test("test csv stream with configured schema") {
    withTempDirectory { directory =>
      Files.write(
        directory.resolve("part-0.csv"),
        "name,value\nalpha,1\nbeta,2\n".getBytes(StandardCharsets.UTF_8))

      val config = Some(Map(
        "path" -> directory.toString,
        "fileType" -> "csv",
        "streamRead" -> true,
        "options" -> Map("header" -> "true"),
        "schema" -> Map(
          "name" -> "string",
          "value" -> "int"
        )
      ))

      val dataFrame = new FileSource().initData(sparkSession, config)
      assert(dataFrame.isStreaming)
      assert(dataFrame.schema.fieldNames.toSet == Set("name", "value"))

      val rows = collectStream(dataFrame, "file_source_csv_stream")
      val values = rows.map(row => (row.getAs[String]("name"), row.getAs[Int]("value"))).toSet
      assert(values == Set(("alpha", 1), ("beta", 2)))
    }
  }

  test("test text stream with schema conversion") {
    withTempDirectory { directory =>
      Files.write(
        directory.resolve("part-0.txt"),
        "zhangSan,3.1415926,1,2024-11-21\n".getBytes(StandardCharsets.UTF_8))

      val config = Some(Map(
        "path" -> directory.toString,
        "fileType" -> "text",
        "streamRead" -> true,
        "forceConvertSchema" -> true,
        "options" -> Map("encoding" -> "UTF-8"),
        "schema" -> Map(
          "a1" -> "string",
          "a2" -> "double",
          "a3" -> "int",
          "a4" -> "date"
        )
      ))

      val dataFrame = new FileSource().initData(sparkSession, config)
      assert(dataFrame.isStreaming)
      assert(dataFrame.schema.fieldNames.toSet == Set("a1", "a2", "a3", "a4"))

      val row = collectStream(dataFrame, "file_source_text_stream").head
      assert(row.getAs[String]("a1") == "zhangSan")
      assert(row.getAs[Double]("a2") == 3.1415926)
      assert(row.getAs[Int]("a3") == 1)
      assert(row.getAs[Date]("a4") == Date.valueOf("2024-11-21"))
    }
  }

  test("test csv stream requires a schema") {
    withTempDirectory { directory =>
      val config = Some(Map(
        "path" -> directory.toString,
        "fileType" -> "csv",
        "streamRead" -> true
      ))

      val error = intercept[PuddingException] {
        new FileSource().initData(sparkSession, config)
      }
      assert(error.getMessage.contains("requires a configured schema"))
    }
  }

  test("test stream read rejects multiple paths") {
    withTempDirectory { directory =>
      val config = Some(Map(
        "path" -> s"${directory.resolve("first")},${directory.resolve("second")}",
        "fileType" -> "csv",
        "streamRead" -> true,
        "schema" -> Map("name" -> "string")
      ))

      val error = intercept[PuddingException] {
        new FileSource().initData(sparkSession, config)
      }
      assert(error.getMessage.contains("exactly one path"))
    }
  }
}
