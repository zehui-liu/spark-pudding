package org.pudding.source

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Date

/**
 * test for HdfsSource
 */
class FileSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("test for hdfs source")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.close()
  }

  private val mockCsvConfig = Some(Map(
    "path" -> getClass.getClassLoader.getResource("test.csv").getFile,
    "fileType" -> "csv",
    "forceConvertSchema" -> "true",
    "options" -> null,
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
    "path" -> getClass.getClassLoader.getResource("test.text").getFile,
    "fileType" -> "text",
    "forceConvertSchema" -> "true",
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
    "path" -> getClass.getClassLoader.getResource("test.json").getFile,
    "fileType" -> "json",
    "forceConvertSchema" -> "true",
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

  private val mockHdfsJsonConfig = Some(Map(
    "path" -> "hdfs://localhost:9000/tmp/spark/test.json",
    "fileType" -> "json",
    "forceConvertSchema" -> "true",
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
    val dataFrame = new FileSource().initData(sparkSession, mockHdfsJsonConfig)

    dataFrame.printSchema()
    dataFrame.show()

  }

  test("test parquet file") {

  }


}
