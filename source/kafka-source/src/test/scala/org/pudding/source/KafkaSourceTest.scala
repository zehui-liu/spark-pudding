package org.pudding.source

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 *
 */
class KafkaSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll{

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("test for hdfs source")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.close()
  }

  test("stream read data from kafka") {
    // todo
  }

  test("batch read data from kafka") {
    // todo
  }


}
