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

package org.pudding.sink

import jodd.io.FileUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.CommonTestUtils

import java.nio.file.Paths

/**
 * test for FileSink
 */
class FileSinkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val utils = new CommonTestUtils("test for file sink")
  var sparkSession: SparkSession = _

  var mockDataFrame :DataFrame = _

  case class Age(age: Int, id: Int)

  override def beforeAll(): Unit = {
    utils.initLocalSparkSession()
    sparkSession = utils.sparkSession
    mockDataFrame = sparkSession.read.json(getClass.getClassLoader.getResource("test.json").getFile)
  }

  override def afterAll(): Unit = {
    utils.closeSparkSession()
  }

  private val mockConfig = Some(Map(
    "path" -> s"${Paths.get("").toAbsolutePath.toString}/sink/file-common-sink/src/test/resources/temp",
    "fileType" -> "parquet",
    "saveMode" -> "Overwrite",
  ))


  test("test file sink") {
    new FileSink().writer(mockDataFrame,mockConfig)
    println(mockConfig.get("path"))
    FileUtil.deleteDir(mockConfig.get("path"))
  }

}
