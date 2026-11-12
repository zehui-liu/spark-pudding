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

package org.pudding.core.parser

import org.pudding.core.PuddingException
import org.pudding.core.configure.TransformConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

/**
 * Parser coverage for HOCON job files, including nested maps and validation errors.
 */
class HoconJobConfigureParserTest extends AnyFunSuite with Matchers {

  private val hoconJobConfigureParser = new HoconJobConfigureParser()

  private def readConf(resource: String): String = Source.fromResource(resource).mkString

  test("test method parse") {
    val jobPipelineCfg = hoconJobConfigureParser.parseFromString(readConf("spark-pudding-test.conf"))
    val sources = jobPipelineCfg.sources
    val transforms = jobPipelineCfg.transforms
    val sinks = jobPipelineCfg.sinks
    val job = jobPipelineCfg.job

    sources.size should be(2)
    sources.map(_.`type`).toList should be(List("hive", "hdfs"))
    sources.head.config.get.size should be(5)

    transforms.isDefined should be(true)
    transforms.get.isInstanceOf[Seq[TransformConf]] should be(true)
    transforms.get.size should be(2)
    transforms.get.map(_.`type`).toList should be(List("filter", "map"))
    transforms.get.head.dependencies should be(List("source1"))
    transforms.get(1).dependencies should be(List("transform1", "source2"))

    sinks.size should be(1)
    sinks.head.`type` should be("mysql")
    sinks.head.config.get.size should be(6)
    sinks.head.dependency should be("transform2")

    job.config.isDefined should be(true)
    job.config.get.size should be(2)
    job.config.get("spark.master") should be("local[*]")
    job.describe should be(Some("spark test job"))
  }

  test("test method parse without transform") {
    val jobPipelineCfg = hoconJobConfigureParser.parseFromString(
      readConf("spark-pudding-test-withoutTransform.conf"))

    jobPipelineCfg.transforms.isEmpty should be(true)
  }

  test("test method parse with complex transform dependencies") {
    val jobPipelineCfg = hoconJobConfigureParser.parseFromString(
      readConf("spark-pudding-test-complex.conf"))
    val transforms = jobPipelineCfg.transforms

    jobPipelineCfg.sources.size should be(3)
    transforms.isDefined should be(true)
    transforms.get.size should be(3)
    transforms.get.map(_.id).toList should be(List("transform_1", "transform_2", "transform_3"))
    transforms.get.head.dependencies should be(List("source1", "source2"))
    transforms.get(1).dependencies should be(List("transform_1", "source3"))
    transforms.get(2).dependencies should be(List("transform_2"))
    transforms.get.head.config.get("joinColumn") should be("id")
    jobPipelineCfg.sinks.head.dependency should be("transform_3")
  }

  test("test parse config value types") {
    val jobPipelineCfg = hoconJobConfigureParser.parseFromString(readConf("spark-pudding-test.conf"))
    val sourceConfig = jobPipelineCfg.sources.head.config.get
    sourceConfig("hostname") should be("localhost")
    sourceConfig("port") should be(3306)

    val sinkConfig = jobPipelineCfg.sinks.head.config.get
    sinkConfig("port") should be(3306)
    sinkConfig("database") should be("test")
  }

  test("normalize typed job config values to strings") {
    val jobPipelineCfg = hoconJobConfigureParser.parseFromString(
      """
        |job {
        |  mode = "batch"
        |  appName = "typed config"
        |  config {
        |    spark.executor.instances = 2
        |    spark.sql.adaptive.enabled = true
        |  }
        |}
        |source = [{ type = "file-common-source", id = "source-1" }]
        |sink = [{ type = "console-sink", id = "sink-1", dependency = "source-1" }]
        |""".stripMargin)

    jobPipelineCfg.job.config shouldBe Some(Map(
      "spark.executor.instances" -> "2",
      "spark.sql.adaptive.enabled" -> "true"))
  }

  test("flatten nested connection properties to dotted keys") {
    val jobPipelineCfg = hoconJobConfigureParser.parseFromString(
      """
        |job { mode = "batch", appName = "nested properties" }
        |connection = [{
        |  name = "mongo"
        |  type = "mongodb"
        |  properties {
        |    spark.mongodb.read.connection.uri = "mongodb://mongo:27017"
        |  }
        |}]
        |source = [{ type = "file-common-source", id = "source-1" }]
        |sink = [{ type = "console-sink", id = "sink-1", dependency = "source-1" }]
        |""".stripMargin)

    jobPipelineCfg.connections.head.properties shouldBe Map(
      "spark.mongodb.read.connection.uri" -> "mongodb://mongo:27017")
  }

  test("reject malformed HOCON with a PuddingException") {
    val error = intercept[PuddingException] {
      hoconJobConfigureParser.parseFromString("job { appName = ")
    }
    error.getMessage should include ("parser hocon file error")
  }

  test("test missing source throws PuddingException") {
    val conf =
      """
        |job {
        |  mode = "batch"
        |  appName = "spark test job"
        |  sparkHome = "/tmp/spark"
        |  master = "local[*]"
        |  deployMode = "client"
        |}
        |""".stripMargin

    an[PuddingException] should be thrownBy hoconJobConfigureParser.parseFromString(conf)
  }
}
