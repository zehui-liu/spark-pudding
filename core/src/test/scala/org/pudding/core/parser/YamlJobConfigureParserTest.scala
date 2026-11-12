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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.pudding.core.PuddingException
import org.pudding.core.configure.{JobMode, SyncStrategy, TransformConf}

import scala.collection.immutable._
import scala.io.Source

/**
 * Parser coverage for YAML job files, including nested maps and validation errors.
 */
class YamlJobConfigureParserTest extends AnyFunSuite with Matchers {

  val yamlJobConfigureParser = new YamlJobConfigureParser()

  test("test method parse") {
    val jobPipelineCfg = new YamlJobConfigureParser()
      .parseFromString(Source.fromResource("spark-pudding-test.yaml").mkString)
    val sources = jobPipelineCfg.sources
    val transforms = jobPipelineCfg.transforms
    val sinks = jobPipelineCfg.sinks
    val job = jobPipelineCfg.job

    assert(sources.size == 2)
    assert(sources.map(_.`type`).toList == List("hive", "hdfs"))
    assert(sources.head.config.get.size == 5)

    assert(transforms.get.isInstanceOf[Seq[TransformConf]])
    val tfs = transforms.get
    assert(tfs.size == 2)
    assert(tfs.map(_.`type`).toList == List("filter", "map"))
    assert(tfs.head.dependencies == List("source1"))
    assert(tfs(1).dependencies == List("transform1", "source2"))

    assert(sinks.size == 1)
    assert(sinks.head.`type` == "mysql")
    assert(sinks.head.config.get.size == 6)
    assert(sinks.head.dependency == "transform2")

    assert(job.config.nonEmpty)
    assert(job.config.get.size == 2)
    assert(job.describe.nonEmpty)
    assert(job.describe.get == "spark test job")
  }

  test("test method parse without transform") {
    val jobPipelineCfg = new YamlJobConfigureParser().parseFromString(
      Source.fromResource("spark-pudding-test-withoutTransform.yaml").mkString)

    assert(jobPipelineCfg.transforms.isEmpty)
  }

  test("test target-supplied job settings and optional descriptions are optional") {
    val conf =
      """
        |job:
        |  mode: batch
        |  appName: minimal job
        |
        |source:
        |  - type: file-common-source
        |    id: source-1
        |
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val jobPipelineCfg = new YamlJobConfigureParser().parseFromString(conf)

    assert(jobPipelineCfg.job.sparkHome == "")
    assert(jobPipelineCfg.job.sparkPuddingHome.isEmpty)
    assert(jobPipelineCfg.job.master == "")
    assert(jobPipelineCfg.job.deployMode == "")
    assert(jobPipelineCfg.job.describe.isEmpty)
    assert(jobPipelineCfg.sources.head.describe.isEmpty)
    assert(jobPipelineCfg.sinks.head.describe.isEmpty)
  }

  test("normalize typed job config values to strings") {
    val conf =
      """
        |job:
        |  mode: batch
        |  appName: typed config
        |  config:
        |    spark.executor.instances: 2
        |    spark.sql.adaptive.enabled: true
        |
        |source:
        |  - type: file-common-source
        |    id: source-1
        |
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val jobPipelineCfg = new YamlJobConfigureParser().parseFromString(conf)

    jobPipelineCfg.job.config shouldBe Some(Map(
      "spark.executor.instances" -> "2",
      "spark.sql.adaptive.enabled" -> "true"))
  }

  test("reject a non-map job config") {
    val conf =
      """
        |job:
        |  mode: batch
        |  appName: invalid config
        |  config: not-a-map
        |
        |source:
        |  - type: file-common-source
        |    id: source-1
        |
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val error = intercept[org.pudding.core.PuddingException] {
      new YamlJobConfigureParser().parseFromString(conf)
    }
    error.getMessage should include ("config must be a map")
  }

  test("reject malformed YAML and non-list sections") {
    intercept[PuddingException] {
      new YamlJobConfigureParser().parseFromString("not: [a yaml document")
    }

    val nonListSource = intercept[PuddingException] {
      new YamlJobConfigureParser().parseFromString(
        """
          |job:
          |  mode: batch
          |  appName: invalid source
          |source:
          |  type: file-common-source
          |sink:
          |  - type: console-sink
          |    id: sink-1
          |    dependency: source-1
          |""".stripMargin)
    }
    nonListSource.getMessage should include ("source must be a list")

    val nonMapSource = intercept[PuddingException] {
      new YamlJobConfigureParser().parseFromString(
        """
          |job:
          |  mode: batch
          |  appName: invalid source
          |source:
          |  - file-common-source
          |sink:
          |  - type: console-sink
          |    id: sink-1
          |    dependency: source-1
          |""".stripMargin)
    }
    nonMapSource.getMessage should include ("source[0] entries must be a map")

    val nonMapJob = intercept[PuddingException] {
      new YamlJobConfigureParser().parseFromString(
        """
          |job: not-a-map
          |source:
          |  - type: file-common-source
          |    id: source-1
          |sink:
          |  - type: console-sink
          |    id: sink-1
          |    dependency: source-1
          |""".stripMargin)
    }
    nonMapJob.getMessage should include ("job must be a map")
  }

  test("require job.mode") {
    val error = intercept[PuddingException] {
      new YamlJobConfigureParser().parseFromString(
        """
          |job:
          |  appName: missing mode
          |source:
          |  - type: file-common-source
          |    id: source-1
          |sink:
          |  - type: console-sink
          |    id: sink-1
          |    dependency: source-1
          |""".stripMargin)
    }
    error.getMessage should include ("job.mode is required")
  }

  private def modeConfig(jobFields: String): String = {
    s"""
       |job:
       |  mode: batch
       |  appName: mode test
       |$jobFields
       |source:
       |  - type: file-common-source
       |    id: source-1
       |sink:
       |  - type: console-sink
       |    id: sink-1
       |    dependency: source-1
       |""".stripMargin
  }

  test("parse job mode, strategy and trigger settings") {
    val streaming = new YamlJobConfigureParser().parseFromString(
      modeConfig("  mode: streaming\n  trigger: available-now"))
    streaming.job.mode shouldBe JobMode.Streaming
    streaming.job.trigger shouldBe Some("available-now")
    streaming.job.strategy shouldBe None

    val sync = new YamlJobConfigureParser().parseFromString(
      modeConfig("  mode: sync\n  strategy: incremental"))
    sync.job.mode shouldBe JobMode.Sync
    sync.job.strategy shouldBe Some(SyncStrategy.Incremental)
    sync.job.trigger shouldBe None
  }

  test("reject invalid job mode, strategy and trigger settings") {
    def parse(jobFields: String): Unit = {
      new YamlJobConfigureParser().parseFromString(modeConfig(jobFields))
    }

    val invalidMode = intercept[PuddingException] {
      parse("  mode: merge")
    }
    invalidMode.getMessage should include ("unsupported job.mode")

    val legacyMode = intercept[PuddingException] {
      parse("  mode: etl")
    }
    legacyMode.getMessage should include ("no longer supported")

    val invalidStrategy = intercept[PuddingException] {
      parse("  mode: sync\n  strategy: merge")
    }
    invalidStrategy.getMessage should include ("unsupported job.strategy")

    val legacyExecution = intercept[PuddingException] {
      parse(
        """  mode: batch
          |  execution:
          |    mode: streaming""".stripMargin)
    }
    legacyExecution.getMessage should include ("job.execution is no longer supported")

    val emptyTrigger = intercept[PuddingException] {
      parse("  mode: streaming\n  trigger: '  '")
    }
    emptyTrigger.getMessage should include ("trigger can not be empty")
  }

  test("test appName has a default when omitted") {
    val conf =
      """
        |job:
        |  mode: batch
        |
        |source:
        |  - type: file-common-source
        |    id: source-1
        |
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val jobPipelineCfg = new YamlJobConfigureParser().parseFromString(conf)

    assert(jobPipelineCfg.job.appName == "spark-pudding")
  }

  test("reject the removed job.enableHiveSupport key") {
    val conf =
      """
        |job:
        |  mode: batch
        |  enableHiveSupport: true
        |
        |source:
        |  - type: file-common-source
        |    id: source-1
        |
        |sink:
        |  - type: console-sink
        |    id: sink-1
        |    dependency: source-1
        |""".stripMargin

    val error = intercept[PuddingException] {
      new YamlJobConfigureParser().parseFromString(conf)
    }
    error.getMessage should include ("job.enableHiveSupport is no longer supported")
    error.getMessage should include ("spark.sql.catalogImplementation=hive")
  }

}
