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
import org.pudding.core.configure.TransformConf

import scala.collection.immutable._
import scala.io.Source

/**
 * test for YamlJobConfigureParser class
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

}
