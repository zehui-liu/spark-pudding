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

package org.pudding.core.spark

import org.pudding.core.parser.YamlJobConfigureParser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

/**
 * test buildJob method
 */
class BuildJobTest extends AnyFunSuite with Matchers with BeforeAndAfterAll{

  test("test for method buildSparkJob") {
    val yamlStr = Source.fromResource("spark-pudding-test-mock.yaml").mkString
    val jobPipelineCfg = new YamlJobConfigureParser().parse(yamlStr)
    BuildJob.buildSparkJob(jobPipelineCfg)
  }
}
