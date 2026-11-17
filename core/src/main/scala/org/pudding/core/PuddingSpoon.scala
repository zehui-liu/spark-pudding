/*
 * Copyright 2024-2026 the spark-pudding authors
 *
 * Licensed under the Apache License, Version 2.0
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

package org.pudding.core

import org.rogach.scallop.{ScallopConf, ScallopOption}

import org.pudding.core.spark.BuildJob

class Conf(arguments: Array[String]) extends ScallopConf(arguments) {
  val confType: ScallopOption[String] = opt[String](
    name = "confType",
    required = true,
    descr = "configure file type"
  )
  val jobText: ScallopOption[String] = opt[String](
    name = "jobText",
    required = false,
    descr = "configuration text; mutually exclusive with --jobEnv and --jobFile"
  )
  val jobEnv: ScallopOption[String] = opt[String](
    name = "jobEnv",
    required = false,
    descr = "environment variable containing the configuration; mutually exclusive with " +
      "--jobText and --jobFile"
  )
  val jobFile: ScallopOption[String] = opt[String](
    name = "jobFile",
    required = false,
    descr = "path to a configuration file; mutually exclusive with --jobText and --jobEnv"
  )
  verify()
}

/**
 * Command-line entry point that resolves the job text and builds the pipeline for submission.
 */
object PuddingSpoon {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val jobText = JobConfigSource.resolve(
      conf.jobText.toOption,
      conf.jobEnv.toOption,
      conf.jobFile.toOption)
    val jobPipelineConf = JobConfigureParser(conf.confType()).parseFromString(jobText)
    BuildJob.buildSparkJob(jobPipelineConf)
  }

}
