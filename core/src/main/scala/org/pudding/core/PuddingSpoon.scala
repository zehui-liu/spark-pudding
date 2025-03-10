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

package org.pudding.core

import java.util.Locale

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
    required = true,
    descr = "configure Text"
  )
  verify()
}

/**
 * main class
 */
object PuddingSpoon {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val jobPipelineConf = JobConfigureParser(conf.confType()).parseFromString(conf.jobText())
    BuildJob.buildSparkJob(jobPipelineConf)
  }

  /**
   * get configure file type from path file name
   *
   * @param pipelineCfgPath String
   * @return String
   */
  def getFileType(pipelineCfgPath: String): String = {
    pipelineCfgPath.lastIndexOf(".") match {
      case -1 =>
        throw new PuddingException("configure file must end with .json or .conf or .yaml!")
      case index =>
        val fileType = pipelineCfgPath.substring(index + 1).toLowerCase(Locale.ROOT)
        fileType match {
          case "yaml" | "json" => fileType
          case "conf" => "hocon"
          case _ => throw new PuddingException(
            s"unsupported configure file type : $fileType, configure file must end with .json or .conf or .yaml!")
        }
    }
  }

}
