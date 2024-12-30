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

import java.io.FileReader

import scala.collection.immutable._

import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{Formats, NoTypeHints}

import org.pudding.core.configure.JobPipelineConf
import org.pudding.core.{JobConfigureParser, PuddingException}

/**
 * yaml configure parser
 */
class YamlJobConfigureParser extends JobConfigureParser {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  override def parseFromFile(pipelineCfgName: String): JobPipelineConf = {
    val jsonStr = io.circe.yaml.parser.parse(new FileReader(pipelineCfgName)) match {
      case Right(json) => json.toString()
      case Left(errorInfo) => throw new PuddingException(s"parser yaml file error: $errorInfo")
    }

    val configMap = JsonMethods.parse(jsonStr).extract[Map[String, Any]]
    checkMapValueNull(configMap)
    defaultParse(configMap)
  }

  override def parse(pipelineCfgText: String): JobPipelineConf = {
    val jsonStr = io.circe.yaml.parser.parse(pipelineCfgText) match {
      case Right(json) => json.toString()
      case Left(errorInfo) => throw new PuddingException(s"parser yaml file error: $errorInfo")
    }
    // jsonStr must be json
    val configMap = JsonMethods.parse(jsonStr).extract[Map[String, Any]]
    checkMapValueNull(configMap)
    defaultParse(configMap)
  }
}
