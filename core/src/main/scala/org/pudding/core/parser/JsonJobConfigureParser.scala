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

import scala.io.Source

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.{JsonMethods, Serialization}

import org.pudding.core.configure.JobPipelineConf
import org.pudding.core.JobConfigureParser

/**
 * json configure parser
 */
class JsonJobConfigureParser extends JobConfigureParser {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  override def parseFromString(pipelineCfgStr: String): JobPipelineConf = {
    val configMap = JsonMethods.parse(pipelineCfgStr).extract[Map[String, Any]]
    checkMapValueNull(configMap)
    defaultParse(configMap)
  }

}
