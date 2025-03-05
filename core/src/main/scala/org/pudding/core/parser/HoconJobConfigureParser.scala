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

import scala.collection.JavaConverters.asScalaSetConverter

import com.typesafe.config.{Config, ConfigFactory}

import org.pudding.core.configure.JobPipelineConf
import org.pudding.core.JobConfigureParser

/**
 * hocon configure parser
 */
class HoconJobConfigureParser extends JobConfigureParser {

  override def parseFromString(pipelineCfgStr: String): JobPipelineConf = {
    val config = ConfigFactory.parseString(pipelineCfgStr)
    val configMap = toMapConfig(config)
    checkMapValueNull(configMap)
    defaultParse(configMap)
  }

  private def toMapConfig(config: Config): Map[String, Any] =
    config.entrySet().asScala
      .map(entry => (entry.getKey, entry.getValue.unwrapped()))
      .toMap

}
