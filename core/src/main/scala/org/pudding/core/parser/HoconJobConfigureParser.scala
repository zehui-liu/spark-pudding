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

package org.pudding.core.parser

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory}

import org.pudding.core.configure.JobPipelineConf
import org.pudding.core.{JobConfigureParser, PuddingException}

/**
 * hocon configure parser
 */
class HoconJobConfigureParser extends JobConfigureParser {

  override def parseFromString(pipelineCfgStr: String): JobPipelineConf = {
    val configMap = try {
      val config = ConfigFactory.parseString(pipelineCfgStr)
      toMapConfig(config)
    } catch {
      case error: Exception =>
        throw new PuddingException(
          s"parser hocon file error: ${error.getClass.getSimpleName}", error)
    }
    checkMapValueNull(configMap)
    defaultParse(configMap)
  }

  private def toMapConfig(config: Config): Map[String, Any] =
    convertValue(config.root().unwrapped()).asInstanceOf[Map[String, Any]]

  /**
   * typesafe config unwrapped values use java collections, convert them to scala collections
   */
  private def convertValue(value: Any): Any = value match {
    case map: java.util.Map[_, _] =>
      map.asScala.map { case (k, v) => k.toString -> convertValue(v) }.toMap
    case list: java.util.List[_] =>
      list.asScala.map(convertValue).toList
    case other => other
  }

}
