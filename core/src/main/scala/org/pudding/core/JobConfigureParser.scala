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

import scala.collection.immutable._

import org.pudding.core.JobConfigureParser._
import org.pudding.core.configure._
import org.pudding.core.parser._

/**
 * job confugure parser interfaces
 */
trait JobConfigureParser extends EasyHandle {

  /**
   * from file path parse configuration to class configuration
   *
   * @param pipelineCfgPath Path
   * @return JobPipelineCfg
   */
  def parseFromFile(pipelineCfgPath: String): JobPipelineConf

  /**
   * from text configuration to class configuration
   *
   * @param pipelineCfgText String
   * @return JobPipelineCfg
   */
  def parse(pipelineCfgText: String): JobPipelineConf

  def defaultParse(configMap: Map[String, Any]): JobPipelineConf = {
    val sourceCfs = extractConf[Seq[Map[String, Any]], Seq[SourceConf]](configMap, SOURCE_KEY, parseSource)
    // transformCfs is optional
    val transformCfs = configMap.get(TRANSFORM_KEY) match {
      case Some(transformCfs) =>
        Some(parseTransform(transformCfs.asInstanceOf[Seq[Map[String, Any]]]))
      case None =>
        Option.empty[Seq[TransformConf]]
    }
    val sinkCfs = extractConf[Seq[Map[String, Any]], Seq[SinkConf]](configMap, SINK_KEY, parseSink)
    val jobConf = extractConf[Map[String, Any], JobConf](configMap, JOB_KEY, parseJob)

    JobPipelineConf(sourceCfs, transformCfs, sinkCfs, jobConf)
  }

}

object JobConfigureParser extends EasyHandle {
  /**
   * common parameter key
   */
  private val TYPE_KEY = "type"
  private val DESCRIBE_KEY = "describe"
  private val ID_KEY = "id"
  private val CONFIG_KEY = "config"
  private val SOURCE_KEY = "source"
  private val TRANSFORM_KEY = "transform"
  private val SINK_KEY = "sink"
  private val DEPENDENCIES_KEY = "dependencies"
  private val DEPENDENCY_KEY = "dependency"
  private val JOB_KEY = "job"

  /**
   * jobConf key
   */
  private val APP_NAME_KEY = "appName"
  private val ENABLE_HIVE_SUPPORT_KEY = "enableHiveSupport"

  def apply(confType: String): JobConfigureParser = {
    confType.toLowerCase(Locale.ROOT) match {
      case "yaml" => new YamlJobConfigureParser()
      case "json" => new JsonJobConfigureParser()
      case "hocon" => new HoconJobConfigureParser()
      case _ => throw new PuddingException(
        s"Unsupported configure type: $confType, support yaml, json, hocon type configure file")
    }
  }

  private def parseSource(sourceMaps: Seq[Map[String, Any]]): Seq[SourceConf] = {
    sourceMaps.map(s =>
      SourceConf(
        `type` = getMapValueThrow[String](s, TYPE_KEY),
        describe = Option(s(DESCRIBE_KEY).asInstanceOf[String]),
        id = getMapValueThrow[String](s, ID_KEY),
        config = extractConfig(s)
      )
    )
  }

  private def parseTransform(transformMaps: Seq[Map[String, Any]]): Seq[TransformConf] = {
    transformMaps.map(t =>
      TransformConf(
        `type` = getMapValueThrow[String](t, TYPE_KEY),
        describe = Option(t(DESCRIBE_KEY).asInstanceOf[String]),
        id = getMapValueThrow[String](t, ID_KEY),
        config = extractConfig(t),
        dependencies = getMapValueThrow[Seq[String]](t, DEPENDENCIES_KEY)
      )
    )
  }

  private def parseSink(sinMaps: Seq[Map[String, Any]]): Seq[SinkConf] = {
    sinMaps.map(s =>
      SinkConf(
        `type` = getMapValueThrow[String](s, TYPE_KEY),
        describe = Option(s(DESCRIBE_KEY).asInstanceOf[String]),
        id = getMapValueThrow[String](s, ID_KEY),
        config = extractConfig(s),
        dependency = getMapValueThrow[String](s, DEPENDENCY_KEY)
      )
    )
  }

  private def parseJob(jobMap: Map[String, Any]): JobConf = {
    JobConf(
      appName = getMapValueThrow[String](jobMap, APP_NAME_KEY),
      describe = Option(jobMap(DESCRIBE_KEY).asInstanceOf[String]),
      enableHiveSupport = Option(jobMap(ENABLE_HIVE_SUPPORT_KEY).toString),
      config = extractConfig(jobMap)
        .map(s => s.map { case (k, v) => (k, v.asInstanceOf[String]) })
    )
  }

  private def extractConf[A, B](configMap: Map[String, Any], key: String, f: A => B): B = {
    configMap.get(key) match {
      case Some(v) =>
        if (v == null) {
          throw new PuddingException(s"$key can not be null")
        } else {
          f(v.asInstanceOf[A])
        }
      case None =>
        throw new PuddingException(s"$key can not be null")
    }
  }

  private def extractConfig(configMap: Map[String, Any]): Option[Map[String, Any]] = {
    configMap.get(CONFIG_KEY) match {
      case Some(v) =>
        val cf = try {
          v.asInstanceOf[Map[String, Any]]
        } catch {
          case e: Throwable =>
            throw new PuddingException(s"$v must be map[string, any] type, error: $e")
        }
        checkMapValueNull(cf)
        Some(cf)
      case None =>
        Option.empty[Map[String, Any]]
    }
  }
}
