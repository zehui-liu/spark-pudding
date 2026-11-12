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

import scala.collection.JavaConverters._
import scala.collection.immutable._

import org.pudding.core.JobConfigureParser._
import org.pudding.core.configure._
import org.pudding.core.parser._

/**
 * Parses a job configuration document into the typed pipeline model. Subclasses implement the
 * HOCON, JSON and YAML source formats.
 */
trait JobConfigureParser extends EasyHandle {

  /**
   * Parse a configuration document.
   *
   * @param pipelineCfgStr configuration text
   * @return the parsed pipeline configuration
   */
  def parseFromString(pipelineCfgStr: String): JobPipelineConf

  protected def defaultParse(configMap: Map[String, Any]): JobPipelineConf = {
    val sourceCfs = parseSource(
      JobConfigureParser.requiredSeqOfMaps(configMap, SOURCE_KEY))
    // transformCfs is optional
    val transformCfs = JobConfigureParser.optionalSeqOfMaps(configMap, TRANSFORM_KEY)
      .map(maps => parseTransform(maps))
    val sinkCfs = parseSink(
      JobConfigureParser.requiredSeqOfMaps(configMap, SINK_KEY))
    val jobConf = parseJob(JobConfigureParser.requiredMap(configMap, JOB_KEY))
    val connectionCfs = JobConfigureParser.optionalSeqOfMaps(configMap, CONNECTION_KEY)
      .map(maps => parseConnection(maps)).getOrElse(Seq.empty)
    val catalogCfs = JobConfigureParser.optionalSeqOfMaps(configMap, CATALOG_KEY)
      .map(maps => parseCatalog(maps)).getOrElse(Seq.empty)

    JobPipelineConf(sourceCfs, transformCfs, sinkCfs, jobConf, connectionCfs, catalogCfs)
  }

}

object JobConfigureParser extends EasyHandle {
  /** Keys shared by sources, transforms, sinks, connections and catalogs. */
  private val TYPE_KEY = "type"
  private val DESCRIBE_KEY = "describe"
  private val ID_KEY = "id"
  private val NAME_KEY = "name"
  private val CONFIG_KEY = "config"
  private val SOURCE_KEY = "source"
  private val TRANSFORM_KEY = "transform"
  private val SINK_KEY = "sink"
  private val CONNECTION_KEY = "connection"
  private val CATALOG_KEY = "catalog"
  private val ENDPOINT_KEY = "endpoint"
  private val AUTH_KEY = "auth"
  private val PROPERTIES_KEY = "properties"
  private val DEPENDENCIES_KEY = "dependencies"
  private val DEPENDENCY_KEY = "dependency"
  private val JOB_KEY = "job"

  /** Job-level keys. */
  private val SPARK_HOME = "sparkHome"
  private val SPARK_PUDDING_HOME = "sparkPuddingHome"
  private val APP_NAME_KEY = "appName"
  private val MASTER = "master"
  private val DEPLOY_MODE = "deployMode"
  private val ENABLE_HIVE_SUPPORT_KEY = "enableHiveSupport"
  private val JOB_MODE_KEY = "mode"
  private val STRATEGY_KEY = "strategy"
  private val TRIGGER_KEY = "trigger"
  private val LEGACY_EXECUTION_KEY = "execution"
  private val DEFAULT_APP_NAME = "spark-pudding"

  def apply(confType: String): JobConfigureParser = {
    Option(confType).map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty) match {
      case Some("yaml") => new YamlJobConfigureParser()
      case Some("json") => new JsonJobConfigureParser()
      case Some("hocon") => new HoconJobConfigureParser()
      case _ => throw new PuddingException(
        "Unsupported configure type: " +
          s"${Option(confType).getOrElse("<empty>")}, support yaml, json, hocon type configure file")
    }
  }

  private def parseSource(sourceMaps: Seq[Map[String, Any]]): Seq[SourceConf] = {
    sourceMaps.map(s =>
      SourceConf(
        `type` = getMapValueThrow[String](s, TYPE_KEY),
        describe = getMapValue[String](s, DESCRIBE_KEY),
        id = getMapValueThrow[String](s, ID_KEY).trim,
        config = extractConfig(s),
        connection = getMapValue[String](s, CONNECTION_KEY).map(_.trim).filter(_.nonEmpty),
        catalog = getMapValue[String](s, CATALOG_KEY).map(_.trim).filter(_.nonEmpty)
      )
    )
  }

  private def parseTransform(transformMaps: Seq[Map[String, Any]]): Seq[TransformConf] = {
    transformMaps.map(t =>
      TransformConf(
        `type` = getMapValueThrow[String](t, TYPE_KEY),
        describe = getMapValue[String](t, DESCRIBE_KEY),
        id = getMapValueThrow[String](t, ID_KEY).trim,
        config = extractConfig(t),
        dependencies = getMapValue[Seq[String]](t, DEPENDENCIES_KEY)
          .getOrElse(Seq.empty).map(_.trim)
      )
    )
  }

  private def parseSink(sinMaps: Seq[Map[String, Any]]): Seq[SinkConf] = {
    sinMaps.map(s =>
      SinkConf(
        `type` = getMapValueThrow[String](s, TYPE_KEY),
        describe = getMapValue[String](s, DESCRIBE_KEY),
        id = getMapValueThrow[String](s, ID_KEY).trim,
        config = extractConfig(s),
        dependency = getMapValueThrow[String](s, DEPENDENCY_KEY).trim,
        connection = getMapValue[String](s, CONNECTION_KEY).map(_.trim).filter(_.nonEmpty),
        catalog = getMapValue[String](s, CATALOG_KEY).map(_.trim).filter(_.nonEmpty)
      )
    )
  }

  private def parseConnection(connectionMaps: Seq[Map[String, Any]]): Seq[ConnectionConf] = {
    connectionMaps.map(c =>
      ConnectionConf(
        name = getMapValue[String](c, NAME_KEY)
          .orElse(getMapValue[String](c, ID_KEY))
          .map(_.trim)
          .getOrElse(throw new PuddingException(s"connection $NAME_KEY can not be null")),
        `type` = getMapValueThrow[String](c, TYPE_KEY),
        describe = getMapValue[String](c, DESCRIBE_KEY),
        endpoint = getMapValue[String](c, ENDPOINT_KEY),
        auth = extractStringMap(c, AUTH_KEY),
        properties = extractStringMap(c, PROPERTIES_KEY)
      )
    )
  }

  private def parseCatalog(catalogMaps: Seq[Map[String, Any]]): Seq[CatalogConf] = {
    catalogMaps.map(c =>
      CatalogConf(
        name = getMapValue[String](c, NAME_KEY)
          .orElse(getMapValue[String](c, ID_KEY))
          .map(_.trim)
          .getOrElse(throw new PuddingException(s"catalog $NAME_KEY can not be null")),
        `type` = getMapValueThrow[String](c, TYPE_KEY),
        describe = getMapValue[String](c, DESCRIBE_KEY),
        connection = getMapValue[String](c, CONNECTION_KEY).map(_.trim).filter(_.nonEmpty),
        properties = extractStringMap(c, PROPERTIES_KEY)
      )
    )
  }

  private def requiredSeqOfMaps(
      configMap: Map[String, Any],
      key: String): Seq[Map[String, Any]] = {
    this.optionalSeqOfMaps(configMap, key).getOrElse(
      throw new PuddingException(s"$key can not be null"))
  }

  private def optionalSeqOfMaps(
      configMap: Map[String, Any],
      key: String): Option[Seq[Map[String, Any]]] = {
    configMap.get(key) match {
      case Some(null) => throw new PuddingException(s"$key can not be null")
      case Some(values: Seq[_]) =>
        Some(values.zipWithIndex.map { case (value, index) =>
          JobConfigureParser.asConfigMap(value, s"$key[$index]")
        })
      case Some(values: java.util.List[_]) =>
        Some(values.asScala.zipWithIndex.map { case (value, index) =>
          JobConfigureParser.asConfigMap(value, s"$key[$index]")
        }.toList)
      case Some(_) => throw new PuddingException(s"$key must be a list")
      case None => None
    }
  }

  private def requiredMap(
      configMap: Map[String, Any],
      key: String): Map[String, Any] = {
    configMap.get(key) match {
      case Some(null) | None => throw new PuddingException(s"$key can not be null")
      case Some(values: Map[_, _]) =>
        this.checkedMap(values, key)
      case Some(values: java.util.Map[_, _]) =>
        this.checkedMap(values.asScala, key)
      case Some(_) => throw new PuddingException(s"$key must be a map")
    }
  }

  /**
   * Connection and catalog values are strings at runtime. YAML and HOCON can produce typed
   * scalars (a port number or a boolean flag), so normalize them here without changing what the
   * provider sees.
   */
  private def extractStringMap(configMap: Map[String, Any], key: String): Map[String, String] = {
    configMap.get(key) match {
      case Some(null) => throw new PuddingException(s"$key can not be null")
      case Some(_) => this.getStringOptions(configMap, key)
      case None => Map.empty
    }
  }

  private def parseJob(jobMap: Map[String, Any]): JobConf = {
    if (jobMap.contains(ENABLE_HIVE_SUPPORT_KEY)) {
      throw new PuddingException(
        s"job.$ENABLE_HIVE_SUPPORT_KEY is no longer supported; set " +
          "job.config.spark.sql.catalogImplementation=hive instead")
    }
    // sparkHome, master and deployMode may be supplied by the execution target; appName has a
    // default so a pipeline submitted through a platform does not need deployment details.
    JobConf(
      sparkHome = getMapValue[String](jobMap, SPARK_HOME).getOrElse(""),
      sparkPuddingHome = getMapValue[String](jobMap, SPARK_PUDDING_HOME),
      appName = getMapValue[String](jobMap, APP_NAME_KEY).getOrElse(DEFAULT_APP_NAME),
      master = getMapValue[String](jobMap, MASTER).getOrElse(""),
      deployMode = getMapValue[String](jobMap, DEPLOY_MODE).getOrElse(""),
      describe = getMapValue[String](jobMap, DESCRIBE_KEY),
      config = extractConfig(jobMap).map(this.flattenConfig),
      mode = this.parseJobMode(jobMap),
      strategy = this.parseStrategy(jobMap),
      trigger = this.parseTrigger(jobMap)
    )
  }

  private def parseJobMode(jobMap: Map[String, Any]): JobMode = {
    getMapValue[Any](jobMap, JOB_MODE_KEY).filter(_ != null) match {
      case Some(value: String) => JobMode.parse(value, s"job.$JOB_MODE_KEY")
      case Some(value) =>
        throw new PuddingException(
          s"job.$JOB_MODE_KEY must be a string, but got: ${value.getClass.getSimpleName}")
      case None =>
        throw new PuddingException(
          "job.mode is required; supported: " +
            JobMode.values.map(_.name).toSeq.sorted.mkString(", "))
    }
  }

  private def parseStrategy(jobMap: Map[String, Any]): Option[SyncStrategy] = {
    if (jobMap.contains(LEGACY_EXECUTION_KEY)) {
      throw new PuddingException(
        "job.execution is no longer supported; use job.mode: batch|sync|streaming, " +
          "job.strategy: full|incremental for sync, and job.trigger for streaming")
    }
    getMapValue[Any](jobMap, STRATEGY_KEY).filter(_ != null) match {
      case Some(value: String) =>
        Some(SyncStrategy.parse(value, s"job.$STRATEGY_KEY"))
      case Some(_) =>
        throw new PuddingException(
          s"job.$STRATEGY_KEY must be a string")
      case None => None
    }
  }

  private def parseTrigger(jobMap: Map[String, Any]): Option[String] = {
    getMapValue[Any](jobMap, TRIGGER_KEY).filter(_ != null) match {
      case Some(text: String) if text.trim.nonEmpty => Some(text.trim)
      case Some(_: String) =>
        throw new PuddingException(s"job.$TRIGGER_KEY can not be empty")
      case Some(_) =>
        throw new PuddingException(s"job.$TRIGGER_KEY must be a string")
      case None => None
    }
  }

  /**
   * Spark configuration is a flat key-value map. HOCON treats unquoted dots as nesting, so
   * `spark.executor.instances = 2` arrives as a nested map; flatten it back to the dotted key.
   */
  private def flattenConfig(values: Map[String, Any]): Map[String, String] = {
    def flatten(
        values: scala.collection.Map[_, _],
        prefix: String): Vector[(String, String)] = {
      values.toSeq.flatMap {
        case (key, value) =>
          if (key == null || value == null) {
            throw new PuddingException(s"$CONFIG_KEY can not contain a null key or value")
          }
          val name = prefix + key.toString
          value match {
            case nested: Map[_, _] =>
              flatten(nested, s"$name.")
            case nested: java.util.Map[_, _] =>
              flatten(nested.asScala, s"$name.")
            case other =>
              Seq(name -> other.toString)
          }
      }.toVector
    }

    flatten(values, "").toMap
  }

  private[core] def asConfigMap(value: Any, field: String): Map[String, Any] = value match {
    case map: Map[_, _] =>
      this.checkedMap(map, field)
    case map: java.util.Map[_, _] =>
      this.checkedMap(map.asScala, field)
    case _ => throw new PuddingException(s"$field entries must be a map")
  }

  private def checkedMap(
      map: scala.collection.Map[_, _],
      field: String): Map[String, Any] = {
    map.map {
      case (key, item) =>
        if (key == null || item == null) {
          throw new PuddingException(s"$field can not contain a null key or value")
        }
        key.toString -> item
    }.toMap
  }

  private def extractConfig(configMap: Map[String, Any]): Option[Map[String, Any]] = {
    configMap.get(CONFIG_KEY) match {
      case Some(null) =>
        throw new PuddingException(s"$CONFIG_KEY can not be null")
      case Some(map: Map[_, _]) =>
        Some(this.checkedMap(map, CONFIG_KEY))
      case Some(map: java.util.Map[_, _]) =>
        Some(this.checkedMap(map.asScala, CONFIG_KEY))
      case Some(_) =>
        throw new PuddingException(s"$CONFIG_KEY must be a map")
      case None =>
        Option.empty[Map[String, Any]]
    }
  }
}
