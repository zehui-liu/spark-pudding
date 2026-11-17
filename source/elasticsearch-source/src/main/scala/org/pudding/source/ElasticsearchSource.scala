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

package org.pudding.source

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.PuddingException
import org.pudding.core.definition.{DataSource, SourceContext}

/**
 * read data from elasticsearch 8
 * For details, please refer to： https://github.com/elasticsearch/elasticsearch-hadoop
 */
class ElasticsearchSource extends DataSource {

  private val ES_NODES_KEY = "esNodes"
  private val ES_PORT_KEY = "esPort"
  private val ES_USER_KEY = "esUser"
  private val ES_PASS_KEY = "esPass"
  private val ES_LIMIT_KEY = "limit"

  override def identifier: String = "elasticsearch-source"

  override def supportedConnectionTypes: Set[String] = Set("elasticsearch")

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    if (context.connection.isDefined) {
      this.warnLegacyConnectionKeys(cfg)
    }
    val esProps = ElasticsearchSource.readerOptions(context)

    val df = sparkSession
      .read
      .format("org.elasticsearch.spark.sql")
      .options(esProps)
      .load()

    // Limit data amount
    cfg.get(ES_LIMIT_KEY).filter(_ != null) match {
      case Some(value) =>
        val limit = try {
          value.toString.trim.toInt
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$ES_LIMIT_KEY must be an integer, but got: $value")
        }
        if (limit <= 0) {
          throw new PuddingException(
            s"$ES_LIMIT_KEY must be greater than zero, but got: $limit")
        }
        df.limit(limit)
      case None => df
    }
  }

  private def warnLegacyConnectionKeys(cfg: Map[String, Any]): Unit = {
    val legacyKeys = Seq(ES_NODES_KEY, ES_PORT_KEY, ES_USER_KEY, ES_PASS_KEY)
      .filter(cfg.contains)
    if (legacyKeys.nonEmpty) {
      logWarning(
        s"${legacyKeys.mkString(", ")} are ignored because an elasticsearch connection is configured")
    }
  }

}

private[source] object ElasticsearchSource extends org.pudding.core.EasyHandle {

  def readerOptions(context: SourceContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val esIndex = requiredString(cfg, "esIndex")
    val options = getStringOptions(cfg, "options")

    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "elasticsearch") {
          throw new PuddingException(
            s"elasticsearch-source requires an elasticsearch connection, " +
              s"but got: ${connection.`type`}")
        }
        connection.options
      case None =>
        legacyConnectionOptions(cfg)
    }

    // Connection settings win over source options, so a source can not accidentally point the
    // job at another cluster or replace credentials.
    var properties = options ++ connectionOptions
    properties += "es.resource" -> esIndex

    cfg.get("fields").filter(_ != null).foreach {
      case value: String =>
        val fields = value.split(",").map(_.trim).filter(_.nonEmpty).mkString(",")
        if (fields.nonEmpty) properties += "es.read.field.include" -> fields
      case seq: Seq[_] =>
        val fields = seq.map(_.toString.trim).filter(_.nonEmpty).mkString(",")
        if (fields.nonEmpty) properties += "es.read.field.include" -> fields
      case other => properties += "es.read.field.include" -> other.toString
    }

    cfg.get("scrollSize").filter(_ != null).foreach { value =>
      val scrollSize = try {
        value.toString.trim.toInt
      } catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"scrollSize must be an integer, but got: $value")
      }
      if (scrollSize <= 0) {
        throw new PuddingException(
          s"scrollSize must be greater than zero, but got: $scrollSize")
      }
      properties += "es.scroll.size" -> scrollSize.toString
    }
    properties
  }

  private def legacyConnectionOptions(cfg: Map[String, Any]): Map[String, String] = {
    val esNodes = requiredString(cfg, "esNodes")
    val esPort = cfg.get("esPort").filter(_ != null)
      .map(_.toString.trim).filter(_.nonEmpty).getOrElse("9200")
    var options = Map("es.nodes" -> esNodes, "es.port" -> esPort)
    cfg.get("esUser").filter(_ != null).map(_.toString.trim).filter(_.nonEmpty)
      .foreach(user => options += "es.net.http.auth.user" -> user)
    cfg.get("esPass").filter(_ != null).map(_.toString).filter(_.nonEmpty)
      .foreach(password => options += "es.net.http.auth.pass" -> password)
    options
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => throw new PuddingException(s"can not find param: $key")
    }
  }
}
