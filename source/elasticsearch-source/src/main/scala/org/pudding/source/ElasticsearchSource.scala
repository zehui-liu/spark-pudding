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

package org.pudding.source

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.PuddingException
import org.pudding.core.definition.DataSource

/**
 * read data from elasticsearch 8
 * For details, please refer to： https://github.com/elasticsearch/elasticsearch-hadoop
 */
class ElasticsearchSource extends DataSource {

  private val ES_NODES_KEY = "esNodes"
  private val ES_PORT_KEY = "esPort"
  private val ES_INDEX_KEY = "esIndex"
  private val ES_USER_KEY = "esUser"
  private val ES_PASS_KEY = "esPass"
  private val ES_FIELDS_KEY = "fields"
  private val ES_SCROLL_SIZE_KEY = "scrollSize"
  private val ES_LIMIT_KEY = "limit"
  private val OPTIONS_KEY = "options"

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "elasticsearch-source"

  /**
   * Initial data source, use this.
   *
   * @param sparkSession SparkSession
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return DataFrame
   */
  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    // config parameter must exist
    val cfg = config.getOrElse(throw new PuddingException("config can not be null!"))

    val esNodes = this.getMapValueThrow[String](cfg, ES_NODES_KEY)
    val esIndex = this.getMapValueThrow[String](cfg, ES_INDEX_KEY)

    // optional parameters
    val esPort = cfg.get(ES_PORT_KEY).map(_.toString).getOrElse("9200")
    val esUser = cfg.get(ES_USER_KEY).map(_.toString)
    val esPass = cfg.get(ES_PASS_KEY).map(_.toString)

    val options = cfg.get(OPTIONS_KEY) match {
      case Some(opt) => opt.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    // Map internal config to elasticsearch-hadoop properties
    val esProps = scala.collection.mutable.HashMap[String, String]()
    esProps.put("es.nodes", esNodes)
    esProps.put("es.port", esPort)
    esProps.put("es.resource", esIndex)

    // Authentication for ES 8
    esUser.foreach(user => esProps.put("es.net.http.auth.user", user))
    esPass.foreach(pass => esProps.put("es.net.http.auth.pass", pass))

    // Field selection: es.read.field.include
    cfg.get(ES_FIELDS_KEY).foreach {
      case s: String => esProps.put("es.read.field.include", s)
      case l: List[_] => esProps.put("es.read.field.include", l.mkString(","))
      case seq: Seq[_] => esProps.put("es.read.field.include", seq.mkString(","))
      case other => esProps.put("es.read.field.include", other.toString)
    }

    // Scroll size: es.scroll.size
    cfg.get(ES_SCROLL_SIZE_KEY).foreach(s => esProps.put("es.scroll.size", s.toString))

    // Merge additional options
    options.foreach { case (k, v) => esProps.put(k, v) }

    val df = sparkSession
      .read
      .format("org.elasticsearch.spark.sql")
      .options(esProps.toMap)
      .load()

    // Limit data amount
    cfg.get(ES_LIMIT_KEY) match {
      case Some(l) => df.limit(l.toString.toInt)
      case None => df
    }
  }
}
