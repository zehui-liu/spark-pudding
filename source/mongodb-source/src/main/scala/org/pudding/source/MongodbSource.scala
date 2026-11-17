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
 * Reads a MongoDB collection through the official MongoDB Spark connector.
 *
 * The connection string comes from a mongodb connection, which injects credentials from a
 * secret reference and appends URI options. The database and collection stay in the source
 * config because they identify the resource, not the connection.
 */
class MongodbSource extends DataSource {

  override def identifier: String = "mongodb-source"

  override def supportedConnectionTypes: Set[String] = Set("mongodb")

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    this.initData(sparkSession, SourceContext(config))
  }

  override def initData(sparkSession: SparkSession, context: SourceContext): DataFrame = {
    val options = MongodbSource.readerOptions(context)
    sparkSession.read.format("mongodb").options(options).load()
  }
}

private[source] object MongodbSource extends org.pudding.core.EasyHandle {

  private val DATABASE_KEY = "database"

  private val COLLECTION_KEY = "collection"

  private val OPTIONS_KEY = "options"

  private val URI_KEY = "uri"

  private val CONNECTION_URI_KEY = "connectionUri"

  private val ReadConnectionUri = "spark.mongodb.read.connection.uri"

  def readerOptions(context: SourceContext): Map[String, String] = {
    val cfg = context.config.getOrElse(throw new PuddingException("config can not be null!"))
    val database = requiredString(cfg, DATABASE_KEY)
    val collection = requiredString(cfg, COLLECTION_KEY)
    val sourceOptions = getStringOptions(cfg, OPTIONS_KEY)

    val connectionOptions = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "mongodb") {
          throw new PuddingException(
            s"mongodb-source requires a mongodb connection, but got: ${connection.`type`}")
        }
        connection.options.map {
          case ("connection.uri", value) => ReadConnectionUri -> value
          case (key, value) if key.startsWith("spark.mongodb.") => key -> value
          case (key, value) => s"spark.mongodb.read.$key" -> value
        }
      case None =>
        cfg.get(URI_KEY)
          .orElse(cfg.get(CONNECTION_URI_KEY))
          .map(_.toString)
          .filter(_.trim.nonEmpty)
          .map(value => ReadConnectionUri -> value)
          .toMap
    }

    if (!connectionOptions.contains(ReadConnectionUri)) {
      throw new PuddingException(
        s"mongodb-source requires a mongodb connection or config.$URI_KEY")
    }

    val prefixedSourceOptions = sourceOptions.map {
      case (key, value) if key.startsWith("spark.mongodb.") => key -> value
      case (key, value) => s"spark.mongodb.read.$key" -> value
    }

    prefixedSourceOptions ++ connectionOptions ++ Map(
      "spark.mongodb.read.database" -> database,
      "spark.mongodb.read.collection" -> collection)
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    cfg.get(key) match {
      case Some(value: String) if value.trim.nonEmpty => value.trim
      case Some(value: String) =>
        throw new PuddingException(s"$key can not be empty, but got: '$value'")
      case Some(_) => throw new PuddingException(s"$key must be a string")
      case None => throw new PuddingException(s"can not find param: $key")
    }
  }

}
