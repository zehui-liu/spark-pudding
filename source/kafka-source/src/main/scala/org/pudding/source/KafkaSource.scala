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
 * read data from kafka
 * For details, please refer to： https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 */
class KafkaSource extends DataSource {

  private val KAFKA_BOOTSTRAP_SERVERS_KEY = "kafkaBootstrapServers"

  private val SUBSCRIBE_KEY = "subscribe"

  private val OPTIONS_KEY = "options"

  private val FORCE_CONVERT_SCHEMA_KEY = "forceConvertSchema"

  private val SCHEMA_KEY = "schema"

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "kafka-source"

  /**
   * Initial data source, use this. have a surprised
   *
   * @param sparkSession SparkSession
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return DataFrame
   */
  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    // config parameter must exist
    val cfg = config.getOrElse(throw new PuddingException("config can not be null!"))

    val kafkaBootstrapServers = this.getMapValueThrow[String](cfg, KAFKA_BOOTSTRAP_SERVERS_KEY)
    val topics = this.getMapValueThrow[String](cfg, SUBSCRIBE_KEY)

    // options parameter is optional
    val options = cfg.get(OPTIONS_KEY) match {
      case Some(options) => options.asInstanceOf[Map[String, String]]
      case None => Map.empty[String, String]
    }

    /**
     * Warn！Not currently supported assign and subscribePattern parameter
     * Warn！Not currently supported kafka use confluent schema registry
     */
    import  sparkSession.implicits._
    sparkSession
      .read
      .format("kafka")
      .option(KAFKA_BOOTSTRAP_SERVERS_KEY, kafkaBootstrapServers)
      .option(SUBSCRIBE_KEY, topics)
      .options(options)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .toDF()
  }
}
