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

package org.pudding.core.definition

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.configure.SourceConf
import org.pudding.core.{EasyHandle, PuddingException}

/**
 * Support creating dataframes from different data sources and configuring them to be passed in the form of maps.
 * Interface implementers need to perform parameter validation.
 */
trait DataSource extends EasyHandle with Logging with Serializable {

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  def identifier: String

  /**
   * Initial data source, use this. have a surprised
   *
   * @param sparkSession SparkSession
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return DataFrame
   */
  def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame
}

object DataSource {

  /**
   * factory method
   * @param sourceDefs Seq[SourceDef]
   * @return map[id, (DataSource, config map)]
   */
  def apply(sourceCfs: Seq[SourceConf]): Map[String, (DataSource, Option[Map[String, Any]])] = {
    assert(sourceCfs.nonEmpty)
    val serviceLoader = ServiceLoader.load(classOf[DataSource])
    val identifierAndServices = serviceLoader.iterator().asScala.toList.map(s => s.identifier -> s).toMap

    sourceCfs.map(s => (s.id, (identifierAndServices.get(s.`type`) match {
      case Some(dataSource) => dataSource
      case _ => throw new PuddingException(s"${s.`type`} can not get DataSource implement!")
    }) -> s.config)).toMap
  }
}
