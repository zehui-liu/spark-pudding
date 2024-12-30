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
import org.apache.spark.sql.DataFrame

import org.pudding.core.configure.SinkConf
import org.pudding.core.{EasyHandle, PuddingException}

/**
 * Support writing dataframes to different data sources, with parameters passed in the form of maps.
 * Interface implementers need to perform interface validation.
 */
trait DataSink extends EasyHandle with Logging {

  /**
   * Data sink identifier，Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  def identifier: String

  /**
   * Writer dataframe
   *
   * @param dataFrame DataFrame
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   */
  def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit
}

object DataSink {

  /**
   * factory method
   * @param sinkDefs Seq[SinkDef]
   * @return map[id, (DataSink, config map)]
   */
  def apply(sinkCfs: Seq[SinkConf]): Map[String, (DataSink, Option[Map[String, Any]])] = {
    assert(sinkCfs.nonEmpty)
    val serviceLoader = ServiceLoader.load(classOf[DataSink])
    val identifierAndServices = serviceLoader.iterator().asScala.toList.map(s => s.identifier -> s).toMap

    sinkCfs.map(s => (s.id, (identifierAndServices.get(s.`type`) match {
      case Some(dataSink) => dataSink
      case _ => throw new PuddingException(s"${s.`type`} can not get DataSink implement!")
    }) -> s.config)).toMap
  }
}
