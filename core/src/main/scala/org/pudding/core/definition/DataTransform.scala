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

import org.pudding.core.configure.TransformConf
import org.pudding.core.{EasyHandle, PuddingException}

/**
 * Perform data processing operations such as aggregation, join, filtering, etc.,
 * supporting the input of multiple dataframes and the output of one dataframe, many to one.
 * with parameters passed in the form of a map.
 * The interface implementer needs to perform parameter validation themselves
 */
trait DataTransform extends EasyHandle with Logging {

  /**
   * transform identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  def identifier: String

  /**
   * data handle
   *
   * @param dataFrames Map[String, DataFrame],mapping map[id, DataFrame], Multiple dataframes can be transmitted,
   * supporting the merging of multiple dataframes into one to achieve complex logic such as joins
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return Return the dataframe for use by the next transform or data sink
   */
  def handle(dataFrames: Map[String, DataFrame], config: Option[Map[String, Any]]): DataFrame
}

object DataTransform {

  /**
   * factory method
   * @param transformDefs Seq[TransformDef]
   * @return map[id, (Transform, config map)]
   */
  def apply(transformCfs: Seq[TransformConf]): Map[String, (DataTransform, Option[Map[String, Any]])] = {
    assert(transformCfs.nonEmpty)
    val serviceLoader = ServiceLoader.load(classOf[DataTransform])
    val identifierAndServices = serviceLoader.iterator().asScala.toList.map(s => s.identifier -> s).toMap

    transformCfs.map(t => (t.id, (identifierAndServices.get(t.`type`) match {
      case Some(transform) => transform
      case _ => throw new PuddingException(s"${t.`type`} can not get Transform implement!")
    }) -> t.config)).toMap
  }
}
