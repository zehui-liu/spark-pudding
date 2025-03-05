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

package org.pudding.core.spark.mock

import org.apache.spark.sql.DataFrame
import org.pudding.core.PuddingException
import org.pudding.core.definition.DataTransform

/**
 *
 */
class MockJoinTransform extends DataTransform {

  private val JOIN_COLUMN_KEY = "joinColumn"

  /**
   * transform identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "mock-join"

  /**
   * data handle
   *
   * @param dataFrames Map[String, DataFrame],mapping map[id, DataFrame], Multiple dataframes can be transmitted,
   * supporting the merging of multiple dataframes into one to achieve complex logic such as joins
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return Return the dataframe for use by the next transform or data sink
   */
  override def handle(dataFrames: Map[String, DataFrame], config: Option[Map[String, Any]]): DataFrame = {
    // config parameter must exist
    val cfg = config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val joinColumn = this.getMapValueThrow[String](cfg, JOIN_COLUMN_KEY)

    assert(dataFrames.size == 2)
    val dfLists = dataFrames.values.toList
    val firstDf = dfLists.head
    // test for broadcast
    val session = firstDf.sparkSession
    val value = session.sparkContext.broadcast("this is a broadcast value")
    println(value.value)

    val secondDf = dfLists(1)
    firstDf.join(secondDf,joinColumn)
  }
}
