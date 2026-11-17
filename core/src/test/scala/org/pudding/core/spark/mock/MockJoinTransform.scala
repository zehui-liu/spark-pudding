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

package org.pudding.core.spark.mock

import org.apache.spark.sql.DataFrame
import org.pudding.core.PuddingException
import org.pudding.core.definition.DataTransform

class MockJoinTransform extends DataTransform {

  private val JOIN_COLUMN_KEY = "joinColumn"

  override def identifier: String = "mock-join"

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
