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

package org.pudding.sink

import org.apache.spark.sql.DataFrame

import org.pudding.core.definition.DataSink

/**
 *
 */
class HiveSink extends DataSink{

  /**
   * Data sink identifier，Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "hive-sink"

  /**
   * Writer dataframe
   *
   * @param dataFrame DataFrame
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   */
  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    // todo
  }
}
