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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.definition.DataSource

case class Person1(name: String, id: Int)

/**
 *
 */
class MockDataSource1 extends DataSource {

  /**
   * dataSource identifier, Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "mock_source1"

  /**
   * Initial data source, use this. have a surprised
   *
   * @param sparkSession SparkSession
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   * @return DataFrame
   */
  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    import sparkSession.implicits._
    Seq(Person1("Alice", 1), Person1("Bob", 2), Person1("Charlie", 3)).toDF()
  }

}
