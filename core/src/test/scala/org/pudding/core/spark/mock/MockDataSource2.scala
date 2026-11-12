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

case class Age(age: Int, id: Int)

/**
 * Second source of the multi-source join test.
 */
class MockDataSource2 extends DataSource {

  override def identifier: String = "mock_source2"

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    import sparkSession.implicits._
    Seq(Age(19, 1), Age(23, 2), Age(32, 3)).toDF()
  }
}
