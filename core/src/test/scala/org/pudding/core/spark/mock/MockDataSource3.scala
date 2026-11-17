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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pudding.core.definition.DataSource

case class Salary(salary: Int, id: Int)

/**
 * Third source of the multi-source join test.
 */
class MockDataSource3 extends DataSource {

  override def identifier: String = "mock_source3"

  override def initData(sparkSession: SparkSession, config: Option[Map[String, Any]]): DataFrame = {
    import sparkSession.implicits._
    Seq(Salary(2000, 1), Salary(3000, 2), Salary(3200, 3)).toDF()
  }
}
