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

package org.pudding.core

import org.apache.spark.sql.SparkSession

/**
 * Creates and closes a local Spark session for connector and engine tests.
 */
class CommonTestUtils(appName: String) {

  var sparkSession: SparkSession = _

  def initLocalSparkSession(): Unit = {
    sparkSession = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .getOrCreate()

  }

  def closeSparkSession(): Unit = if (sparkSession != null) sparkSession.close()

}
