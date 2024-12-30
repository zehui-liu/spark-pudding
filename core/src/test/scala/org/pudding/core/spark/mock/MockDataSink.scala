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
import org.pudding.core.definition.DataSink

/**
 *
 */
class MockDataSink extends DataSink {

  /**
   * default 20 rows
   */
  private val NUM_ROWS_KEY = "numRows"

  /**
   * default 20
   */
  private val TRUNCATE_KEY = "truncate"

  /**
   * default false
   */
  private val VERTICAL_KEY = "vertical"

  /**
   * default false
   */
  private val PRINT_SCHEMA_KEY = "printSchema"

  /**
   * default Int.MaxValue
   */
  private val LEVEL_KEY = "level"

  /**
   * Data sink identifier，Require implementation of class unique, And consistent with the configuration
   *
   * @return String
   */
  override def identifier: String = "show-sink"

  /**
   * Writer dataframe
   *
   * @param dataFrame DataFrame
   * @param config Option[Map[String, Any] Parameter configuration requires self parameter verification
   */
  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {

    config match {
      case Some(cfg) =>
        if (cfg.getOrElse(PRINT_SCHEMA_KEY, false).asInstanceOf[Boolean]) {
          dataFrame.printSchema(cfg.getOrElse(LEVEL_KEY, Int.MaxValue).asInstanceOf[Int])
        }

        dataFrame.show(
          numRows = cfg.getOrElse(NUM_ROWS_KEY, 20).asInstanceOf[BigInt].toInt,
          truncate = cfg.getOrElse(TRUNCATE_KEY, 20).asInstanceOf[BigInt].toInt,
          vertical = cfg.getOrElse(VERTICAL_KEY, false).asInstanceOf[Boolean]
        )
      case None =>
        dataFrame.show()
    }

  }
}
