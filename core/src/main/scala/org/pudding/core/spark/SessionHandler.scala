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

package org.pudding.core.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.pudding.core.EasyHandle
import org.pudding.core.configure.JobConf

/**
 * create Spark driver client
 */
class SessionHandler extends EasyHandle {

  def sparkSessionBuilder(jobConf: JobConf): SparkSession = {

    val builder = SparkSession.builder().appName(jobConf.appName)
    if (this.checkBoolean(jobConf.enableHiveSupport)) {
      builder.enableHiveSupport()
    }

    if (jobConf.config.nonEmpty) {
      val sparkConf = new SparkConf()
      sparkConf.setAll(jobConf.config.get)
      builder.config(sparkConf)
    }

    builder.getOrCreate()
  }
}

object SessionHandler {
  def apply(): SessionHandler = new SessionHandler()
}
