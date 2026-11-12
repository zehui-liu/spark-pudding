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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import org.pudding.core.EasyHandle
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.configure.JobConf
import org.pudding.core.metric.StreamingMetrics

/**
 * create Spark driver client
 */
class SessionHandler extends EasyHandle with Logging {

  private val MASTER_KEY = "spark.master"

  private val DEPLOY_MODE_KEY = "spark.submit.deployMode"

  def sparkSessionBuilder(jobConf: JobConf): SparkSession = {
    this.sparkSessionBuilder(jobConf, Seq.empty)
  }

  def sparkSessionBuilder(
      jobConf: JobConf,
      catalogs: Seq[ResolvedCatalog]): SparkSession = {

    val builder = SparkSession.builder().appName(jobConf.appName)
    // Resolve every Spark source before the session is created: Spark reads catalog settings
    // only during initialization, and conflicting values must not be silently overwritten.
    val sparkConf = SparkConfigResolver.resolve(jobConf, catalogs, new SparkConf())
    builder.config(sparkConf)

    // The runtime master is set by spark-submit and must not be overridden. When there is no
    // runtime master, for example when SparkSession is created directly, job.master is used.
    this.applyMaster(builder, jobConf, sparkConf)
    this.warnDeployModeMismatch(jobConf, sparkConf)

    val session = builder.getOrCreate()
    StreamingMetrics.register(session)
    session
  }

  private def applyMaster(
      builder: SparkSession.Builder,
      jobConf: JobConf,
      sparkConf: SparkConf): Unit = {
    val jobMaster = Option(jobConf.master).map(_.trim).filter(_.nonEmpty)
    val runtimeMaster = Option(sparkConf.get(MASTER_KEY, null)).map(_.trim).filter(_.nonEmpty)

    (jobMaster, runtimeMaster) match {
      case (Some(configured), Some(actual)) if configured != actual =>
        logWarning(
          s"job master '$configured' is ignored because runtime master '$actual' is already set")
      case (Some(configured), None) =>
        logInfo(s"using job master '$configured'")
        builder.master(configured)
      case _ =>
    }
  }

  private def warnDeployModeMismatch(jobConf: JobConf, sparkConf: SparkConf): Unit = {
    val jobDeployMode = Option(jobConf.deployMode).map(_.trim).filter(_.nonEmpty)
    val runtimeDeployMode = Option(sparkConf.get(DEPLOY_MODE_KEY, null))
      .map(_.trim)
      .filter(_.nonEmpty)

    (jobDeployMode, runtimeDeployMode) match {
      case (Some(configured), Some(actual)) if configured != actual =>
        logWarning(
          s"job deployMode '$configured' can not change runtime deployMode '$actual'; " +
            "deployMode is applied by the launcher")
      case _ =>
    }
  }
}

object SessionHandler {
  def apply(): SessionHandler = new SessionHandler()
}
