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

import scala.collection.immutable._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import org.pudding.core.PuddingException
import org.pudding.core.configure.JobPipelineConf
import org.pudding.core.definition.{DataSink, DataSource, DataTransform}

/**
 * Core classes for choreographing Spark Jobs
 */
object BuildJob extends Logging {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Initial Map[id, dataframe] is used to store the constructed id and dataframe mapping
   */
  private val idDf = mutable.Map[String, DataFrame]()

  def buildSparkJob(jobPipelineConf: JobPipelineConf): Unit = {
    // Initial SparkSession
    logInfo("Initial SparkSession")
    val jobConf = jobPipelineConf.job
    logInfo(s"jobConf is ${Serialization.write(jobConf)}")
    val sparkSession = SessionHandler().sparkSessionBuilder(jobConf)

    // Initial data source
    logInfo("Initial data source.")
    initSource(jobPipelineConf, sparkSession)

    // Initial transform, if transformCfs is none, do not process transform
    logInfo("Initial transform.")
    initTransform(jobPipelineConf)

    // Write sink
    logInfo("Write sink...")
    writeSink(jobPipelineConf)

    logInfo("close sparkSession")
    sparkSession.close()
  }

  private def initSource(jobPipelineConf: JobPipelineConf, sparkSession: SparkSession): Unit = {
    val sourceCfs = jobPipelineConf.sources
    if (sourceCfs.isEmpty) {
      throw new PuddingException(
        s"sourceCfs can not be empty, get sourceCfg is $sourceCfs. please check configure file!")
    } else {
      logInfo(s"sourceCfs is ${Serialization.write(sourceCfs)}")
    }

    val dataSources = DataSource(sourceCfs)
    assert(dataSources.nonEmpty)

    // Adding DataFrame from datasource
    idDf ++= dataSources.map {
      case (id, (dataSource, config)) => (id, dataSource.initData(sparkSession, config))
    }
  }

  private def initTransform(jobPipelineCfs: JobPipelineConf): Unit = {
    jobPipelineCfs.transforms match {
      case Some(transformCfs) =>
        logInfo(s"transformCfs is ${Serialization.write(transformCfs)}")

        val transforms = DataTransform(transformCfs)
        // Building DAG, Traverse transforms and process them sequentially
        for (transformConf <- transformCfs) {
          // Dependencies is an upstream id
          val transformIdDf = transformConf.dependencies
            .map(id =>
              id -> (idDf.get(id) match {
                case Some(df) => df
                case None => throw new PuddingException("transforms can not get dataframe from dependencies!")
              }))
            .toMap

          val (transform, config) = transforms.get(transformConf.id) match {
            case Some(tc) => tc
            case None => throw new PuddingException("can not get transform from transforms!")
          }

          idDf ++= Map(transformConf.id -> transform.handle(transformIdDf, config))
        }
      case None =>
        logInfo("transform configure non-existent")
    }
  }

  private def writeSink(jobPipelineConf: JobPipelineConf): Unit = {
    val sinkCfs = jobPipelineConf.sinks
    if (sinkCfs.isEmpty) {
      throw new PuddingException(
        s"sinkCfs can not be isEmpty, get sinkCfs is $sinkCfs. please check configure file!")
    } else {
      logInfo(s"sinkCfs is ${Serialization.write(sinkCfs)}")
    }

    val dataSinks = DataSink(sinkCfs)
    if (dataSinks.isEmpty) {
      throw new PuddingException("dataSinks can not be null!")
    } else {
      for (sinkConf <- sinkCfs) {
        val df = idDf.get(sinkConf.dependency) match {
          case Some(df) => df
          case None => throw new PuddingException("sinkConf can not get dataframe from idDf")
        }

        val (dataSink, config) = dataSinks.get(sinkConf.id) match {
          case Some(dataSink) => dataSink
          case None => throw new PuddingException("can not get dataSink from dataSinks")
        }
        dataSink.writer(df, config)
      }
    }
  }
}
