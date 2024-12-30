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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.pudding.core.PuddingException
import org.pudding.core.definition.{DataSource, DataTransform}
import org.pudding.core.spark.mock.MockConf.{mockJobCfg, mockSourceCfg, mockTransformCfg}

import scala.collection.mutable
import scala.collection.immutable._

/**
 * test for Transform
 */
class DataTransformTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  var sS: SparkSession = _
  var idDf: mutable.Map[String, DataFrame] = _

  override def beforeAll(): Unit = {
    sS = SessionHandler.apply().sparkSessionBuilder(mockJobCfg)
    idDf = mutable.Map[String, DataFrame]()

    val dataSources = DataSource(Seq(mockSourceCfg))
    assert(dataSources.nonEmpty)
    idDf ++= dataSources.map { case (id, (dataSource, configs)) =>
      (id, dataSource.initData(sS, configs))
    }
  }

  override def afterAll(): Unit = sS.close()

  test("test transform factory method") {
    val transforms = DataTransform(Seq(mockTransformCfg))
    assert(transforms.size == 1)
    assert(transforms.head._1 == "trans_1")

    val (transform, config) = transforms.head._2

    val transformIdDf = mockTransformCfg.dependencies
      .map(id => id -> (idDf.get(id) match {
        case Some(df) => df
        case None => throw new PuddingException("can not get dataframe from dependencies!")
      }))
      .toMap

    val df = transform.handle(transformIdDf, config)
    val rows = df.collect()

    assert(rows.length == 1)
    assert(rows.head.get(0) == "Charlie")
    assert(rows.head.get(1) == 35)

    df.show(false)
  }

}
