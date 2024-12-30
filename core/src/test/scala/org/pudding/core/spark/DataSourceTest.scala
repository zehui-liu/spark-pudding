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

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.pudding.core.definition.DataSource
import org.pudding.core.spark.mock.MockConf.{mockJobCfg, mockSourceCfg}

import scala.collection.immutable._

/**
 * test for DataSource
 */
class DataSourceTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  var sS: SparkSession = _

  override def beforeAll(): Unit = {
    sS = SessionHandler().sparkSessionBuilder(mockJobCfg)
  }

  override def afterAll(): Unit = sS.close()

  test("test datasource factory method") {
    val dataSources = DataSource(Seq(mockSourceCfg))

    assert(dataSources.isInstanceOf[Map[String, (DataSource, Option[Map[String, Any]])]])
    assert(dataSources.size == 1)
    assert(dataSources.head._1 == mockSourceCfg.id)

    val (datasource, config) = dataSources.get(mockSourceCfg.id).head
    val dataFrame = datasource.initData(sS, config)
    val rows = dataFrame.collect()
    assert(dataFrame.collect().length == 3)
    assert(rows.head.get(0).equals("Alice"))
    assert(rows.head.get(1).asInstanceOf[Int] == 30)

    dataFrame.show(true)
  }

}
