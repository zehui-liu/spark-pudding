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

package org.pudding.core

import org.pudding.core.configure.{SinkConf, SourceConf, TransformConf}
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable._

/**
 *
 */
class JobConfigureParserTest extends AnyFunSuite with Matchers with EasyHandle with PrivateMethodTester {

  private val mockSourceMaps = Seq(
    Map("type" -> "hive",
      "describe" -> "hive data source",
      "id" -> "source1",
      "config" -> Map(
        "k1" -> "v1",
        "k2" -> "v2"
      )
    )
  )

  test("test private method parseSource") {
    val privateMethod = PrivateMethod[Seq[SourceConf]]('parseSource)
    val methodRel = JobConfigureParser.invokePrivate(privateMethod(mockSourceMaps))
    assert(methodRel.isInstanceOf[Seq[SourceConf]])

    assert(methodRel.size == 1)
    val sc = methodRel.head
    assert(sc.id == "source1")
    assert(sc.`type` == "hive")
    assert(sc.describe.contains("hive data source"))

    assert(sc.config.isInstanceOf[Option[Map[String, Any]]])
    assert(sc.config.get.size == 2)
    assert(sc.config.get.values.toList == List("v1", "v2"))
    assert(sc.config.get.keys.toList == List("k1", "k2"))
  }

  private val mockTransformMaps = Seq(
    Map("type" -> "filter",
      "describe" -> "filter source1",
      "id" -> "transform1",
      "config" -> Map(
        "k1" -> "v1",
        "k2" -> "v2"
      ),
      "dependencies" -> Seq("source1")
    ),
    Map("type" -> "join",
      "describe" -> "join transform1 and source2",
      "id" -> "transform2",
      "config" -> Map(
        "k1" -> "v1",
        "k2" -> "v2"
      ),
      "dependencies" -> Seq("transform1", "source2")
    ),
  )

  test("test private method parseTransform") {
    val privateMethod = PrivateMethod[Seq[TransformConf]]('parseTransform)
    val methodRel = JobConfigureParser.invokePrivate(privateMethod(mockTransformMaps))

    assert(methodRel.size == 2)
    val tans1 = methodRel.head
    assert(tans1.id == "transform1")
    assert(tans1.`type` == "filter")
    assert(tans1.describe.contains("filter source1"))

    assert(tans1.config.isInstanceOf[Option[Map[String, Any]]])
    assert(tans1.config.get.size == 2)
    assert(tans1.config.get.values.toList == List("v1", "v2"))
    assert(tans1.config.get.keys.toList == List("k1", "k2"))

    assert(tans1.dependencies.size == 1)
    assert(tans1.dependencies == List("source1"))

    val tans2 = methodRel(1)
    assert(tans2.id == "transform2")
    assert(tans2.`type` == "join")
    assert(tans2.describe.contains("join transform1 and source2"))

    assert(tans2.config.isInstanceOf[Option[Map[String, Any]]])
    assert(tans2.config.get.size == 2)
    assert(tans2.config.get.values.toList == List("v1", "v2"))
    assert(tans2.config.get.keys.toList == List("k1", "k2"))

    assert(tans2.dependencies.size == 2)
    assert(tans2.dependencies == List("transform1", "source2"))
  }

  private val mockSinkMaps = Seq(
    Map("type" -> "mysql",
      "describe" -> "write data to mysql",
      "id" -> "sink1",
      "config" -> Map(
        "k1" -> "v1",
        "k2" -> "v2"
      ),
      "dependency" -> "transform2"
    )
  )

  test("test private method parseSink") {
    val privateMethod = PrivateMethod[Seq[SinkConf]]('parseSink)
    val methodRel = JobConfigureParser.invokePrivate(privateMethod(mockSinkMaps))

    assert(methodRel.size == 1)
    val tans1 = methodRel.head
    assert(tans1.id == "sink1")
    assert(tans1.`type` == "mysql")
    assert(tans1.describe.contains("write data to mysql"))

    assert(tans1.config.isInstanceOf[Option[Map[String, Any]]])
    assert(tans1.config.get.size == 2)
    assert(tans1.config.get.values.toList == List("v1", "v2"))
    assert(tans1.config.get.keys.toList == List("k1", "k2"))

    assert(tans1.dependency.isInstanceOf[String])
    assert(tans1.dependency == "transform2")
  }
}
