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

package org.pudding.client.submit

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class YarnSiteConfigTest extends AnyFunSuite with Matchers {

  test("read the ResourceManager web application address from yarn-site.xml") {
    val dir = Files.createTempDirectory("yarn-site")
    try {
      Files.write(
        dir.resolve("yarn-site.xml"),
        """<?xml version="1.0"?>
          |<configuration>
          |  <property>
          |    <name>yarn.resourcemanager.webapp.address</name>
          |    <value>rm.example:8088</value>
          |  </property>
          |</configuration>
          |""".stripMargin.getBytes("UTF-8"))

      YarnSiteConfig.webappAddress(Map("YARN_CONF_DIR" -> dir.toString).asJava) shouldBe
        java.util.Optional.of("http://rm.example:8088")
    } finally {
      delete(dir)
    }
  }

  test("fall back to the HTTPS web application address") {
    val dir = Files.createTempDirectory("yarn-site-https")
    try {
      Files.write(
        dir.resolve("yarn-site.xml"),
        """<configuration>
          |  <property>
          |    <name>yarn.resourcemanager.webapp.https.address</name>
          |    <value>rm.example:8090</value>
          |  </property>
          |</configuration>
          |""".stripMargin.getBytes("UTF-8"))

      YarnSiteConfig.webappAddress(Map("HADOOP_CONF_DIR" -> dir.toString).asJava) shouldBe
        java.util.Optional.of("https://rm.example:8090")
    } finally {
      delete(dir)
    }
  }

  test("read an HA ResourceManager web application address") {
    val dir = Files.createTempDirectory("yarn-site-ha")
    try {
      Files.write(
        dir.resolve("yarn-site.xml"),
        """<configuration>
          |  <property>
          |    <name>yarn.resourcemanager.webapp.address.rm1</name>
          |    <value>rm1.example:8088</value>
          |  </property>
          |</configuration>
          |""".stripMargin.getBytes("UTF-8"))

      YarnSiteConfig.webappAddress(Map("YARN_CONF_DIR" -> dir.toString).asJava) shouldBe
        java.util.Optional.of("http://rm1.example:8088")
    } finally {
      delete(dir)
    }
  }

  test("prefer an explicit PUDDING_YARN_RM_URL") {
    YarnSiteConfig.webappAddress(
      Map("PUDDING_YARN_RM_URL" -> "http://rm.local:8088/").asJava) shouldBe
      java.util.Optional.of("http://rm.local:8088")
  }

  test("return empty when no YARN web application address is configured") {
    YarnSiteConfig.webappAddress(Map.empty[String, String].asJava) shouldBe
      java.util.Optional.empty()
  }

  private def delete(dir: Path): Unit = {
    val paths = Files.walk(dir)
    try {
      paths.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    } finally {
      paths.close()
    }
  }
}
