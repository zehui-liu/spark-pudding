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

package org.pudding.core.incremental

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Helpers for tests that launch a separate JVM with the current test class path.
 */
object ProcessTestSupport {

  def javaExecutable: String = {
    new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath
  }

  /**
   * Module openings that Spark, Hadoop and Hudi reflection need on JDK 17. The ScalaTest fork
   * receives them through `argLine`, but child JVMs started by tests do not inherit that, so the
   * same list is repeated here. Keep it in sync with the `jdk-17` profile in the root `pom.xml`.
   */
  def jdkModuleOptions: Seq[String] = {
    if (Runtime.version().feature() >= 17) {
      Seq(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
    } else {
      Seq.empty
    }
  }

  /** Command that runs a class from the test class path in a child JVM. */
  def javaCommand(arguments: String*): java.util.List[String] = {
    (Seq(javaExecutable) ++ jdkModuleOptions ++ arguments).asJava
  }

  /**
   * Maven ScalaTest may run in a classloader rather than a forked JVM, so combine the JVM class
   * path with URL classloader entries.
   */
  def testClasspath: String = {
    val entries = mutable.LinkedHashSet[String]()
    Option(System.getProperty("java.class.path"))
      .filter(_.nonEmpty)
      .foreach(_.split(File.pathSeparator).filter(_.nonEmpty).foreach(entries += _))
    var loader = Thread.currentThread().getContextClassLoader
    while (loader != null) {
      loader match {
        case urls: URLClassLoader =>
          urls.getURLs.foreach { url =>
            if (url.getProtocol == "file") {
              entries += new File(url.toURI).getAbsolutePath
            }
          }
        case _ =>
      }
      loader = loader.getParent
    }
    entries.mkString(File.pathSeparator)
  }

  def readLog(path: Path): String = {
    if (!Files.exists(path)) "" else new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
  }

  def delete(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(item => Try(Files.deleteIfExists(item)))
      } finally {
        stream.close()
      }
    }
  }
}
