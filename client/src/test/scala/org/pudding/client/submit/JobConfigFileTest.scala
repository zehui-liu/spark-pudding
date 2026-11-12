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

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileSystems, Files, Path}
import java.util.{Collections, Optional}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Tests owner-only job configuration files and their lifecycle.
 */
class JobConfigFileTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val createdDirectories = scala.collection.mutable.ArrayBuffer.empty[Path]

  override def afterEach(): Unit = {
    JobConfigFile.delete(createdDirectories.asJava)
    createdDirectories.foreach(path => Files.deleteIfExists(path))
    createdDirectories.clear()
  }

  test("writes the configuration to an owner-only file") {
    val content = "job:\n  mode: batch\n  appName: file test\n"
    val file = JobConfigFile.create(JobConfiguration.fromText("yaml", content))
    val directory = file.getParent
    createdDirectories += directory

    file.getFileName.toString shouldBe "job.conf"
    new String(Files.readAllBytes(file), StandardCharsets.UTF_8) shouldBe content
    if (FileSystems.getDefault.supportedFileAttributeViews.contains("posix")) {
      Files.getPosixFilePermissions(file) shouldBe
        PosixFilePermissions.fromString("rw-------")
      Files.getPosixFilePermissions(directory) shouldBe
        PosixFilePermissions.fromString("rwx------")
    }
  }

  test("deletes the temporary directory after the application finishes") {
    val file = JobConfigFile.create(JobConfiguration.fromText("yaml", "job:\n  mode: batch\n  appName: a\n"))
    val directory = file.getParent
    createdDirectories += directory

    JobConfigFile.scheduleCleanup(new FinishedApplication, Collections.singletonList(directory))

    awaitDeleted(directory)
    Files.exists(file) shouldBe false
  }

  test("deleting a pending directory twice is safe") {
    val file = JobConfigFile.create(JobConfiguration.fromText("yaml", "job:\n  mode: batch\n  appName: a\n"))
    val directory = file.getParent
    createdDirectories += directory

    JobConfigFile.delete(Collections.singletonList(directory))
    JobConfigFile.delete(Collections.singletonList(directory))

    Files.exists(directory) shouldBe false
  }

  private def awaitDeleted(path: Path): Unit = {
    val deadline = System.currentTimeMillis() + 5000
    while (Files.exists(path) && System.currentTimeMillis() < deadline) {
      Thread.sleep(20)
    }
    Files.exists(path) shouldBe false
  }

  private final class FinishedApplication extends RunningApplication {

    override def getTargetName: String = "test"

    override def getApplicationId: Optional[String] = Optional.empty()

    override def getState: ApplicationState = ApplicationState.FINISHED

    override def getError: Optional[Throwable] = Optional.empty()

    override def await(): SubmissionResult =
      new SubmissionResult("test", ApplicationState.FINISHED, null, null)

    override def cancel(): Unit = ()
  }
}
