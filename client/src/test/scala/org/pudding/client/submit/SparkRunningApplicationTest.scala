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

import java.util.{ArrayList, Optional}

import org.apache.spark.launcher.SparkAppHandle
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Tests the application handle wrapper without starting a real Spark process.
 */
class SparkRunningApplicationTest extends AnyFunSuite with Matchers {

  test("maps final state, application id and error") {
    val handle = new FakeHandle
    handle.applicationId = "application_1"
    val application = new SparkRunningApplication("yarn")
    application.attach(handle)

    handle.transition(SparkAppHandle.State.RUNNING)
    application.getState shouldBe ApplicationState.RUNNING
    application.getApplicationId shouldBe Optional.of("application_1")

    handle.currentError = Optional.of(new IllegalStateException("driver failed"))
    handle.transition(SparkAppHandle.State.FAILED)
    val result = application.await()

    result.getState shouldBe ApplicationState.FAILED
    result.getTargetName shouldBe "yarn"
    result.getApplicationId shouldBe Optional.of("application_1")
    result.isSuccessful shouldBe false
    result.getError.isPresent shouldBe true
  }

  test("await returns success and can be called twice") {
    val handle = new FakeHandle
    val application = new SparkRunningApplication("local")
    application.attach(handle)

    handle.transition(SparkAppHandle.State.FINISHED)

    val first = application.await()
    val second = application.await()

    first.getState shouldBe ApplicationState.FINISHED
    first.isSuccessful shouldBe true
    second.getState shouldBe first.getState
    second.getApplicationId.isPresent shouldBe false
  }

  test("cancel requests an application kill") {
    val handle = new FakeHandle
    val application = new SparkRunningApplication("kubernetes")
    application.attach(handle)

    application.cancel()

    handle.killed shouldBe true
  }

  private class FakeHandle extends SparkAppHandle {

    private val registeredListeners = new ArrayList[SparkAppHandle.Listener]()

    var currentState: SparkAppHandle.State = SparkAppHandle.State.UNKNOWN

    var applicationId: String = _

    var currentError: Optional[Throwable] = Optional.empty()

    var killed = false

    override def addListener(listener: SparkAppHandle.Listener): Unit = {
      registeredListeners.add(listener)
    }

    override def getState: SparkAppHandle.State = currentState

    override def getAppId: String = applicationId

    override def stop(): Unit = {
      killed = true
    }

    override def kill(): Unit = {
      killed = true
    }

    override def disconnect(): Unit = {
    }

    override def getError: Optional[Throwable] = currentError

    def transition(nextState: SparkAppHandle.State): Unit = {
      currentState = nextState
      registeredListeners.asScala.foreach(_.stateChanged(this))
    }
  }
}
