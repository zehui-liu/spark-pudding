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

import scala.util.control.NonFatal

import org.testcontainers.containers.GenericContainer

/**
 * Shared rules for opt-in container tests.
 *
 * Tests run when a concrete endpoint environment variable is set or when
 * `PUDDING_TESTCONTAINERS=true`. If container startup fails, a normal local build skips the
 * tests; `PUDDING_TESTCONTAINERS_REQUIRED=true` (used by CI) turns the startup failure into a
 * hard failure so a broken Docker setup can not silently disable the suite.
 */
object ContainerTestSupport {

  def enabled: Boolean = {
    sys.env.get("PUDDING_TESTCONTAINERS").exists(_.trim.equalsIgnoreCase("true"))
  }

  def required: Boolean = {
    sys.env.get("PUDDING_TESTCONTAINERS_REQUIRED").exists(_.trim.equalsIgnoreCase("true"))
  }

  def start[T <: GenericContainer[_]](description: String)(create: => T): Option[T] = {
    var container: Option[T] = None
    try {
      val value = create
      container = Some(value)
      value.start()
      Some(value)
    } catch {
      case NonFatal(error) =>
        // A failed start may still have created a container and claimed ports; do not leave it
        // running for the rest of the test JVM.
        container.foreach(value =>
          try value.stop()
          catch { case NonFatal(_) => })
        if (required) {
          throw new IllegalStateException(
            s"$description container is required but failed to start: " +
              error.getClass.getSimpleName, error)
        }
        None
    }
  }
}
