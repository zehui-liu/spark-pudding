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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests target name normalization and master-based inference.
 */
class TargetNamesTest extends AnyFunSuite with Matchers {

  test("infers targets from a Spark master") {
    TargetNames.inferFromMaster("local") shouldBe TargetNames.LOCAL
    TargetNames.inferFromMaster("local[4]") shouldBe TargetNames.LOCAL
    TargetNames.inferFromMaster("local-cluster[2,1024,1024]") shouldBe TargetNames.LOCAL
    TargetNames.inferFromMaster("yarn") shouldBe TargetNames.YARN
    TargetNames.inferFromMaster("k8s://https://cluster.example:443") shouldBe
      TargetNames.KUBERNETES
  }

  test("rejects masters without a known target") {
    val error = intercept[SubmissionException] {
      TargetNames.inferFromMaster("spark://cluster.example:7077")
    }
    error.getMessage should include ("pass --target explicitly")
  }

  test("normalizes aliases") {
    TargetNames.normalize(" LOCAL ") shouldBe TargetNames.LOCAL
    TargetNames.normalize("yarn-cluster") shouldBe TargetNames.YARN
    TargetNames.normalize("yarn-client") shouldBe TargetNames.YARN
    TargetNames.normalize("k8s") shouldBe TargetNames.KUBERNETES
    TargetNames.normalize("Kubernetes") shouldBe TargetNames.KUBERNETES
  }

  test("rejects unknown target names") {
    intercept[SubmissionException] {
      TargetNames.normalize("mesos")
    }
    intercept[SubmissionException] {
      TargetNames.normalize(" ")
    }
  }

  test("validates named target aliases") {
    TargetNames.requireName("yarn-prod") shouldBe "yarn-prod"
    TargetNames.requireName(" k8s-eu-1 ") shouldBe "k8s-eu-1"
    TargetNames.requireName("local") shouldBe "local"
    TargetNames.requireName("a" * TargetNames.MAX_NAME_LENGTH) shouldBe
      ("a" * TargetNames.MAX_NAME_LENGTH)

    intercept[SubmissionException] {
      TargetNames.requireName("Yarn-Prod")
    }
    intercept[SubmissionException] {
      TargetNames.requireName("-yarn")
    }
    intercept[SubmissionException] {
      TargetNames.requireName("yarn_prod")
    }
    intercept[SubmissionException] {
      TargetNames.requireName("a" * (TargetNames.MAX_NAME_LENGTH + 1))
    }
    intercept[SubmissionException] {
      TargetNames.requireName(" ")
    }
  }
}
