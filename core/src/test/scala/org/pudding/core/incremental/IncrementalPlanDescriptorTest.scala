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

package org.pudding.core.incremental

import org.pudding.core.definition.IncrementalSemantics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the additional resolved-resource and transform contract fingerprint.
 */
class IncrementalPlanDescriptorTest extends AnyFunSuite with Matchers {

  private def descriptor(contract: Option[Map[String, Any]]): IncrementalPlanDescriptor = {
    IncrementalPlanDescriptor.withContract(
      IncrementalPlanDescriptor.create(
        sourceType = "hudi-source",
        sourceId = "source",
        sinkType = "paimon-sink",
        sinkId = "sink",
        semantics = IncrementalSemantics.Cdc,
        keyFields = Seq("id"),
        opColumn = "__op",
        sourceConfig = Some(Map("table" -> "orders")),
        sinkConfig = Some(Map("table" -> "sales.orders")),
        stateBackend = "filesystem"),
      contract)
  }

  test("include the resolved contract and transform chain") {
    val without = descriptor(None)
    val first = descriptor(Some(Map("transforms" -> Seq(
      Map("id" -> "filter", "type" -> "filter-transform", "config" -> Map("x" -> 1))))))
    val changed = descriptor(Some(Map("transforms" -> Seq(
      Map("id" -> "filter", "type" -> "filter-transform", "config" -> Map("x" -> 2))))))

    first.fingerprint shouldNot equal (without.fingerprint)
    changed.fingerprint shouldNot equal (first.fingerprint)
    first.fingerprint shouldBe descriptor(Some(Map("transforms" -> Seq(
      Map("id" -> "filter", "type" -> "filter-transform", "config" -> Map("x" -> 1)))))).fingerprint
  }
}
