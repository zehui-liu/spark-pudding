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

package org.pudding.core.catalog

import org.pudding.core.PuddingException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for qualifying table names with a resolved catalog.
 */
class CatalogTableNameTest extends AnyFunSuite with Matchers {

  private def namedCatalog(name: String): ResolvedCatalog = {
    ResolvedCatalog(
      name = name,
      `type` = "lake",
      sparkCatalogName = name,
      sessionCatalog = false,
      sparkConf = Map.empty,
      properties = Map.empty)
  }

  private def sessionCatalog: ResolvedCatalog = {
    ResolvedCatalog(
      name = "hive",
      `type` = "hive",
      sparkCatalogName = "spark_catalog",
      sessionCatalog = true,
      sparkConf = Map.empty,
      properties = Map.empty)
  }

  test("qualify one and two part names with a named catalog") {
    CatalogTableName.qualify(namedCatalog("lake"), "orders") shouldBe "lake.default.orders"
    CatalogTableName.qualify(namedCatalog("lake"), "sales.orders") shouldBe "lake.sales.orders"
    CatalogTableName.qualify(namedCatalog("lake"), "orders", "analytics") shouldBe
      "lake.analytics.orders"
  }

  test("keep an already qualified name unchanged") {
    CatalogTableName.qualify(namedCatalog("lake"), "lake.sales.orders") shouldBe
      "lake.sales.orders"
  }

  test("keep session catalog names as written") {
    CatalogTableName.qualify(sessionCatalog, "orders") shouldBe "orders"
    CatalogTableName.qualify(sessionCatalog, " sales.orders ") shouldBe "sales.orders"
  }

  test("trim surrounding whitespace") {
    CatalogTableName.qualify(namedCatalog("lake"), " orders ") shouldBe "lake.default.orders"
    CatalogTableName.qualify(namedCatalog("lake"), "sales. orders") shouldBe "lake.sales.orders"
  }

  test("reject empty or malformed table names") {
    intercept[PuddingException] {
      CatalogTableName.qualify(namedCatalog("lake"), null)
    }
    intercept[PuddingException] {
      CatalogTableName.qualify(namedCatalog("lake"), "  ")
    }
    intercept[PuddingException] {
      CatalogTableName.qualify(namedCatalog("lake"), "sales..orders")
    }
    intercept[PuddingException] {
      CatalogTableName.qualify(namedCatalog("lake"), ".orders")
    }
    intercept[PuddingException] {
      CatalogTableName.qualify(sessionCatalog, " ")
    }
  }
}
