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

package org.pudding.source

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.pudding.core.PuddingException

class IcebergSourceTest extends AnyFlatSpec with Matchers {

  "IcebergSource" should "have correct identifier" in {
    val source = new IcebergSource()
    source.identifier should be("iceberg-source")
    source.supportsCatalog should be(true)
  }

  it should "qualify a two-part table with a named catalog" in {
    IcebergSource.qualify("db.orders", "lake", catalogConfigured = true,
      legacyCatalogName = "spark_catalog") should be("lake.db.orders")
  }

  it should "leave a fully qualified table unchanged" in {
    IcebergSource.qualify("lake.db.orders", "lake", catalogConfigured = true,
      legacyCatalogName = "spark_catalog") should be("lake.db.orders")
  }

  it should "keep db.table for the session catalog" in {
    IcebergSource.qualify("db.orders", "spark_catalog", catalogConfigured = true,
      legacyCatalogName = "spark_catalog") should be("db.orders")
  }

  it should "reject a one-part table for a named catalog" in {
    intercept[PuddingException] {
      IcebergSource.qualify("orders", "lake", catalogConfigured = true,
        legacyCatalogName = "spark_catalog")
    }
  }

  it should "preserve the legacy one-part prefix behavior" in {
    IcebergSource.qualify("orders", "lake", catalogConfigured = false,
      legacyCatalogName = "lake") should be("lake.orders")
  }
}
