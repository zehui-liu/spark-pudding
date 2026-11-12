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

package org.pudding.core.catalog

import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.ConnectionRegistry

/**
 * Test-only catalog providers keep core tests independent from the provider modules. The real
 * built-in providers live in `catalog-providers`.
 */
class TestIcebergCatalogProvider extends CatalogProvider {

  override def identifier: String = "iceberg"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    ResolvedCatalog(
      conf.name,
      identifier,
      conf.name,
      sessionCatalog = false,
      sparkConf = Map.empty,
      properties = conf.properties)
  }
}

class TestHiveCatalogProvider extends CatalogProvider {

  override def identifier: String = "hive"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    ResolvedCatalog(
      conf.name,
      identifier,
      "spark_catalog",
      sessionCatalog = true,
      sparkConf = Map.empty,
      properties = conf.properties)
  }
}

class TestGravitinoCatalogProvider extends CatalogProvider {

  override def identifier: String = "gravitino"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {
    ResolvedCatalog(
      conf.name,
      "iceberg",
      conf.name,
      sessionCatalog = false,
      sparkConf = Map.empty,
      properties = conf.properties)
  }
}
