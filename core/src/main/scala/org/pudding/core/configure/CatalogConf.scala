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

package org.pudding.core.configure

/**
 * A table catalog definition. Catalogs are installed into the Spark session before it is
 * created, so a source or sink only needs to name the catalog and the table.
 *
 * @param name       unique catalog name inside one job; sources and sinks reference this name
 * @param `type`     catalog provider identifier, such as hive, iceberg, jdbc or gravitino
 * @param describe   optional description
 * @param connection optional connection used to reach the underlying metadata service
 * @param properties provider-specific catalog properties
 */
case class CatalogConf(
  name: String,
  `type`: String,
  describe: Option[String] = None,
  connection: Option[String] = None,
  properties: Map[String, String] = Map.empty
)
