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

/**
 * A catalog after provider resolution.
 *
 * @param name             catalog name used by source and sink references
 * @param `type`           native catalog type: hive, hudi, paimon, iceberg or jdbc
 * @param sparkCatalogName name to use in a fully qualified Spark table identifier
 * @param sessionCatalog   true when this is Spark's single Hive session catalog
 * @param sparkConf        settings to apply to the SparkSession before it is created
 * @param properties       resolved catalog properties, which may contain credentials
 */
case class ResolvedCatalog(
  name: String,
  `type`: String,
  sparkCatalogName: String,
  sessionCatalog: Boolean,
  sparkConf: Map[String, String],
  properties: Map[String, String])
