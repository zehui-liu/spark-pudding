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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.ConnectionRegistry

/**
 * Resolves a catalog definition to native Spark settings. Implementations are discovered with
 * ServiceLoader. Resolution runs on the driver before the SparkSession is created.
 */
trait CatalogProvider {

  def identifier: String

  def resolve(conf: CatalogConf, connections: ConnectionRegistry): ResolvedCatalog
}

object CatalogProvider {

  def providers: Map[String, CatalogProvider] = {
    val loader = ServiceLoader.load(classOf[CatalogProvider])
    loader.iterator().asScala.map(provider => provider.identifier -> provider).toMap
  }

  def identifiers: Set[String] = this.providers.keySet

  def forType(catalogType: String): CatalogProvider = {
    this.providers.getOrElse(catalogType,
      throw new PuddingException(
        s"unknown catalog type: $catalogType, available: ${this.availableTypes}"))
  }

  private def availableTypes: String = {
    val types = this.identifiers
    if (types.isEmpty) "none" else types.toSeq.sorted.mkString(", ")
  }
}
