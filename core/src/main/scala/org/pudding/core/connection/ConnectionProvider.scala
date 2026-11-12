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

package org.pudding.core.connection

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf

/**
 * Turns a connection definition into native Spark options. Implementations are discovered with
 * ServiceLoader, so a connector module can add a new connection type without changing core.
 */
trait ConnectionProvider {

  /**
   * connection type identifier, for example jdbc, kafka or mongodb.
   */
  def identifier: String

  /**
   * Resolve credentials and produce the options consumed by the matching Spark data source.
   */
  def resolve(conf: ConnectionConf): ResolvedConnection
}

object ConnectionProvider {

  def providers: Map[String, ConnectionProvider] = {
    val loader = ServiceLoader.load(classOf[ConnectionProvider])
    loader.iterator().asScala.map(provider => provider.identifier -> provider).toMap
  }

  def identifiers: Set[String] = this.providers.keySet

  def forType(connectionType: String): ConnectionProvider = {
    this.providers.getOrElse(connectionType,
      throw new PuddingException(
        s"unknown connection type: $connectionType, available: ${this.availableTypes}"))
  }

  private def availableTypes: String = {
    val types = this.identifiers
    if (types.isEmpty) "none" else types.toSeq.sorted.mkString(", ")
  }
}
