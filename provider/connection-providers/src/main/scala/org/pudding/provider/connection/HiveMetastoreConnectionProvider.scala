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

package org.pudding.provider.connection

import org.pudding.core.connection.{ConnectionProvider, ResolvedConnection}
import org.pudding.core.configure.ConnectionConf

/**
 * Connection to a remote Hive metastore. It is consumed by the hive catalog provider rather than
 * by a source directly, because Spark has one Hive session catalog per session.
 */
class HiveMetastoreConnectionProvider extends ConnectionProvider {

  override def identifier: String = "hive-metastore"

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    import ConnectionProviderSupport._

    val resolvedProperties = properties(conf)
    val resolvedEndpoint = endpointOrFail(conf, "hive.metastore.uris", "metastore.uris")
    val resolvedAuth = auth(conf)

    var options = resolvedProperties.map { case (key, value) =>
      val optionKey = if (key.startsWith("hive.") || key.startsWith("spark.")) key else s"hive.$key"
      optionKey -> value
    }
    options += "hive.metastore.uris" -> resolvedEndpoint

    ResolvedConnection(
      conf.name, identifier, Some(resolvedEndpoint), resolvedAuth, resolvedProperties, options)
  }
}
