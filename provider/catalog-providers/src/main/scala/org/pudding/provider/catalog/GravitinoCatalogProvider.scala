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

package org.pudding.provider.catalog

import java.util.Locale

import org.pudding.core.PuddingException
import org.pudding.core.configure.CatalogConf
import org.pudding.core.connection.ConnectionRegistry
import org.pudding.core.catalog.{CatalogProvider, ResolvedCatalog}

/**
 * Resolves a Gravitino catalog through the Gravitino REST API and installs the native Spark
 * catalog. This keeps Gravitino on the metadata plane only: Spark still connects to the
 * underlying metastore, JDBC database or Iceberg catalog directly.
 */
class GravitinoCatalogProvider extends CatalogProvider {

  override def identifier: String = "gravitino"

  override def resolve(
      conf: CatalogConf,
      connections: ConnectionRegistry): ResolvedCatalog = {

    val properties = CatalogProviderSupport.properties(conf)
    val connection = CatalogProviderSupport.connection(
      conf, connections, Set("gravitino")).getOrElse {
      throw new PuddingException(s"gravitino catalog '${conf.name}' requires a connection")
    }
    val endpoint = connection.endpoint.getOrElse {
      throw new PuddingException(s"gravitino catalog '${conf.name}' requires a connection endpoint")
    }

    val metalake = properties.get("metalake").map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"gravitino catalog '${conf.name}' requires property metalake")
    }
    val gravitinoCatalog = properties.get("catalog")
      .orElse(properties.get("gravitinoCatalog"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(conf.name)
    val token = connection.auth.get("token").orElse(connection.properties.get("token"))
    val username = connection.auth.get("username").orElse(connection.auth.get("user"))
    val password = connection.auth.get("password")
    val timeoutMs = properties.get("timeoutMs").map(_.trim).filter(_.nonEmpty).map { value =>
      val timeout = scala.util.Try(value.toInt).getOrElse {
        throw new PuddingException(
          s"gravitino catalog '${conf.name}' property timeoutMs must be an integer")
      }
      if (timeout <= 0) {
        throw new PuddingException(
          s"gravitino catalog '${conf.name}' property timeoutMs must be greater than zero")
      }
      timeout
    }.getOrElse(30000)

    val info = new GravitinoRestClient(endpoint, token, username, password, timeoutMs)
      .loadCatalog(metalake, gravitinoCatalog)

    val overrides = properties -- Set(
      "metalake", "catalog", "gravitinoCatalog", "timeoutMs", "token")
    val nativeProperties = info.properties ++ overrides
    val provider = Option(info.provider).getOrElse("").toLowerCase(Locale.ROOT)
    val catalogType = Option(info.`type`).getOrElse("").toLowerCase(Locale.ROOT)

    if (provider.contains("iceberg") || catalogType.contains("iceberg")) {
      IcebergCatalogProvider.build(conf.name, nativeProperties, None)
    } else if (provider.contains("paimon")) {
      PaimonCatalogProvider.build(conf.name, nativeProperties, None)
    } else if (provider.contains("hudi")) {
      HudiCatalogProvider.build(conf.name, nativeProperties, None)
    } else if (provider.contains("hive")) {
      HiveCatalogProvider.build(conf.name, nativeProperties, None)
    } else if (provider.contains("jdbc")) {
      JdbcCatalogProvider.build(conf.name, nativeProperties, None)
    } else {
      throw new PuddingException(
        s"Gravitino catalog '$metalake.$gravitinoCatalog' has unsupported provider " +
          s"'${info.provider}', supported: hive, iceberg, paimon, hudi, jdbc")
    }
  }
}
