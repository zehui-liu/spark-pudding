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

package org.pudding.provider.connection

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf
import org.pudding.core.connection.{ConnectionProvider, ResolvedConnection}

/**
 * MongoDB connection. The endpoint is a MongoDB connection string; user and password from the
 * auth section are injected into it so credentials can live in a secret reference. Other
 * properties are appended as connection string options unless they already use the
 * `spark.mongodb.` namespace, in which case they are passed to the Spark connector unchanged.
 */
class MongoConnectionProvider extends ConnectionProvider {

  override def identifier: String = "mongodb"

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    import ConnectionProviderSupport._

    val resolvedProperties = properties(conf)
    val resolvedEndpoint = endpointOrFail(
      conf, "uri", "connection.uri", "spark.mongodb.read.connection.uri")
    val resolvedAuth = auth(conf)

    val user = text(resolvedAuth, "username", "user")
    val password = resolvedAuth.get("password").orElse(Some("")).filter(_ => user.isDefined)
    var uri = resolvedEndpoint
    user.foreach { value =>
      uri = MongoConnectionProvider.withCredentials(uri, value, password.getOrElse(""))
    }
    val uriOptions = resolvedProperties.filterNot { case (key, _) =>
      key.startsWith("spark.mongodb.") || key == "uri" || key == "connection.uri"
    }
    uri = MongoConnectionProvider.withQueryOptions(uri, uriOptions)

    val sparkOptions = resolvedProperties.filter { case (key, _) =>
      key.startsWith("spark.mongodb.")
    }
    ResolvedConnection(
      conf.name,
      identifier,
      Some(resolvedEndpoint),
      resolvedAuth,
      resolvedProperties,
      sparkOptions + ("connection.uri" -> uri))
  }
}

private[connection] object MongoConnectionProvider {

  def withCredentials(uri: String, user: String, password: String): String = {
    val schemeEnd = uri.indexOf("://")
    if (schemeEnd < 0) {
      throw new PuddingException("mongodb endpoint must start with mongodb:// or mongodb+srv://")
    }
    val scheme = uri.substring(0, schemeEnd)
    if (scheme != "mongodb" && scheme != "mongodb+srv") {
      throw new PuddingException("mongodb endpoint must start with mongodb:// or mongodb+srv://")
    }

    val authorityStart = schemeEnd + 3
    val rest = uri.substring(authorityStart)
    val authorityEnd = rest.indexWhere(character => character == '/' || character == '?')
    val authority = if (authorityEnd < 0) rest else rest.substring(0, authorityEnd)
    if (authority.contains("@")) {
      uri
    } else {
      val credentials = s"${encode(user)}:${encode(password)}@"
      uri.substring(0, authorityStart) + credentials + rest
    }
  }

  def withQueryOptions(uri: String, options: Map[String, String]): String = {
    if (options.isEmpty) {
      uri
    } else {
      val (base, existingQuery) = uri.indexOf('?') match {
        case -1 => (uri, Map.empty[String, String])
        case index => (uri.substring(0, index), parseQuery(uri.substring(index + 1)))
      }
      val merged = existingQuery ++ options
      val query = merged.toSeq.sortBy(_._1).map { case (key, value) =>
        s"${encode(key)}=${encode(value)}"
      }.mkString("&")
      s"$base?$query"
    }
  }

  private def parseQuery(query: String): Map[String, String] = {
    query.split("&").filter(_.nonEmpty).map { entry =>
      entry.split("=", 2) match {
        case Array(key, value) => decode(key) -> decode(value)
        case Array(key) => decode(key) -> ""
      }
    }.toMap
  }

  private def encode(value: String): String = {
    URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20")
  }

  private def decode(value: String): String = {
    java.net.URLDecoder.decode(value, StandardCharsets.UTF_8.name())
  }
}
