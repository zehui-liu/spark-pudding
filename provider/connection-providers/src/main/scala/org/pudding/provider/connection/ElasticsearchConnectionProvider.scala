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

import java.net.URI

import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf
import org.pudding.core.connection.{ConnectionProvider, ResolvedConnection}

/**
 * Elasticsearch connection: nodes, port, TLS flag and HTTP basic authentication. Index or index
 * pattern stays in the source config because it is a resource locator, not a connection property.
 */
class ElasticsearchConnectionProvider extends ConnectionProvider {

  override def identifier: String = "elasticsearch"

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    import ConnectionProviderSupport._

    val resolvedProperties = properties(conf)
    val resolvedEndpoint = endpointOrFail(conf, "es.nodes", "nodes")
    val resolvedAuth = auth(conf)
    val parsed = ElasticsearchConnectionProvider.parseEndpoints(resolvedEndpoint)

    var options = Map[String, String]("es.nodes" -> parsed.nodes.mkString(","))
    parsed.port.foreach(port => options += "es.port" -> port.toString)
    if (parsed.ssl) {
      options += "es.net.ssl" -> "true"
    }

    val user = text(resolvedAuth, "username", "user").orElse(parsed.user)
    val password = resolvedAuth.get("password").orElse(parsed.password)
    user.foreach(value => options += "es.net.http.auth.user" -> value)
    password.foreach(value => options += "es.net.http.auth.pass" -> value)

    resolvedProperties.foreach { case (key, value) =>
      val optionKey = if (key.startsWith("es.")) key else s"es.$key"
      options += optionKey -> value
    }

    ResolvedConnection(
      conf.name, identifier, Some(resolvedEndpoint), resolvedAuth, resolvedProperties, options)
  }
}

private[connection] case class ElasticsearchEndpoint(
  nodes: Seq[String],
  port: Option[Int],
  ssl: Boolean,
  user: Option[String],
  password: Option[String])

private[connection] object ElasticsearchConnectionProvider {

  def parseEndpoints(endpoint: String): ElasticsearchEndpoint = {
    val entries = endpoint.split(",").map(_.trim).filter(_.nonEmpty)
    if (entries.isEmpty) {
      throw new PuddingException("elasticsearch endpoint can not be empty")
    }

    var ssl = false
    var user: Option[String] = None
    var password: Option[String] = None
    val ports = scala.collection.mutable.LinkedHashSet[Int]()
    val nodes = entries.map { entry =>
      val withScheme = if (entry.contains("://")) entry else s"http://$entry"
      val uri = try {
        new URI(withScheme)
      } catch {
        case _: Exception => throw new PuddingException("invalid elasticsearch endpoint")
      }
      Option(uri.getHost).filter(_.nonEmpty).getOrElse {
        throw new PuddingException("invalid elasticsearch endpoint: missing host")
      }
      if ("https".equalsIgnoreCase(uri.getScheme)) {
        ssl = true
      }
      Option(uri.getPort).filter(_ > 0).foreach(ports += _)
      Option(uri.getUserInfo).foreach { info =>
        val parts = info.split(":", 2)
        if (user.isEmpty && parts.nonEmpty) {
          user = Some(parts(0))
        }
        if (password.isEmpty && parts.length > 1) {
          password = Some(parts(1))
        }
      }
      uri.getHost
    }

    if (ports.size > 1) {
      throw new PuddingException(
        "elasticsearch endpoint uses different ports; configure one port per connection")
    }

    ElasticsearchEndpoint(nodes, ports.headOption, ssl, user, password)
  }
}
