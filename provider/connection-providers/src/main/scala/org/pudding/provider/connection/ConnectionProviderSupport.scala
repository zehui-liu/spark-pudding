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

import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf
import org.pudding.core.connection.SecretResolver

/**
 * Shared helpers for built-in connection providers.
 */
private[connection] object ConnectionProviderSupport {

  def auth(conf: ConnectionConf): Map[String, String] = {
    SecretResolver.resolveMap(conf.auth)
  }

  def properties(conf: ConnectionConf): Map[String, String] = {
    SecretResolver.resolveMap(conf.properties)
  }

  def endpoint(conf: ConnectionConf, propertyKeys: String*): Option[String] = {
    val resolved = conf.endpoint.map(SecretResolver.resolve).map(_.trim).filter(_.nonEmpty)
    resolved.orElse {
      val props = properties(conf)
      propertyKeys.iterator
        .map(key => props.get(key).map(_.trim).filter(_.nonEmpty))
        .collectFirst { case Some(value) => value }
    }
  }

  def endpointOrFail(conf: ConnectionConf, propertyKeys: String*): String = {
    endpoint(conf, propertyKeys: _*).getOrElse {
      throw new PuddingException(
        s"connection '${conf.name}' (type '${conf.`type`}') requires endpoint")
    }
  }

  def text(auth: Map[String, String], keys: String*): Option[String] = {
    keys.iterator
      .map(key => auth.get(key).map(_.trim).filter(_.nonEmpty))
      .collectFirst { case Some(value) => value }
  }

  def property(props: Map[String, String], keys: String*): Option[String] = {
    keys.iterator
      .map(key => props.get(key).map(_.trim).filter(_.nonEmpty))
      .collectFirst { case Some(value) => value }
  }
}
