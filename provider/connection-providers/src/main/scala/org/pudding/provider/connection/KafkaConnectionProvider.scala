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

import org.pudding.core.connection.{ConnectionProvider, ResolvedConnection}
import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf

/**
 * Kafka connection: bootstrap servers plus raw Kafka client properties. Properties are prefixed
 * with `kafka.` because that is the option namespace of Spark's Kafka data source.
 */
class KafkaConnectionProvider extends ConnectionProvider {

  override def identifier: String = "kafka"

  override def resolve(conf: ConnectionConf): ResolvedConnection = {
    import ConnectionProviderSupport._

    val resolvedProperties = properties(conf)
    val resolvedEndpoint = endpointOrFail(conf, "bootstrap.servers", "kafka.bootstrap.servers")
    val resolvedAuth = auth(conf)

    var options = Map.empty[String, String]
    resolvedProperties.foreach { case (key, value) =>
      val optionKey = if (key == "bootstrap.servers") {
        "kafka.bootstrap.servers"
      } else if (key.startsWith("kafka.")) {
        key
      } else {
        s"kafka.$key"
      }
      options += optionKey -> value
    }
    options += "kafka.bootstrap.servers" -> resolvedEndpoint

    val user = text(resolvedAuth, "username", "user")
    val password = resolvedAuth.get("password")
    if (user.isDefined && password.isDefined && !options.contains("kafka.sasl.jaas.config")) {
      val mechanism = text(resolvedAuth, "mechanism")
        .orElse(property(resolvedProperties, "sasl.mechanism", "kafka.sasl.mechanism"))
        .getOrElse("PLAIN")
      options += "kafka.sasl.jaas.config" ->
        KafkaConnectionProvider.jaasConfig(mechanism, user.get, password.get)
      if (!options.contains("kafka.sasl.mechanism")) {
        options += "kafka.sasl.mechanism" -> mechanism
      }
    }

    ResolvedConnection(
      conf.name, identifier, Some(resolvedEndpoint), resolvedAuth, resolvedProperties, options)
  }
}

private[connection] object KafkaConnectionProvider {

  def jaasConfig(mechanism: String, username: String, password: String): String = {
    val loginModule = mechanism.toUpperCase(java.util.Locale.ROOT) match {
      case "PLAIN" => "org.apache.kafka.common.security.plain.PlainLoginModule"
      case value if value.startsWith("SCRAM") =>
        "org.apache.kafka.common.security.scram.ScramLoginModule"
      case other =>
        throw new PuddingException(s"unsupported Kafka SASL mechanism: $other")
    }
    s"""$loginModule required username="${escape(username)}" password="${escape(password)}";"""
  }

  private def escape(value: String): String = {
    value.replace("\\", "\\\\").replace("\"", "\\\"")
  }
}
