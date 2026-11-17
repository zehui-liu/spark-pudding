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

package org.pudding.connector.jdbc

import java.util.Locale

import org.pudding.core.PuddingException

/**
 * Shared configuration parsing for the JDBC source and sink connectors.
 *
 * Only key names appear in validation errors. Connection strings, user names,
 * passwords and option values are never echoed, so failures remain safe to log.
 */
object JdbcConnectorConfig {

  val UrlKey = "url"

  val TableKey = "table"

  val UserKey = "user"

  val PasswordKey = "password"

  val DriverKey = "driver"

  val OptionsKey = "options"

  private val PostgreSQLPrefix = "jdbc:postgresql:"

  private val PostgreSQLDriver = "org.postgresql.Driver"

  private val DefaultSaveMode = "overwrite"

  private val SupportedSaveModes =
    Set("append", "overwrite", "ignore", "error", "errorifexists")

  /**
   * Parses the Spark save mode used by the JDBC sinks. The value is case-insensitive.
   */
  def saveMode(cfg: Map[String, Any]): String = {
    cfg.get("mode").filter(_ != null) match {
      case Some(value: String) =>
        val normalized = value.trim.toLowerCase(Locale.ROOT)
        if (!SupportedSaveModes.contains(normalized)) {
          throw new PuddingException(s"unsupported save mode: $normalized")
        }
        normalized
      case Some(_) => throw new PuddingException("mode must be a string")
      case None => DefaultSaveMode
    }
  }

  /**
   * Builds the option map passed to Spark's JDBC data source. Spark expects the
   * table under `dbtable`; the top-level keys always win over `options`, so a
   * tuning map cannot silently replace the connection or credentials.
   *
   * @param connectionOptions native options from a resolved connection. They
   *                          take precedence over the legacy inline keys, but
   *                          source/sink options may still tune the reader or
   *                          writer.
   */
  def connectionOptions(
      cfg: Map[String, Any],
      connectionOptions: Map[String, String] = Map.empty): Map[String, String] = {
    val url = connectionOptions.get(UrlKey)
      .orElse(optionalString(cfg, UrlKey))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new PuddingException(
        s"can not find param: $UrlKey, $UrlKey must be exist!"))
    if (!url.toLowerCase(Locale.ROOT).startsWith("jdbc:")) {
      throw new PuddingException(s"$UrlKey must start with jdbc:")
    }

    val table = requiredString(cfg, TableKey).trim
    if (table.isEmpty) {
      throw new PuddingException(s"$TableKey can not be empty!")
    }

    val user = connectionOptions.get(UserKey)
      .orElse(optionalString(cfg, UserKey))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new PuddingException(s"$UserKey can not be empty!"))

    // An empty password is valid for local trust-based databases. When a
    // connection is used, a missing password is treated as empty; legacy
    // configurations must still set the key explicitly.
    val password = connectionOptions.get(PasswordKey)
      .orElse(optionalString(cfg, PasswordKey))
      .getOrElse {
        if (connectionOptions.nonEmpty) {
          ""
        } else {
          throw new PuddingException(
            s"can not find param: $PasswordKey, $PasswordKey must be exist!")
        }
      }

    val driver = connectionOptions.get(DriverKey)
      .orElse(optionalString(cfg, DriverKey))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse {
        if (url.toLowerCase(Locale.ROOT).startsWith(PostgreSQLPrefix)) {
          PostgreSQLDriver
        } else {
          throw new PuddingException(s"$DriverKey is required for this JDBC url")
        }
      }

    val connectionTuning = connectionOptions -- Set(UrlKey, UserKey, PasswordKey, DriverKey)
    connectionTuning ++ optionalStringMap(cfg, OptionsKey) ++ Map(
      "url" -> url,
      "dbtable" -> table,
      UserKey -> user,
      PasswordKey -> password,
      DriverKey -> driver
    )
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    optionalString(cfg, key).getOrElse(
      throw new PuddingException(s"can not find param: $key, $key must be exist!"))
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null).map {
      case value: String => value
      case _ => throw new PuddingException(s"$key must be a string")
    }
  }

  private def optionalStringMap(cfg: Map[String, Any], key: String): Map[String, String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(options: Map[_, _]) =>
        options.map {
          case (optionKey: String, value: String) => optionKey -> value
          case (optionKey: String, value: Boolean) => optionKey -> value.toString
          case (optionKey: String, value: Number) => optionKey -> value.toString
          case _ =>
            throw new PuddingException(
              s"$key keys must be strings and values must be strings, booleans or numbers")
        }.toMap
      case Some(_) => throw new PuddingException(s"$key must be a map")
      case None => Map.empty[String, String]
    }
  }
}
