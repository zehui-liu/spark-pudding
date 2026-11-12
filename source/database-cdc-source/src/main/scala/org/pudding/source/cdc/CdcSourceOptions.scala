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

package org.pudding.source.cdc

import java.net.URI
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types.StructType

import org.pudding.core.PuddingException
import org.pudding.core.connection.ResolvedConnection
import org.pudding.core.definition.SourceContext

/**
 * Convert a Pudding source configuration and its optional connection into the flat, typed
 * options consumed by the `pudding-cdc` Spark data source. Credentials are copied into the Spark
 * option map because Spark's TableProvider boundary is option-based; they are never logged.
 */
private[cdc] object CdcSourceOptions {

  private val Mapper = new ObjectMapper()

  private val UserKeys = Seq("user", "username")

  private val PasswordKeys = Seq("password")

  def resolve(
      context: SourceContext,
      kind: String): (Map[String, String], StructType, String) = {
    val cfg = context.config.getOrElse(
      throw new PuddingException("config can not be null!"))
    val schema = CdcSchema.parse(cfg)
    val opColumn = this.optionalString(cfg, "opColumn").getOrElse(CdcSchema.OpColumn)
    CdcSchema.envelope(schema, opColumn)
    val maxRecords = this.positiveInt(cfg, "maxRecordsPerBatch", 1000)
    val queueCapacity = math.max(
      maxRecords, this.positiveInt(cfg, "queueCapacity", math.max(10000, maxRecords)))

    val common = Map(
      CdcConfig.KindKey -> kind,
      CdcConfig.SchemaKey -> schema.json,
      CdcConfig.OpColumnKey -> opColumn,
      CdcConfig.MaxRecordsKey -> maxRecords.toString,
      CdcConfig.QueueCapacityKey -> queueCapacity.toString)

    kind match {
      case "mysql" | "postgres" =>
        val endpoint = this.jdbcEndpoint(context, kind, cfg)
        val (database, schemaName, tableName) = this.tableParts(kind, cfg, endpoint)
        val credentials = this.jdbcCredentials(context, cfg)
        val table = (kind, schemaName) match {
          case ("postgres", Some(value)) => s"$database.$value.$tableName"
          case _ => s"$database.$tableName"
        }
        val serverName = this.optionalString(cfg, "serverName")
          .getOrElse(this.stableName(kind, table))
        if (!serverName.matches("[A-Za-z0-9_]+")) {
          throw new PuddingException(
            "serverName may contain only letters, digits and underscores")
        }
        val serverId = this.optionalLong(cfg, "serverId")
          .getOrElse(this.stableServerId(kind, table))
        if (serverId <= 0L || serverId > 4294967295L) {
          throw new PuddingException("serverId must be between 1 and 4294967295")
        }
        val connectorOptions = this.connectorOptions(cfg)
        val result = common ++ Map(
          CdcConfig.TableKey -> table,
          CdcConfig.HostKey -> endpoint.host,
          CdcConfig.PortKey -> endpoint.port.toString,
          CdcConfig.DatabaseKey -> database,
          CdcConfig.TableNameKey -> tableName,
          CdcConfig.UserKey -> credentials.user,
          CdcConfig.PasswordKey -> credentials.password,
          CdcConfig.ServerNameKey -> serverName,
          CdcConfig.ServerIdKey -> serverId.toString,
          CdcConfig.SnapshotModeKey -> this.optionalString(cfg, "snapshotMode")
            .getOrElse("initial"),
          CdcConfig.ConnectorOptionsKey -> this.renderOptions(connectorOptions))
        val withSchema = schemaName match {
          case Some(value) => result + (CdcConfig.SchemaNameKey -> value)
          case None => result
        }
        (withSchema, schema, opColumn)
      case "mongodb" =>
        val uri = this.mongoUri(context, cfg)
        val database = this.requiredString(cfg, "database")
        val collection = this.requiredString(cfg, "collection")
        if (!schema.fieldNames.contains("_id")) {
          throw new PuddingException(
            "schema must contain '_id' for mongodb-cdc-source; MongoDB delete events only " +
              "carry the document key")
        }
        val result = common ++ Map(
          CdcConfig.TableKey -> s"$database.$collection",
          CdcConfig.DatabaseKey -> database,
          CdcConfig.CollectionKey -> collection,
          CdcConfig.UriKey -> uri,
          CdcConfig.SnapshotModeKey -> this.optionalString(cfg, "snapshotMode")
            .getOrElse("initial"),
          CdcConfig.MaxAwaitTimeKey -> this.positiveLong(cfg, "maxAwaitTimeMs", 1000L).toString)
        (result, schema, opColumn)
      case other =>
        throw new PuddingException(s"unsupported CDC source kind: $other")
    }
  }

  private case class JdbcEndpoint(host: String, port: Int, database: Option[String])

  private case class Credentials(user: String, password: String)

  private def jdbcEndpoint(
      context: SourceContext,
      kind: String,
      cfg: Map[String, Any]): JdbcEndpoint = {
    val connectionUrl = context.connection.flatMap { connection =>
      this.validateConnection(connection, kind)
      connection.options.get("url").orElse(connection.options.get("jdbc-url"))
    }
    val inlineUrl = this.optionalString(cfg, "url")
      .orElse(this.optionalString(cfg, "jdbcUrl"))
    val configuredUrl = connectionUrl.orElse(inlineUrl)
    val configuredDatabase = this.optionalString(cfg, "database")
    configuredUrl match {
      case Some(url) =>
        val parsed = this.parseJdbcUrl(url, kind)
        JdbcEndpoint(parsed.host, parsed.port, configuredDatabase.orElse(parsed.database))
      case None =>
        val host = this.requiredString(cfg, "host")
        val defaultPort = if (kind == "mysql") 3306 else 5432
        val port = this.optionalInt(cfg, "port").getOrElse(defaultPort)
        if (port < 1 || port > 65535) {
          throw new PuddingException("port must be between 1 and 65535")
        }
        JdbcEndpoint(host, port, configuredDatabase)
    }
  }

  private def validateConnection(connection: ResolvedConnection, kind: String): Unit = {
    val allowed = if (kind == "mysql") Set("mysql", "jdbc") else Set("jdbc")
    if (!allowed.contains(connection.`type`)) {
      throw new PuddingException(
        s"$kind-cdc-source requires a ${allowed.toSeq.sorted.mkString(" or ")} " +
          s"connection, but got: ${connection.`type`}")
    }
  }

  private def parseJdbcUrl(url: String, kind: String): JdbcEndpoint = {
    if (url == null || !url.startsWith("jdbc:")) {
      throw new PuddingException("connection url must start with jdbc:")
    }
    val scheme = if (kind == "mysql") "mysql" else "postgresql"
    val uri = try URI.create(url.substring("jdbc:".length)) catch {
      case _: IllegalArgumentException =>
        throw new PuddingException("connection url is invalid")
    }
    if (!scheme.equalsIgnoreCase(uri.getScheme) || uri.getHost == null) {
      throw new PuddingException(
        s"connection url must use the jdbc:$scheme scheme and include a host")
    }
    val defaultPort = if (kind == "mysql") 3306 else 5432
    val port = if (uri.getPort < 0) defaultPort else uri.getPort
    if (port < 1 || port > 65535) {
      throw new PuddingException("connection url port must be between 1 and 65535")
    }
    val database = Option(uri.getPath).map(_.stripPrefix("/")).filter(_.nonEmpty)
      .map(value => URLDecoder.decode(value, StandardCharsets.UTF_8.name()))
    JdbcEndpoint(uri.getHost, port, database)
  }

  private def tableParts(
      kind: String,
      cfg: Map[String, Any],
      endpoint: JdbcEndpoint): (String, Option[String], String) = {
    val tableValue = this.requiredString(cfg, "table")
    val split = tableValue.split("\\.", -1).filter(_.nonEmpty).toSeq
    if (kind == "mysql") {
      val database = this.optionalString(cfg, "database")
        .orElse(if (split.size == 2) Some(split.head) else None)
        .orElse(endpoint.database)
        .getOrElse {
          throw new PuddingException(
            "can not find param: database (or a database in connection url)")
        }
      val table = if (split.size >= 2) split.last else tableValue
      (database, None, table)
    } else {
      val database = this.optionalString(cfg, "database").orElse(endpoint.database).getOrElse {
        throw new PuddingException(
          "can not find param: database (or a database in connection url)")
      }
      val configuredSchema = this.optionalString(cfg, "databaseSchema")
      val schema = configuredSchema
        .orElse(if (split.size == 2) Some(split.head) else None)
        .getOrElse("public")
      val table = if (split.size >= 2) split.last else tableValue
      (database, Some(schema), table)
    }
  }

  private def jdbcCredentials(
      context: SourceContext,
      cfg: Map[String, Any]): Credentials = {
    val connectionOptions = context.connection.map(_.options).getOrElse(Map.empty)
    val connectionAuth = context.connection.map(_.auth).getOrElse(Map.empty)
    val user = this.first(connectionOptions, UserKeys)
      .orElse(this.first(connectionAuth, UserKeys))
      .orElse(this.firstString(cfg, UserKeys))
      .getOrElse {
        throw new PuddingException("can not find param: user")
      }
    val password = this.first(connectionOptions, PasswordKeys)
      .orElse(this.first(connectionAuth, PasswordKeys))
      .orElse(this.firstString(cfg, PasswordKeys))
      .getOrElse("")
    Credentials(user, password)
  }

  private def mongoUri(context: SourceContext, cfg: Map[String, Any]): String = {
    val connectionUri = context.connection match {
      case Some(connection) =>
        if (connection.`type` != "mongodb") {
          throw new PuddingException(
            s"mongodb-cdc-source requires a mongodb connection, but got: ${connection.`type`}")
        }
        connection.options.get("connection.uri").orElse(connection.endpoint)
      case None => None
    }
    val uri = connectionUri.orElse(this.optionalString(cfg, "uri"))
      .orElse(this.optionalString(cfg, "connectionUri"))
      .getOrElse {
        throw new PuddingException(
          "mongodb-cdc-source requires a mongodb connection or config.uri")
      }
    val lower = uri.toLowerCase(Locale.ROOT)
    if (!lower.startsWith("mongodb://") && !lower.startsWith("mongodb+srv://")) {
      throw new PuddingException(
        "mongodb uri must start with mongodb:// or mongodb+srv://")
    }
    uri
  }

  private def connectorOptions(cfg: Map[String, Any]): Map[String, String] = {
    cfg.get("options").filter(_ != null) match {
      case None => Map.empty
      case Some(values: Map[_, _]) =>
        values.map { case (key, value) => this.optionEntry(key, value) }.toMap
      case Some(values: java.util.Map[_, _]) =>
        values.asScala.map { case (key, value) => this.optionEntry(key, value) }.toMap
      case Some(_) =>
        throw new PuddingException("options must be a map")
    }
  }

  private def optionEntry(key: Any, value: Any): (String, String) = {
    if (key == null || key.toString.trim.isEmpty) {
      throw new PuddingException("options can not contain an empty key")
    }
    if (value == null) {
      throw new PuddingException(s"options entry '${key.toString}' can not be null")
    }
    key.toString.trim -> value.toString
  }

  private def renderOptions(values: Map[String, String]): String = {
    val node = Mapper.createObjectNode()
    values.toSeq.sortBy(_._1).foreach { case (key, value) => node.put(key, value) }
    Mapper.writeValueAsString(node)
  }

  private def stableName(kind: String, table: String): String = {
    val hash = Integer.toHexString((kind + ":" + table).hashCode)
    s"pudding_${kind}_$hash"
  }

  private def stableServerId(kind: String, table: String): Long = {
    val hash = Math.abs((kind + ":" + table).hashCode.toLong)
    100000L + hash % 1000000L
  }

  private def first(values: Map[String, String], keys: Seq[String]): Option[String] = {
    keys.iterator.flatMap(key => values.get(key)).map(_.trim).find(_.nonEmpty)
  }

  private def firstString(cfg: Map[String, Any], keys: Seq[String]): Option[String] = {
    keys.iterator.flatMap(key => this.optionalString(cfg, key)).toSeq.headOption
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    this.optionalString(cfg, key).getOrElse {
      throw new PuddingException(s"can not find param: $key")
    }
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) => Option(value.trim).filter(_.nonEmpty)
      case Some(_) => throw new PuddingException(s"$key must be a string")
      case None => None
    }
  }

  private def optionalInt(cfg: Map[String, Any], key: String): Option[Int] = {
    cfg.get(key).filter(_ != null).map {
      case value: Int => value
      case value: Long if value >= Int.MinValue && value <= Int.MaxValue => value.toInt
      case value: String =>
        try value.trim.toInt catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$key must be an integer")
        }
      case _ => throw new PuddingException(s"$key must be an integer")
    }
  }

  private def optionalLong(cfg: Map[String, Any], key: String): Option[Long] = {
    cfg.get(key).filter(_ != null).map {
      case value: Long => value
      case value: Int => value.toLong
      case value: String =>
        try value.trim.toLong catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$key must be an integer")
        }
      case _ => throw new PuddingException(s"$key must be an integer")
    }
  }

  private def positiveInt(cfg: Map[String, Any], key: String, default: Int): Int = {
    val value = this.optionalInt(cfg, key).getOrElse(default)
    if (value <= 0) {
      throw new PuddingException(s"$key must be positive")
    }
    value
  }

  private def positiveLong(cfg: Map[String, Any], key: String, default: Long): Long = {
    val value = this.optionalLong(cfg, key).getOrElse(default)
    if (value <= 0L) {
      throw new PuddingException(s"$key must be positive")
    }
    value
  }
}
