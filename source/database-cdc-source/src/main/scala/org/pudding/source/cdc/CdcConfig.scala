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

package org.pudding.source.cdc

import java.util.Locale

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.pudding.core.PuddingException

private[cdc] sealed trait CdcConfig {

  def kind: String

  def table: String

  def schema: StructType

  def opColumn: String

  def maxRecordsPerBatch: Int

  def queueCapacity: Int

  def hadoopToken: Option[String]

  def label: String = s"$kind CDC '$table'"
}

private[cdc] case class DebeziumConfig(
    kind: String,
    host: String,
    port: Int,
    database: String,
    schemaName: Option[String],
    tableName: String,
    table: String,
    user: String,
    password: String,
    serverName: String,
    serverId: Long,
    snapshotMode: String,
    connectorOptions: Map[String, String],
    hadoopToken: Option[String],
    schema: StructType,
    opColumn: String,
    maxRecordsPerBatch: Int,
    queueCapacity: Int) extends CdcConfig {

  override def toString: String = this.label
}

private[cdc] case class MongodbConfig(
    uri: String,
    database: String,
    collection: String,
    table: String,
    snapshotMode: String,
    maxAwaitTimeMs: Long,
    hadoopToken: Option[String],
    schema: StructType,
    opColumn: String,
    maxRecordsPerBatch: Int,
    queueCapacity: Int) extends CdcConfig {

  override def kind: String = "mongodb"

  override def toString: String = this.label
}

private[cdc] object CdcConfig {

  val KindKey = "pudding.cdc.kind"

  val TableKey = "pudding.cdc.table"

  val SchemaKey = "pudding.cdc.schema"

  val OpColumnKey = "pudding.cdc.opColumn"

  val MaxRecordsKey = "pudding.cdc.maxRecordsPerBatch"

  val QueueCapacityKey = "pudding.cdc.queueCapacity"

  val HostKey = "pudding.cdc.host"

  val PortKey = "pudding.cdc.port"

  val DatabaseKey = "pudding.cdc.database"

  val SchemaNameKey = "pudding.cdc.schemaName"

  val TableNameKey = "pudding.cdc.tableName"

  val UserKey = "pudding.cdc.user"

  val PasswordKey = "pudding.cdc.password"

  val ServerNameKey = "pudding.cdc.serverName"

  val ServerIdKey = "pudding.cdc.serverId"

  val SnapshotModeKey = "pudding.cdc.snapshotMode"

  // The "secret" and "url" segments keep Spark's default option-redaction patterns
  // (spark.sql.redaction.options.regex and spark.redaction.regex) covering these values in
  // explain output and the SQL UI, where Spark stores the streaming relation's options.
  val ConnectorOptionsKey = "pudding.cdc.connector.secretOptions"

  val HadoopTokenKey = "pudding.cdc.hadoopToken"

  val UriKey = "pudding.cdc.connection.url"

  val CollectionKey = "pudding.cdc.collection"

  val MaxAwaitTimeKey = "pudding.cdc.maxAwaitTimeMs"

  private val Mapper = new ObjectMapper()

  private val MysqlSnapshotModes = Set(
    "initial", "when_needed", "schema_only", "schema_only_recovery", "never", "initial_only")

  private val PostgresSnapshotModes = Set(
    "initial", "always", "never", "initial_only", "exported", "custom")

  private val MongoSnapshotModes = Set("initial", "latest")

  /**
   * Keep at least one record of headroom above the micro-batch limit. Spark commits the
   * previous source batch only while constructing a batch that has new data. If the queue can
   * fill to exactly `maxRecordsPerBatch`, the planner consumes the whole queue, sees no next
   * record, stops constructing batches, and the poller stays blocked on a full queue forever.
   */
  private[cdc] def queueCapacity(maxRecords: Int, configured: Int): Int = {
    math.max(maxRecords.toLong + 1L, configured.toLong)
      .min(Int.MaxValue.toLong)
      .toInt
  }

  def fromOptions(options: CaseInsensitiveStringMap): CdcConfig = {
    val kind = this.required(options, KindKey).toLowerCase(Locale.ROOT)
    val table = this.required(options, TableKey)
    val schema = CdcSchema.fromJson(this.required(options, SchemaKey))
    val opColumn = this.optional(options, OpColumnKey).getOrElse(CdcSchema.OpColumn)
    CdcSchema.envelope(schema, opColumn)
    val maxRecords = this.positiveInt(options, MaxRecordsKey, 1000)
    val queueCapacity = CdcConfig.queueCapacity(
      maxRecords,
      this.positiveInt(options, QueueCapacityKey, math.max(10000, maxRecords)))
    kind match {
      case "mysql" | "postgres" =>
        val snapshotMode = this.optional(options, SnapshotModeKey)
          .getOrElse("initial")
          .toLowerCase(Locale.ROOT)
        val allowed = if (kind == "mysql") MysqlSnapshotModes else PostgresSnapshotModes
        if (!allowed.contains(snapshotMode)) {
          throw new PuddingException(
            s"$SnapshotModeKey is not supported for $kind: $snapshotMode")
        }
        DebeziumConfig(
          kind = kind,
          host = this.required(options, HostKey),
          port = this.port(options),
          database = this.required(options, DatabaseKey),
          schemaName = this.optional(options, SchemaNameKey),
          tableName = this.required(options, TableNameKey),
          table = table,
          user = this.required(options, UserKey),
          password = this.optional(options, PasswordKey).getOrElse(""),
          serverName = this.required(options, ServerNameKey),
          serverId = this.positiveLong(options, ServerIdKey),
          snapshotMode = snapshotMode,
          connectorOptions = this.connectorOptions(options),
          hadoopToken = this.optional(options, HadoopTokenKey),
          schema = schema,
          opColumn = opColumn,
          maxRecordsPerBatch = maxRecords,
          queueCapacity = queueCapacity)
      case "mongodb" =>
        val snapshotMode = this.optional(options, SnapshotModeKey)
          .getOrElse("initial")
          .toLowerCase(Locale.ROOT)
        if (!MongoSnapshotModes.contains(snapshotMode)) {
          throw new PuddingException(
            s"$SnapshotModeKey is not supported for mongodb: $snapshotMode")
        }
        MongodbConfig(
          uri = this.required(options, UriKey),
          database = this.required(options, DatabaseKey),
          collection = this.required(options, CollectionKey),
          table = table,
          snapshotMode = snapshotMode,
          maxAwaitTimeMs = this.positiveLong(options, MaxAwaitTimeKey),
          hadoopToken = this.optional(options, HadoopTokenKey),
          schema = schema,
          opColumn = opColumn,
          maxRecordsPerBatch = maxRecords,
          queueCapacity = queueCapacity)
      case other =>
        throw new PuddingException(s"unsupported $KindKey: $other")
    }
  }

  def required(options: CaseInsensitiveStringMap, key: String): String = {
    this.optional(options, key).getOrElse {
      throw new PuddingException(s"can not find param: $key")
    }
  }

  def optional(options: CaseInsensitiveStringMap, key: String): Option[String] = {
    Option(options.get(key)).map(_.trim).filter(_.nonEmpty)
  }

  def int(options: CaseInsensitiveStringMap, key: String, default: Int): Int = {
    this.optional(options, key).map { value =>
      try value.toInt catch {
        case _: NumberFormatException =>
          throw new PuddingException(s"$key must be an integer")
      }
    }.getOrElse(default)
  }

  def positiveInt(options: CaseInsensitiveStringMap, key: String, default: Int): Int = {
    val value = this.int(options, key, default)
    if (value <= 0) {
      throw new PuddingException(s"$key must be positive")
    }
    value
  }

  def positiveLong(options: CaseInsensitiveStringMap, key: String): Long = {
    val value = this.optional(options, key).getOrElse {
      throw new PuddingException(s"can not find param: $key")
    }
    val parsed = try value.toLong catch {
      case _: NumberFormatException =>
        throw new PuddingException(s"$key must be an integer")
    }
    if (parsed <= 0) {
      throw new PuddingException(s"$key must be positive")
    }
    parsed
  }

  private def port(options: CaseInsensitiveStringMap): Int = {
    val value = this.int(options, PortKey, -1)
    if (value < 1 || value > 65535) {
      throw new PuddingException(s"$PortKey must be between 1 and 65535")
    }
    value
  }

  private def connectorOptions(options: CaseInsensitiveStringMap): Map[String, String] = {
    this.optional(options, ConnectorOptionsKey) match {
      case None => Map.empty
      case Some(json) =>
        try {
          val node = Mapper.readTree(json)
          if (node == null || !node.isObject) {
            throw new PuddingException(s"$ConnectorOptionsKey must be a JSON object")
          }
          node.fields().asScala.map { entry =>
            val value = entry.getValue
            val text = if (value == null || value.isNull) {
              ""
            } else if (value.isValueNode) {
              value.asText()
            } else {
              throw new PuddingException(
                s"$ConnectorOptionsKey entry '${entry.getKey}' must be a scalar")
            }
            entry.getKey -> text
          }.toMap
        } catch {
          case error: PuddingException => throw error
          case error: RuntimeException =>
            throw new PuddingException(
              s"$ConnectorOptionsKey must be a JSON object: " +
                error.getClass.getSimpleName, error)
        }
    }
  }
}
