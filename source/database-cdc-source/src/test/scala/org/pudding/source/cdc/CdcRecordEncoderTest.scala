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

import java.util.{Base64, Collections, Date}

import com.fasterxml.jackson.databind.ObjectMapper
import org.bson.Document
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CdcRecordEncoderTest extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()

  private val rowSchema = SchemaBuilder.struct()
    .name("inventory.orders")
    .field("id", Schema.INT32_SCHEMA)
    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
    .field("amount", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("created", org.apache.kafka.connect.data.Date.builder().optional().build())
    .field("updated", org.apache.kafka.connect.data.Timestamp.builder().optional().build())
    .optional()
    .build()

  private val sourceSchema = SchemaBuilder.struct()
    .field("ts_ms", Schema.INT64_SCHEMA)
    .build()

  private val envelopeSchema = SchemaBuilder.struct()
    .field("before", rowSchema)
    .field("after", rowSchema)
    .field("source", sourceSchema)
    .field("op", Schema.STRING_SCHEMA)
    .build()

  private val config = DebeziumConfig(
    kind = "mysql",
    host = "localhost",
    port = 3306,
    database = "inventory",
    schemaName = None,
    tableName = "orders",
    table = "inventory.orders",
    user = "pudding",
    password = "secret",
    serverName = "pudding_mysql_test",
    serverId = 1L,
    snapshotMode = "initial",
    connectorOptions = Map.empty,
    hadoopToken = None,
    schema = CdcSchema.parse(Map("schema" ->
      "id int, name string, amount double, created date, updated timestamp")),
    opColumn = "__op",
    maxRecordsPerBatch = 1000,
    queueCapacity = 10000)

  test("snapshot, insert, update and delete rows use the canonical envelope") {
    val row = new Struct(rowSchema)
      .put("id", 1)
      .put("name", "tom")
      .put("amount", 12.5d)
      .put("created", new Date(0L))
      .put("updated", new Date(1700000000000L))
    val source = new Struct(sourceSchema).put("ts_ms", 1700000000123L)
    val snapshot = this.record(row, row, source, "r")
    val insert = this.record(row, row, source, "c")
    val update = this.record(row, row, source, "u")
    val delete = this.record(row, null, source, "d")

    this.op(CdcRecordEncoder.debezium(snapshot, config, 1L).get) shouldBe "I"
    this.op(CdcRecordEncoder.debezium(insert, config, 3L).get) shouldBe "I"
    this.op(CdcRecordEncoder.debezium(update, config, 4L).get) shouldBe "U"
    val deleted = this.op(CdcRecordEncoder.debezium(delete, config, 5L).get)
    deleted shouldBe "D"

    val json = mapper.readTree(CdcRecordEncoder.debezium(insert, config, 3L).get)
    json.get("id").asInt() shouldBe 1
    json.get("name").asText() shouldBe "tom"
    json.get("amount").asDouble() shouldBe 12.5d
    json.get("_cdc_timestamp").asLong() shouldBe 1700000000123L
    json.get("_cdc_sequence").asLong() shouldBe 3L
    json.get("_cdc_table").asText() shouldBe "inventory.orders"
    json.get("created").asText() shouldBe "1970-01-01"
    json.get("updated").asText() should include ("2023-11-14T22:13:20")
  }

  test("epoch-millisecond timestamp longs are converted as milliseconds") {
    val epochSchema = SchemaBuilder.struct()
      .name("inventory.orders_epoch")
      .field("id", Schema.INT32_SCHEMA)
      .field("updated", Schema.OPTIONAL_INT64_SCHEMA)
      .optional()
      .build()
    val epochEnvelope = SchemaBuilder.struct()
      .field("before", epochSchema)
      .field("after", epochSchema)
      .field("source", sourceSchema)
      .field("op", Schema.STRING_SCHEMA)
      .build()
    val row = new Struct(epochSchema).put("id", 1).put("updated", 1700000000123L)
    val source = new Struct(sourceSchema).put("ts_ms", 1700000000123L)
    val value = new Struct(epochEnvelope)
      .put("before", row)
      .put("after", row)
      .put("source", source)
      .put("op", "c")
    val insert = new SourceRecord(
      Collections.emptyMap(), Collections.emptyMap(), "topic", epochEnvelope, value)
    val epochConfig = config.copy(
      schema = CdcSchema.parse(Map("schema" -> "id int, updated timestamp")))

    val json = mapper.readTree(CdcRecordEncoder.debezium(insert, epochConfig, 3L).get)
    json.get("updated").asText() shouldBe "2023-11-14T22:13:20.123Z"
  }

  test("tombstones and unsupported operations fail or are skipped explicitly") {
    CdcRecordEncoder.debezium(
      new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "topic", envelopeSchema, null),
      config, 1L) shouldBe None

    val heartbeatSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build()
    val heartbeat = new SourceRecord(
      Collections.emptyMap(),
      Collections.emptyMap(),
      "topic",
      heartbeatSchema,
      new Struct(heartbeatSchema).put("ts_ms", 1L))
    CdcRecordEncoder.debezium(heartbeat, config, 1L) shouldBe None

    val row = new Struct(rowSchema).put("id", 1)
    val source = new Struct(sourceSchema).put("ts_ms", 1L)
    an[org.pudding.core.PuddingException] should be thrownBy
      CdcRecordEncoder.debezium(this.record(row, row, source, "t"), config, 1L)
  }

  test("encode Mongo Binary, ObjectId and Decimal128 values consistently") {
    val mongoConfig = MongodbConfig(
      uri = "mongodb://localhost:27018",
      database = "source_db",
      collection = "orders",
      table = "source_db.orders",
      snapshotMode = "initial",
      maxAwaitTimeMs = 1000L,
      hadoopToken = None,
      schema = CdcSchema.parse(Map("schema" ->
        "_id string, blob binary, price decimal(10, 2)")),
      opColumn = "__op",
      maxRecordsPerBatch = 1000,
      queueCapacity = 10000)
    val bytes = Array[Byte](1, 2, 3)
    val id = new org.bson.types.ObjectId()
    val document = new Document("_id", id)
      .append("blob", new org.bson.types.Binary(bytes))
      .append("price", new org.bson.types.Decimal128(new java.math.BigDecimal("12.50")))

    val json = mapper.readTree(CdcRecordEncoder.mongoInsert(document, 1L, mongoConfig, 1L))
    json.get("_id").asText() shouldBe id.toHexString
    json.get("blob").asText() shouldBe Base64.getEncoder.encodeToString(bytes)
    json.get("price").asText() shouldBe "12.50"

    val delete = mapper.readTree(CdcRecordEncoder.mongoDelete(id, 2L, mongoConfig, 2L))
    delete.get("_id").asText() shouldBe id.toHexString
  }

  private def record(
      before: Struct,
      after: Struct,
      source: Struct,
      op: String): SourceRecord = {
    val value = new Struct(envelopeSchema)
      .put("before", before)
      .put("after", after)
      .put("source", source)
      .put("op", op)
    new SourceRecord(
      Collections.emptyMap(), Collections.emptyMap(), "topic", envelopeSchema, value)
  }

  private def op(json: String): String = mapper.readTree(json).get("__op").asText()
}
