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

package org.pudding.kafka.schema

import java.util.HashMap

import com.google.protobuf.{DynamicMessage, Message}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Round-trips Confluent Avro and Protobuf wire payloads through the decoder.
 */
class ConfluentValueDecoderTest extends AnyFunSuite with Matchers {

  private val sparkSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType)))

  test("decode a Confluent Avro payload") {
    val client = new MockSchemaRegistryClient()
    val properties = this.properties()

    val avroSchema = SchemaBuilder.record("Order")
      .fields()
      .requiredInt("id")
      .requiredString("name")
      .endRecord()
    val record = new GenericData.Record(avroSchema)
    record.put("id", 7)
    record.put("name", "avro-order")
    val payload = new KafkaAvroSerializer(client, properties)
      .serialize("orders", record)

    val row = new ConfluentValueDecoder(
      sparkSchema, properties, "avro", client).decode("orders", payload)

    row.getInt(0) shouldBe 7
    row.getString(1) shouldBe "avro-order"
  }

  test("decode a Confluent Protobuf payload") {
    val client = new MockSchemaRegistryClient()
    val properties = this.properties()

    val protobuf = new ProtobufSchema(
      """syntax = "proto3";
        |package test;
        |message Order {
        |  int32 id = 1;
        |  string name = 2;
        |}
        |""".stripMargin)
    val descriptor = protobuf.toDescriptor
    val message = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("id"), Integer.valueOf(9))
      .setField(descriptor.findFieldByName("name"), "proto-order")
      .build()
    val payload = new KafkaProtobufSerializer[Message](client, properties)
      .serialize("orders", message)

    val row = new ConfluentValueDecoder(
      sparkSchema, properties, "protobuf", client).decode("orders", payload)

    row.getInt(0) shouldBe 9
    row.getString(1) shouldBe "proto-order"
  }

  test("use a fixed Schema Registry subject") {
    val client = new MockSchemaRegistryClient()
    val properties = this.properties()
    properties.put(
      "value.subject.name.strategy", classOf[FixedSubjectNameStrategy].getName)
    properties.put(FixedSubjectNameStrategy.SubjectKey, "fixed-orders")

    val avroSchema = SchemaBuilder.record("Order")
      .fields()
      .requiredInt("id")
      .requiredString("name")
      .endRecord()
    val record = new GenericData.Record(avroSchema)
    record.put("id", 11)
    record.put("name", "fixed")
    val payload = new KafkaAvroSerializer(client, properties)
      .serialize("orders", record)

    val row = new ConfluentValueDecoder(
      sparkSchema, properties, "avro", client).decode("orders", payload)
    row.getInt(0) shouldBe 11
    row.getString(1) shouldBe "fixed"
  }

  test("reject a protobuf field missing from the configured schema") {
    val client = new MockSchemaRegistryClient()
    val properties = this.properties()
    val protobuf = new ProtobufSchema(
      """syntax = "proto3";
        |package test;
        |message Order { int32 id = 1; }
        |""".stripMargin)
    val descriptor = protobuf.toDescriptor
    val message = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("id"), Integer.valueOf(1))
      .build()
    val payload = new KafkaProtobufSerializer[Message](client, properties)
      .serialize("orders", message)

    val error = intercept[IllegalArgumentException] {
      new ConfluentValueDecoder(
        sparkSchema, properties, "protobuf", client).decode("orders", payload)
    }
    error.getMessage should include ("name")
  }

  private def properties(): HashMap[String, String] = {
    val values = new HashMap[String, String]()
    values.put("schema.registry.url", "mock://")
    values
  }
}
