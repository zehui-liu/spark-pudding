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

package org.pudding.kafka.schema

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Test double for the optional runtime class. The loader only depends on this constructor and
 * `decode(String, Array[Byte])`, so the Kafka connector can be tested without Confluent jars.
 */
class ConfluentValueDecoder(
    schema: StructType,
    config: java.util.Map[String, String],
    format: String) extends Serializable {

  def decode(topic: String, value: Array[Byte]): Row = {
    Row(value.head.toInt, new String(value.tail, StandardCharsets.UTF_8))
  }
}
