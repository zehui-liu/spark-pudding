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

import io.confluent.kafka.serializers.subject.SubjectNameStrategy

/**
 * Schema Registry subject strategy that uses one configured subject for every topic.
 *
 * The Connector configures this class only when `schemaRegistry.subject` is set. It lets one
 * decoder read a topic that does not follow the default `<topic>-value` naming convention.
 */
class FixedSubjectNameStrategy extends SubjectNameStrategy {

  private var configuredSubject: String = _

  override def configure(config: java.util.Map[String, _]): Unit = {
    configuredSubject = Option(config.get(FixedSubjectNameStrategy.SubjectKey))
      .map(_.toString)
      .filter(_.nonEmpty)
      .getOrElse {
        throw new IllegalArgumentException(
          s"missing schema registry subject setting: ${FixedSubjectNameStrategy.SubjectKey}")
      }
  }

  override def getSubjectName(
      topic: String,
      isKey: Boolean,
      value: Any): String = {
    if (configuredSubject == null || configuredSubject.isEmpty) {
      throw new IllegalStateException("schema registry subject strategy is not configured")
    }
    configuredSubject
  }
}

object FixedSubjectNameStrategy {
  val SubjectKey = "pudding.schema.registry.subject"
}
