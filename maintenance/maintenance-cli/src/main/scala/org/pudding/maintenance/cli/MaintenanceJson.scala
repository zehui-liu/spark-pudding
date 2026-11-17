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

package org.pudding.maintenance.cli

import org.pudding.maintenance.api.MaintenanceResult

/**
 * Small dependency-free JSON writer for the maintenance result. Values are strings, so the
 * output stays stable and does not require a JSON library on the Spark classpath.
 */
private[cli] object MaintenanceJson {

  def render(result: MaintenanceResult): String = {
    val fields = Seq(
      "format" -> this.string(result.format),
      "action" -> this.string(result.action),
      "target" -> this.string(result.target),
      "dryRun" -> result.dryRun.toString,
      "executed" -> result.executed.toString,
      "statements" -> result.statements.map(this.string).mkString("[", ",", "]"),
      "summary" -> this.map(result.summary),
      "rows" -> result.rows.map(this.map).mkString("[", ",", "]"),
      "message" -> this.string(result.message))
    fields.map { case (key, value) => this.string(key) + ":" + value }.mkString("{", ",", "}")
  }

  def error(message: String): String = {
    "{" + this.string("error") + ":" + this.string(message) + "}"
  }

  private def map(values: Map[String, String]): String = {
    values.toSeq.sortBy(_._1)
      .map { case (key, value) => this.string(key) + ":" + this.string(value) }
      .mkString("{", ",", "}")
  }

  private def string(value: String): String = {
    val builder = new StringBuilder("\"")
    Option(value).getOrElse("").foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case character if character < ' ' =>
        builder.append(f"\\u${character.toInt}%04x")
      case character => builder.append(character)
    }
    builder.append("\"").toString()
  }
}
