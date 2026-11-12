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

package org.pudding.maintenance.server

import scala.util.control.NonFatal

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization

import org.pudding.maintenance.api._

/**
 * JSON representation for the maintenance control plane. Request parsing is explicit so missing
 * or wrongly typed fields produce a key-specific usage error instead of a serializer stack trace.
 */
private[server] object MaintenanceHttpJson {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def parseRequest(text: String): MaintenanceRequest = {
    if (text == null || text.trim.isEmpty) {
      throw new MaintenanceUsageException("request body is empty")
    }
    val root = try {
      parse(text)
    } catch {
      case NonFatal(_) => throw new MaintenanceUsageException("request body is not valid JSON")
    }
    val obj = root match {
      case value: JObject => value
      case _ => throw new MaintenanceUsageException("request body must be a JSON object")
    }

    MaintenanceRequest(
      format = requiredString(obj, "format"),
      action = requiredString(obj, "action"),
      catalog = optionalString(obj, "catalog"),
      table = optionalString(obj, "table"),
      path = optionalString(obj, "path"),
      retention = retention(obj \ "retention"),
      rewrite = rewrite(obj \ "rewrite"),
      options = options(obj \ "options"),
      dryRun = optionalBoolean(obj, "dryRun").getOrElse(false),
      confirmed = optionalBoolean(obj, "confirmed").getOrElse(false))
  }

  def task(value: MaintenanceTask): String = Serialization.writePretty(value)

  def tasks(values: Seq[MaintenanceTask]): String = Serialization.writePretty(values)

  def error(message: String): String = compact(render(JObject(
    "error" -> JString(Option(message).getOrElse("maintenance request failed")))))

  def capabilities: String = {
    val formats = MaintenanceActions.formats
    val actions = formats.map { format =>
      format -> JArray(MaintenanceActions.specs(format).map { spec =>
        JObject(
          "name" -> JString(spec.name),
          "description" -> JString(spec.description),
          "destructive" -> JBool(spec.destructive),
          "nativeDryRun" -> JBool(spec.nativeDryRun))
      }.toList)
    }
    compact(render(JObject(
      "formats" -> JArray(formats.map(JString).toList),
      "actions" -> JObject(actions.toList))))
  }

  def health(counts: Map[String, Int]): String = {
    compact(render(JObject(
      "status" -> JString("UP"),
      "tasks" -> JObject(counts.toSeq.sortBy(_._1).map {
        case (key, value) => key -> JInt(value)
      }.toList))))
  }

  private def retention(value: JValue): MaintenanceRetention = value match {
    case JNothing | JNull => MaintenanceRetention()
    case obj: JObject =>
      MaintenanceRetention(
        olderThan = optionalString(obj, "olderThan"),
        retainLast = optionalInt(obj, "retainLast"),
        retainMax = optionalInt(obj, "retainMax"),
        retainMin = optionalInt(obj, "retainMin"))
    case _ => throw new MaintenanceUsageException("retention must be a JSON object")
  }

  private def rewrite(value: JValue): MaintenanceRewrite = value match {
    case JNothing | JNull => MaintenanceRewrite()
    case obj: JObject =>
      MaintenanceRewrite(
        targetFileSize = optionalString(obj, "targetFileSize"),
        minInputFiles = optionalInt(obj, "minInputFiles"),
        where = optionalString(obj, "where"),
        strategy = optionalString(obj, "strategy"))
    case _ => throw new MaintenanceUsageException("rewrite must be a JSON object")
  }

  private def options(value: JValue): Map[String, String] = value match {
    case JNothing | JNull => Map.empty
    case JObject(fields) =>
      fields.map {
        case JField(key, JString(value)) => key -> value
        case JField(key, JInt(value)) => key -> value.toString
        case JField(key, JBool(value)) => key -> value.toString
        case JField(key, _) =>
          throw new MaintenanceUsageException(
            s"options.$key must be a string, number or boolean")
      }.toMap
    case _ => throw new MaintenanceUsageException("options must be a JSON object")
  }

  private def requiredString(obj: JObject, key: String): String = {
    optionalString(obj, key).getOrElse {
      throw new MaintenanceUsageException(s"maintenance request requires '$key'")
    }
  }

  private def optionalString(obj: JObject, key: String): Option[String] = obj \ key match {
    case JNothing | JNull => None
    case JString(value) => Option(value.trim).filter(_.nonEmpty)
    case _ => throw new MaintenanceUsageException(s"'$key' must be a string")
  }

  private def optionalBoolean(obj: JObject, key: String): Option[Boolean] = obj \ key match {
    case JNothing | JNull => None
    case JBool(value) => Some(value)
    case _ => throw new MaintenanceUsageException(s"'$key' must be a boolean")
  }

  private def optionalInt(obj: JObject, key: String): Option[Int] = obj \ key match {
    case JNothing | JNull => None
    case JInt(value) if value.isValidInt => Some(value.toInt)
    case JString(value) if value.trim.nonEmpty =>
      try Some(value.trim.toInt)
      catch {
        case _: NumberFormatException =>
          throw new MaintenanceUsageException(s"'$key' must be an integer")
      }
    case _ => throw new MaintenanceUsageException(s"'$key' must be an integer")
  }
}
