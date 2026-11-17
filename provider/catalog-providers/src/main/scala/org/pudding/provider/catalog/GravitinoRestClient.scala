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

package org.pudding.provider.catalog

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.io.Source

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.pudding.core.PuddingException
import org.pudding.core.util.ConfigRedactor

/**
 * Minimal Gravitino REST client used to read catalog metadata. It deliberately does not depend on
 * the Gravitino Java client or Spark connector: only the catalog name, provider and properties
 * are needed, and the underlying Spark catalog is installed natively.
 */
class GravitinoRestClient(
    endpoint: String,
    token: Option[String],
    username: Option[String],
    password: Option[String],
    timeoutMs: Int = 30000) {

  private implicit val formats: Formats = DefaultFormats

  private val AcceptHeader = "application/vnd.gravitino.v1+json"

  def loadCatalog(metalake: String, catalog: String): GravitinoCatalogInfo = {
    val base = endpoint.stripSuffix("/")
    val url = new URL(
      s"$base/api/metalakes/${encode(metalake)}/catalogs/${encode(catalog)}")
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Accept", AcceptHeader)
    connection.setConnectTimeout(timeoutMs)
    connection.setReadTimeout(timeoutMs)
    this.authorization.foreach(value => connection.setRequestProperty("Authorization", value))

    val status = try {
      connection.getResponseCode
    } catch {
      case error: Exception =>
        connection.disconnect()
        throw new PuddingException(
          s"failed to connect to Gravitino at ${ConfigRedactor.redactLine(base)}: " +
            error.getClass.getSimpleName)
    }

    if (status < 200 || status >= 300) {
      connection.disconnect()
      val detail = if (status == 401 || status == 403) "authentication failed" else "request failed"
      throw new PuddingException(
        s"Gravitino catalog '$metalake.$catalog' $detail: HTTP $status")
    }

    val body = readBody(connection)
    this.parseCatalog(body)
  }

  private def authorization: Option[String] = {
    token.map(value => s"Bearer $value").orElse {
      for {
        user <- username
        pass <- password
      } yield {
        val encoded = Base64.getEncoder.encodeToString(
          s"$user:$pass".getBytes(StandardCharsets.UTF_8))
        s"Basic $encoded"
      }
    }
  }

  private def parseCatalog(body: String): GravitinoCatalogInfo = {
    val response = try {
      parse(body)
    } catch {
      case _: Exception =>
        throw new PuddingException("Gravitino returned an invalid catalog response")
    }

    val code = (response \ "code") match {
      case JInt(value) => value.toInt
      case _ => 0
    }
    if (code != 0) {
      val message = ConfigRedactor.redactLine(
        optionalString(response \ "message").getOrElse("unknown error"))
      throw new PuddingException(s"Gravitino catalog request failed: $message")
    }

    val catalogJson = response \ "catalog" match {
      case JNothing => response
      case value => value
    }
    if (catalogJson == JNothing) {
      throw new PuddingException("Gravitino response does not contain a catalog")
    }

    val catalogType = optionalString(catalogJson \ "type").getOrElse("")
    val provider = optionalString(catalogJson \ "provider").getOrElse("")
    val properties = catalogJson \ "properties" match {
      case JObject(fields) => fields.map {
        case JField(key, value) => key -> valueAsString(value)
      }.toMap
      case _ => Map.empty[String, String]
    }

    GravitinoCatalogInfo(catalogType, provider, properties)
  }

  private def readBody(connection: HttpURLConnection): String = {
    val stream = connection.getInputStream
    try {
      Source.fromInputStream(stream, StandardCharsets.UTF_8.name()).mkString
    } finally {
      stream.close()
      connection.disconnect()
    }
  }

  private def optionalString(value: JValue): Option[String] = value match {
    case JString(text) => Some(text)
    case _ => None
  }

  private def valueAsString(value: JValue): String = value match {
    case JString(text) => text
    case JBool(bool) => bool.toString
    case JInt(number) => number.toString
    case JDouble(number) => number.toString
    case JDecimal(number) => number.toString
    case JNull | JNothing => ""
    case other => compact(render(other))
  }

  private def encode(value: String): String = {
    URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20")
  }
}

case class GravitinoCatalogInfo(
  `type`: String,
  provider: String,
  properties: Map[String, String])
