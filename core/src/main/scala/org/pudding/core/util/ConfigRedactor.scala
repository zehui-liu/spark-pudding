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

package org.pudding.core.util

import java.util.Locale
import java.util.regex.Matcher

import scala.util.control.NonFatal

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.pudding.core.PuddingException

/**
 * Redact credentials from configuration before it is written to logs.
 *
 * Redaction is applied at two levels:
 *  - a value whose key looks sensitive, such as `password`, `secret` or `token`,
 *    is replaced with [[REDACTED]];
 *  - credentials embedded in ordinary string values, such as URL user-info or
 *    `password=...`, are masked as well.
 */
object ConfigRedactor {

  val REDACTED = "******"

  private val SENSITIVE_KEY_PARTS = Seq(
    "password",
    "passwd",
    "passphrase",
    "pwd",
    "secret",
    "token",
    "credential",
    "privatekey",
    "accesskey",
    "apikey",
    "accountkey",
    "keytab",
    "jaasconfig",
    "authorization",
    "cookie",
    "sessionid",
    "signature",
    "userinfo"
  )

  private val SENSITIVE_KEYS = Set("pass")

  private val URL_USER_INFO =
    """(?i)([a-z][a-z0-9+.-]*://[^/\s:@]*:)([^@/\s]+)(@)""".r

  private val SECRET_ASSIGNMENT = (
    """(?i)((?:"?(?:password|passwd|passphrase|pwd|secret|token|signature|""" +
      """access[._-]?key|secret[._-]?key|api[._-]?key|account[._-]?key|""" +
      """private[._-]?key|credentials?|authorization|cookie)"?\s*(?:=|:|->)\s*))""" +
      """(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\r\n,;&}\]]+)"""
    ).r

  /**
   * Serialize a value to JSON with credentials redacted.
   *
   * If the value can not be serialized, the redaction marker is returned instead of
   * the original value. Logging must never expose the unredacted configuration.
   */
  def redactToJson(value: Any)(implicit formats: Formats): String = {
    try {
      compact(render(this.redactJson(Extraction.decompose(value))))
    } catch {
      case NonFatal(_) => REDACTED
    }
  }

  /**
   * Serialize a value with credentials redacted and map keys sorted. The result is suitable for
   * a stable fingerprint: reordering configuration keys does not change it, while changing a
   * non-sensitive value does.
   *
   * A serialization failure is reported instead of falling back to a constant. A constant would
   * make unrelated configurations share a fingerprint and could let a changed plan pass the
   * incremental transaction check.
   */
  def canonicalRedactedJson(value: Any)(implicit formats: Formats): String = {
    try {
      compact(render(this.sortJson(this.redactJson(Extraction.decompose(value)))))
    } catch {
      case NonFatal(error) =>
        throw new PuddingException(
          "failed to serialize a configuration value for a stable fingerprint", error)
    }
  }

  private def sortJson(value: JValue): JValue = value match {
    case JObject(fields) =>
      JObject(fields.sortBy { case JField(name, _) => name }.map {
        case JField(name, fieldValue) => JField(name, this.sortJson(fieldValue))
      })
    case JArray(values) => JArray(values.map(this.sortJson))
    case other => other
  }

  private def redactJson(value: JValue): JValue = value match {
    case JObject(fields) =>
      JObject(fields.map {
        case JField(name, fieldValue) =>
          if (this.isSensitiveKey(name)) {
            JField(name, JString(REDACTED))
          } else {
            JField(name, this.redactJson(fieldValue))
          }
      })
    case JArray(values) =>
      JArray(values.map(this.redactJson))
    case JString(text) =>
      JString(this.redactText(text))
    case other =>
      other
  }

  /**
   * Redact credentials from one log line or a short diagnostic string.
   *
   * This accepts arbitrary text, not only serialized configuration, and is safe to call for
   * every line captured by a log collector. A null value is returned unchanged.
   */
  def redactLine(text: String): String = {
    if (text == null) null else this.redactText(text)
  }

  /**
   * Redact credentials embedded in a non-sensitive value, such as a JDBC URL or a JAAS string.
   */
  private def redactText(text: String): String = {
    // replaceAllIn feeds the returned string to Matcher.appendReplacement, which interprets
    // '$' and '\' as group references. Quote the matched prefix so a key containing those
    // characters can not make redaction fail.
    val userInfoRedacted = URL_USER_INFO.replaceAllIn(
      text,
      matcher => Matcher.quoteReplacement(
        matcher.group(1) + REDACTED + matcher.group(3)))
    SECRET_ASSIGNMENT.replaceAllIn(
      userInfoRedacted,
      matcher => Matcher.quoteReplacement(matcher.group(1) + REDACTED))
  }

  private def isSensitiveKey(key: String): Boolean = {
    if (key == null) {
      false
    } else {
      val normalized = key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "")
      SENSITIVE_KEYS.contains(normalized) || SENSITIVE_KEY_PARTS.exists(normalized.contains)
    }
  }
}
