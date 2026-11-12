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

package org.pudding.core.connection

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Locale
import java.util.Properties
import java.util.regex.Matcher

import scala.collection.JavaConverters._

import org.pudding.core.PuddingException

/**
 * Resolves secret references on the driver. Job configuration only carries references, so the
 * submitted file, client logs and command line never contain the credential itself.
 *
 * Supported references:
 *  - `${secret:env/NAME}` reads an environment variable;
 *  - `${secret:file:/path/to/file#key}` reads a key from a properties file;
 *  - `${secret:file:/path/to/file}` reads and trims the whole file.
 *
 * A reference may be embedded in a larger string. If the whole value is one reference, the
 * resolved value is returned unchanged.
 */
object SecretResolver {

  private val Prefix = "${secret:"

  private val Reference = """\$\{secret:([^}]+)\}""".r

  def containsReference(value: String): Boolean = value != null && value.contains(Prefix)

  def resolve(value: String): String = {
    if (!this.containsReference(value)) {
      value
    } else {
      Reference.replaceAllIn(value, matcher => Matcher.quoteReplacement(resolveReference(matcher.group(1))))
    }
  }

  def resolveMap(values: Map[String, String]): Map[String, String] = {
    if (values == null) {
      Map.empty
    } else {
      values.map { case (key, value) => key -> this.resolve(value) }
    }
  }

  /**
   * Recursively resolve string values in a configuration map. Keys are never interpreted.
   */
  def resolveConfig(values: Option[Map[String, Any]]): Option[Map[String, Any]] = {
    values.map(_.map { case (key, value) => key -> resolveValue(value) })
  }

  private def resolveValue(value: Any): Any = value match {
    case text: String => this.resolve(text)
    case map: Map[_, _] => map.map { case (key, item) => key -> resolveValue(item) }.toMap
    case list: Seq[_] => list.map(resolveValue)
    case map: java.util.Map[_, _] =>
      map.asScala.map { case (key, item) => key -> resolveValue(item) }.toMap
    case list: java.util.List[_] => list.asScala.map(resolveValue).toList
    case other => other
  }

  private def resolveReference(reference: String): String = {
    // Two accepted shapes: env/NAME and file:/path#key. The file provider keeps its
    // leading slash, so the scheme is separated by ':' when present.
    val colon = reference.indexOf(':')
    val slash = reference.indexOf('/')
    val (scheme, name) = if (colon > 0) {
      (reference.substring(0, colon), reference.substring(colon + 1))
    } else if (slash > 0 && slash < reference.length - 1) {
      (reference.substring(0, slash), reference.substring(slash + 1))
    } else {
      throw new PuddingException(
        s"invalid secret reference; expected $${secret:env/NAME} or $${secret:file:/path#key}")
    }

    scheme.toLowerCase(Locale.ROOT) match {
      case "env" => this.resolveEnvironment(name)
      case "file" => this.resolveFile(name)
      case other =>
        throw new PuddingException(s"unsupported secret provider: $other, supported: env, file")
    }
  }

  private def resolveEnvironment(name: String): String = {
    sys.env.get(name).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"secret environment variable is not set: $name")
    }
  }

  private def resolveFile(reference: String): String = {
    val separator = reference.lastIndexOf('#')
    if (separator > 0 && separator < reference.length - 1) {
      val path = reference.substring(0, separator)
      val key = reference.substring(separator + 1)
      val properties = new Properties()
      try {
        val input = new FileInputStream(path)
        try properties.load(input) finally input.close()
      } catch {
        case error: Exception =>
          throw new PuddingException(s"failed to read secret file: $path: ${error.getMessage}")
      }
      Option(properties.getProperty(key)).getOrElse {
        throw new PuddingException(s"secret key is not present in file: $path, key: $key")
      }
    } else {
      try {
        new String(Files.readAllBytes(Paths.get(reference)), StandardCharsets.UTF_8).trim
      } catch {
        case error: Exception =>
          throw new PuddingException(
            s"failed to read secret file: $reference: ${error.getMessage}")
      }
    }
  }
}
