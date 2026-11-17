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

package org.pudding.core.connection

import scala.collection.mutable
import scala.util.control.NonFatal

import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf
import org.pudding.core.util.ConfigRedactor

/**
 * Resolves connections lazily, so a job only pays for and validates the connections it uses.
 */
class ConnectionRegistry(confMap: Map[String, ConnectionConf]) {

  private val resolved = mutable.Map[String, ResolvedConnection]()

  def get(name: String): Option[ResolvedConnection] = {
    confMap.get(name).map { conf =>
      resolved.getOrElseUpdate(name, {
        try {
          ConnectionProvider.forType(conf.`type`).resolve(conf)
        } catch {
          case NonFatal(error) =>
            val detail = ConfigRedactor.redactLine(
              Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName))
            throw new PuddingException(
              s"failed to resolve connection '$name' (type '${conf.`type`}'): $detail",
              error)
        }
      })
    }
  }
}

object ConnectionRegistry {

  /** Empty registry for direct provider tests and callers that declare no connections. */
  val Empty: ConnectionRegistry = new ConnectionRegistry(Map.empty)

  def apply(confs: Seq[ConnectionConf]): ConnectionRegistry = {
    val nonNull = Option(confs).getOrElse(Seq.empty).filter(_ != null)
    val duplicated = nonNull.map(_.name).groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }.toSeq.sorted
    if (duplicated.nonEmpty) {
      throw new PuddingException(s"duplicated connection name: ${duplicated.mkString(", ")}")
    }
    new ConnectionRegistry(nonNull.map(conf => conf.name -> conf).toMap)
  }
}
