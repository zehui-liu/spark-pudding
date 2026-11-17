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

package org.pudding.core.catalog

import org.pudding.core.PuddingException

/**
 * Qualifies a table name for a resolved catalog. A session catalog keeps the table as written;
 * a named catalog accepts `table`, `database.table` or `catalog.database.table` and fills in the
 * default database when only the table is supplied.
 */
object CatalogTableName {

  def qualify(
      catalog: ResolvedCatalog,
      table: String,
      defaultDatabase: String = "default"): String = {
    if (table == null || table.trim.isEmpty) {
      throw new PuddingException("table can not be empty!")
    }
    val trimmed = table.trim
    if (catalog.sessionCatalog) {
      trimmed
    } else {
      val parts = trimmed.split("\\.", -1).map(_.trim)
      if (parts.exists(_.isEmpty)) {
        throw new PuddingException(s"invalid table identifier: $trimmed")
      }
      parts.length match {
        case 1 => s"${catalog.sparkCatalogName}.$defaultDatabase.${parts.head}"
        case 2 => s"${catalog.sparkCatalogName}.${parts.mkString(".")}"
        case _ => parts.mkString(".")
      }
    }
  }
}
