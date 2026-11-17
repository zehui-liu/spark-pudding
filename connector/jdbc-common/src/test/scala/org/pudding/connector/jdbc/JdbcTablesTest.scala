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

package org.pudding.connector.jdbc

import java.sql.{DriverManager, SQLException}
import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JdbcTablesTest extends AnyFunSuite with Matchers {

  test("distinguish a missing table from other SQL failures") {
    JdbcTables.isMissingTable(new SQLException("missing", "42S02")) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", "42S03")) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", "42S04")) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", "42P01")) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", "S0002")) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", null, 942)) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", null, 42103)) shouldBe true
    JdbcTables.isMissingTable(new SQLException("missing", "90079")) shouldBe true

    JdbcTables.isMissingTable(new SQLException("denied", "42501")) shouldBe false
    JdbcTables.isMissingTable(new SQLException("connection lost", "08006")) shouldBe false
    JdbcTables.isMissingTable(new SQLException("serialization", "40001")) shouldBe false
  }

  test("probe an H2 table without treating an error as absence") {
    val database = "jdbc:table_probe_" + UUID.randomUUID().toString.replace("-", "")
    val url = s"jdbc:h2:mem:$database;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
    JdbcTables.tableExists(url, "sa", "", "orders") shouldBe false

    val connection = DriverManager.getConnection(url, "sa", "")
    try {
      val statement = connection.createStatement()
      try statement.execute("CREATE TABLE orders (id INT PRIMARY KEY)")
      finally statement.close()
    } finally {
      connection.close()
    }

    JdbcTables.tableExists(url, "sa", "", "orders") shouldBe true
    // H2 with DATABASE_TO_LOWER stores the unquoted name as `orders`; a quoted probe for
    // `Orders` misses, so metadata must confirm the case-folded table.
    JdbcTables.tableExists(url, "sa", "", "Orders") shouldBe true
    JdbcTables.tableExists(url, "sa", "", "public.Orders") shouldBe true
    // H2 exposes the catalog without the `jdbc:` URL prefix used in the database name.
    val catalog = database.stripPrefix("jdbc:")
    JdbcTables.tableExists(url, "sa", "", s"$catalog.public.Orders") shouldBe true
    JdbcTables.tableExists(url, "sa", "", "missing.Orders") shouldBe false
    JdbcTables.tableExists(url, "sa", "", "missing") shouldBe false
  }
}
