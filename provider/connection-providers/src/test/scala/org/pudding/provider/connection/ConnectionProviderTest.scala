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

package org.pudding.provider.connection

import org.pudding.core.PuddingException
import org.pudding.core.configure.ConnectionConf
import org.pudding.core.connection.{ConnectionProvider, ConnectionRegistry}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for built-in connection providers and connection resolution.
 */
class ConnectionProviderTest extends AnyFunSuite with Matchers {

  private def connection(
      name: String = "test",
      connectionType: String = "jdbc",
      endpoint: Option[String],
      auth: Map[String, String] = Map.empty,
      properties: Map[String, String] = Map.empty): ConnectionConf = {
    ConnectionConf(name, connectionType, endpoint = endpoint, auth = auth, properties = properties)
  }

  test("register every built-in connection type") {
    ConnectionProvider.identifiers should contain allOf (
      "kafka", "jdbc", "mysql", "elasticsearch", "mongodb", "hive-metastore", "gravitino")
  }

  test("resolve a Kafka connection and SASL credentials") {
    val resolved = new KafkaConnectionProvider().resolve(connection(
      connectionType = "kafka",
      endpoint = Some("broker-1:9092,broker-2:9092"),
      auth = Map("username" -> "pudding", "password" -> "secret", "mechanism" -> "SCRAM-SHA-512"),
      properties = Map("security.protocol" -> "SASL_SSL")))

    resolved.options("kafka.bootstrap.servers") shouldBe "broker-1:9092,broker-2:9092"
    resolved.options("kafka.security.protocol") shouldBe "SASL_SSL"
    resolved.options("kafka.sasl.mechanism") shouldBe "SCRAM-SHA-512"
    resolved.options("kafka.sasl.jaas.config") should include (
      "org.apache.kafka.common.security.scram.ScramLoginModule")
    resolved.options("kafka.sasl.jaas.config") should include ("username=\"pudding\"")
    resolved.options("kafka.sasl.jaas.config") should include ("password=\"secret\"")
  }

  test("resolve a JDBC connection") {
    val resolved = new JdbcConnectionProvider().resolve(connection(
      connectionType = "jdbc",
      endpoint = Some("jdbc:mysql://db:3306/app"),
      auth = Map("username" -> "pudding", "password" -> "secret"),
      properties = Map("driver" -> "com.mysql.cj.jdbc.Driver", "fetchsize" -> "500")))

    resolved.options("url") shouldBe "jdbc:mysql://db:3306/app"
    resolved.options("user") shouldBe "pudding"
    resolved.options("password") shouldBe "secret"
    resolved.options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
    resolved.options("fetchsize") shouldBe "500"
  }

  test("mysql connection supplies the default driver") {
    val resolved = new MysqlConnectionProvider().resolve(connection(
      connectionType = "mysql",
      endpoint = Some("jdbc:mysql://db:3306/app"),
      auth = Map("username" -> "pudding", "password" -> "secret")))

    resolved.options("driver") shouldBe "com.mysql.cj.jdbc.Driver"
  }

  test("resolve an Elasticsearch connection") {
    val resolved = new ElasticsearchConnectionProvider().resolve(connection(
      connectionType = "elasticsearch",
      endpoint = Some("https://es-1:9200,es-2:9200"),
      auth = Map("username" -> "elastic", "password" -> "secret"),
      properties = Map("nodes.wan.only" -> "true")))

    resolved.options("es.nodes") shouldBe "es-1,es-2"
    resolved.options("es.port") shouldBe "9200"
    resolved.options("es.net.ssl") shouldBe "true"
    resolved.options("es.net.http.auth.user") shouldBe "elastic"
    resolved.options("es.net.http.auth.pass") shouldBe "secret"
    resolved.options("es.nodes.wan.only") shouldBe "true"
  }

  test("resolve a MongoDB connection with credentials and URI options") {
    val resolved = new MongoConnectionProvider().resolve(connection(
      connectionType = "mongodb",
      endpoint = Some("mongodb://mongo-1:27017,mongo-2:27017/app"),
      auth = Map("username" -> "pudding", "password" -> "p@ss word"),
      properties = Map("authSource" -> "admin", "replicaSet" -> "rs0")))

    val uri = resolved.options("connection.uri")
    uri should startWith ("mongodb://pudding:p%40ss%20word@mongo-1:27017,mongo-2:27017/app?")
    uri should include ("authSource=admin")
    uri should include ("replicaSet=rs0")
  }

  test("resolve a Hive metastore connection") {
    val resolved = new HiveMetastoreConnectionProvider().resolve(connection(
      connectionType = "hive-metastore",
      endpoint = Some("thrift://hms:9083"),
      properties = Map("metastore.sasl.enabled" -> "true")))

    resolved.options("hive.metastore.uris") shouldBe "thrift://hms:9083"
    resolved.options("hive.metastore.sasl.enabled") shouldBe "true"
  }

  test("resolve a Gravitino connection without Spark options") {
    val resolved = new GravitinoConnectionProvider().resolve(connection(
      connectionType = "gravitino",
      endpoint = Some("http://gravitino:8090"),
      auth = Map("token" -> "token-value")))

    resolved.endpoint shouldBe Some("http://gravitino:8090")
    resolved.auth("token") shouldBe "token-value"
    resolved.options shouldBe empty
  }

  test("ConnectionRegistry rejects duplicate names") {
    val error = intercept[PuddingException] {
      ConnectionRegistry(Seq(
        connection(name = "same", endpoint = Some("jdbc:mysql://db/app")),
        connection(name = "same", endpoint = Some("jdbc:mysql://db/app"))))
    }
    error.getMessage should include ("duplicated connection name: same")
  }

  test("ConnectionRegistry resolves lazily") {
    val registry = ConnectionRegistry(Seq(connection(
      name = "jdbc-conn",
      endpoint = Some("jdbc:mysql://db:3306/app"),
      auth = Map("username" -> "pudding", "password" -> "secret"))))

    registry.get("missing") shouldBe None
    registry.get("jdbc-conn").get.options("url") shouldBe "jdbc:mysql://db:3306/app"
  }
}
