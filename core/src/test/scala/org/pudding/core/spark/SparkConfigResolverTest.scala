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

package org.pudding.core.spark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf

import org.pudding.core.PuddingException
import org.pudding.core.catalog.ResolvedCatalog
import org.pudding.core.configure.{JobConf, JobMode}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkConfigResolverTest extends AnyFunSuite with Matchers {

  private def job(config: Map[String, String]): JobConf = {
    JobConf(
      sparkHome = "",
      sparkPuddingHome = None,
      appName = "config resolver test",
      master = "local[2]",
      deployMode = "client",
      describe = None,
      config = Some(config),
      mode = JobMode.Batch)
  }

  private def catalog(name: String, config: Map[String, String]): ResolvedCatalog = {
    ResolvedCatalog(
      name = name,
      `type` = "iceberg",
      sparkCatalogName = name,
      sessionCatalog = false,
      sparkConf = config,
      properties = Map.empty)
  }

  test("merge non-conflicting job, catalog and runtime settings") {
    val runtime = new SparkConf(false)
      .set("spark.runtime", "runtime")
      .set("spark.shared", "same")
      .set(SparkConfigResolver.MasterKey, "local[4]")
    val resolved = SparkConfigResolver.resolve(
      job(Map(
        "spark.job" -> "job",
        "spark.shared" -> "same",
        SparkConfigResolver.MasterKey -> "ignored-target-master")),
      Seq(catalog("lake", Map("spark.catalog" -> "catalog"))),
      runtime)

    resolved.get("spark.job") shouldBe "job"
    resolved.get("spark.catalog") shouldBe "catalog"
    resolved.get("spark.runtime") shouldBe "runtime"
    resolved.get("spark.shared") shouldBe "same"
    resolved.get(SparkConfigResolver.MasterKey) shouldBe "local[4]"
  }

  test("reject conflicting job and catalog values without exposing values") {
    val error = intercept[PuddingException] {
      SparkConfigResolver.resolve(
        job(Map("spark.conflict" -> "job-value")),
        Seq(catalog("lake", Map("spark.conflict" -> "catalog-value"))),
        new SparkConf(false))
    }

    error.getMessage should include ("spark.conflict")
    error.getMessage should include ("job.config")
    error.getMessage should include ("catalog 'lake'")
    error.getMessage should not include ("job-value")
    error.getMessage should not include ("catalog-value")
  }

  test("reject a conflict between job and runtime settings") {
    val runtime = new SparkConf(false).set("spark.conflict", "runtime-value")
    val error = intercept[PuddingException] {
      SparkConfigResolver.resolve(
        job(Map("spark.conflict" -> "job-value")),
        Seq.empty,
        runtime)
    }

    error.getMessage should include ("spark.conflict")
    error.getMessage should include ("runtime SparkConf")
    error.getMessage should not include ("job-value")
    error.getMessage should not include ("runtime-value")
  }

  test("resolve secret references in runtime settings before comparing them") {
    val secretFile = Files.createTempFile("spark-config-secret", ".properties")
    Files.write(
      secretFile,
      "token=resolved-value\n".getBytes(StandardCharsets.UTF_8))
    try {
      val reference = s"$${secret:file:$secretFile#token}"
      val resolved = SparkConfigResolver.resolve(
        job(Map("spark.secret" -> "resolved-value")),
        Seq.empty,
        new SparkConf(false)
          .set("spark.secret", reference)
          .set("spark.runtime-secret", reference))

      resolved.get("spark.secret") shouldBe "resolved-value"
      resolved.get("spark.runtime-secret") shouldBe "resolved-value"
    } finally {
      Files.deleteIfExists(Paths.get(secretFile.toString))
    }
  }

  test("merge Spark extensions from all sources") {
    val resolved = SparkConfigResolver.resolve(
      job(Map(SparkConfigResolver.ExtensionsKey -> "com.example.JobExtension")),
      Seq(catalog(
        "lake",
        Map(SparkConfigResolver.ExtensionsKey -> "com.example.CatalogExtension"))),
      new SparkConf(false).set(
        SparkConfigResolver.ExtensionsKey, "com.example.RuntimeExtension"))

    val extensions = resolved.get(SparkConfigResolver.ExtensionsKey).split(",").toSet
    extensions should contain allOf (
      "com.example.JobExtension",
      "com.example.CatalogExtension",
      "com.example.RuntimeExtension")
  }

  test("ignore target-owned keys in job.config") {
    val resolved = SparkConfigResolver.resolve(
      job(Map(
        SparkConfigResolver.MasterKey -> "job-config-master",
        SparkConfigResolver.DeployModeKey -> "cluster",
        SparkConfigResolver.AppNameKey -> "job-config-name")),
      Seq.empty,
      new SparkConf(false).set(SparkConfigResolver.MasterKey, "runtime-master"))

    resolved.get(SparkConfigResolver.MasterKey) shouldBe "runtime-master"
    resolved.contains(SparkConfigResolver.DeployModeKey) shouldBe false
    resolved.contains(SparkConfigResolver.AppNameKey) shouldBe false
  }
}
