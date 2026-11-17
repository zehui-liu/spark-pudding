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

package org.pudding.core.util

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization

import org.pudding.core.PuddingException
import org.pudding.core.configure.{SourceConf, TransformConf}

/**
 * Guards a stateful streaming checkpoint against an incompatible source/transform
 * configuration.
 *
 * The contract is a SHA-256 fingerprint of the source configuration, the ordered transform
 * chain and the Spark version. Only the hash is stored; the canonical configuration is
 * redacted before hashing and is never written to the checkpoint. A changed hash does not prove
 * that the checkpoint is incompatible, but it is a safe conservative check: an operator must
 * either restore the previous configuration or explicitly start a new checkpoint.
 */
object StreamingStateContract extends Logging {

  val Version = 1

  val FileName = "_pudding_state_contract.json"

  private val ContractPrefix = "sha256:"

  private val Kind = "pudding-streaming-state"

  /**
   * Reader tuning and metrics settings do not change the state schema. Excluding them lets an
   * operator adjust throughput or lag sampling without being forced to start a new checkpoint.
   */
  private val NonStateSourceKeys = Set("options", "lagMetricsIntervalMs")

  /**
   * Build a stable fingerprint for a stateful streaming pipeline. Map key order and changed
   * secret values do not change the fingerprint; changed non-sensitive source or transform
   * settings do.
   */
  def fingerprint(source: SourceConf, transforms: Seq[TransformConf]): String = {
    if (source == null) {
      throw new PuddingException("streaming state contract requires a source")
    }
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val payload = Map[String, Any](
      "kind" -> Kind,
      "version" -> Version,
      "sparkVersion" -> SPARK_VERSION,
      "source" -> Map[String, Any](
        "id" -> source.id,
        "type" -> source.`type`,
        "config" -> source.config.getOrElse(Map.empty[String, Any])
          .filterNot { case (key, _) => NonStateSourceKeys.contains(key) },
        "connection" -> source.connection,
        "catalog" -> source.catalog),
      "transforms" -> transforms.map { transform =>
        Map[String, Any](
          "id" -> transform.id,
          "type" -> transform.`type`,
          "dependencies" -> transform.dependencies,
          "config" -> transform.config.getOrElse(Map.empty[String, Any]))
      })
    val canonical = ConfigRedactor.canonicalRedactedJson(payload)
    if (canonical == ConfigRedactor.REDACTED) {
      throw new PuddingException(
        "streaming state contract could not serialize the source/transform configuration")
    }
    ContractPrefix + this.sha256(canonical)
  }

  /**
   * Verify the contract stored at `location`, or create it for a new or pre-contract
   * checkpoint. A missing contract on an existing checkpoint is adopted with a warning, so
   * upgrading does not make an old checkpoint unusable; the guard starts with the next run.
   */
  def verify(
      location: String,
      expected: String,
      configuration: Configuration = new Configuration()): Unit = {
    val checkpoint = this.checkpointPath(location)
    val contract = this.requireContract(expected)
    val path = new Path(checkpoint)
    val fileSystem = path.getFileSystem(configuration)
    val root = fileSystem.makeQualified(path)
    if (fileSystem.exists(root) && !fileSystem.getFileStatus(root).isDirectory) {
      throw new PuddingException(
        s"checkpoint location '$location' is not a directory")
    }
    val contractPath = new Path(root, FileName)
    if (fileSystem.exists(contractPath)) {
      val stored = this.read(fileSystem, contractPath)
      this.requireCompatible(location, contractPath, stored, contract)
    } else {
      if (fileSystem.exists(root) &&
        fileSystem.exists(new Path(root, "commits"))) {
        logWarning(
          s"adopting an existing streaming checkpoint without a state contract at $location; " +
            "the current source/transform configuration becomes the contract")
      }
      this.write(fileSystem, root, contractPath, contract)
    }
  }

  private def checkpointPath(location: String): String = {
    Option(location).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("streaming state contract requires checkpointLocation")
    }
  }

  private def requireContract(value: String): String = {
    val contract = Option(value).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException("streaming state contract can not be empty")
    }
    if (!contract.toLowerCase(Locale.ROOT).startsWith(ContractPrefix)) {
      throw new PuddingException(
        s"streaming state contract must start with '$ContractPrefix'")
    }
    contract
  }

  private def requireCompatible(
      location: String,
      path: Path,
      stored: StateContractDocument,
      expected: String): Unit = {
    if (stored.version != Version) {
      throw new PuddingException(
        s"state contract file '$path' has unsupported version ${stored.version}; " +
          s"expected $Version")
    }
    if (stored.contract != expected) {
      throw new PuddingException(
        s"checkpoint '$location' was created with a different streaming state contract; " +
          "the source/transform configuration or Spark version changed. Restore the previous " +
          "configuration or start with a new checkpoint after deciding how to handle the " +
          "existing target data")
    }
  }

  private def write(
      fileSystem: FileSystem,
      root: Path,
      contractPath: Path,
      contract: String): Unit = {
    val document = compact(render(JObject(
      "version" -> JInt(Version),
      "contract" -> JString(contract),
      "createdAt" -> JString(Instant.now.toString))))
    if (!fileSystem.mkdirs(root) && !fileSystem.exists(root)) {
      throw new PuddingException(s"can not create checkpoint directory '$root'")
    }
    var stream: org.apache.hadoop.fs.FSDataOutputStream = null
    try {
      stream = fileSystem.create(contractPath, false)
      stream.write(document.getBytes(StandardCharsets.UTF_8))
      stream.write('\n')
    } catch {
      case _: FileAlreadyExistsException =>
        // Another writer created the contract first. Its contract must agree with this job.
        if (stream != null) {
          stream.close()
          stream = null
        }
        val stored = this.read(fileSystem, contractPath)
        this.requireCompatible(root.toString, contractPath, stored, contract)
        return
      case NonFatal(error) =>
        if (stream != null) {
          try {
            stream.close()
          } catch {
            case NonFatal(_) => // Best effort cleanup below.
          }
          stream = null
        }
        try {
          fileSystem.delete(contractPath, false)
        } catch {
          case NonFatal(_) => // The original error is more useful.
        }
        throw new PuddingException(
          s"failed to write state contract file '$contractPath'", error)
    } finally {
      if (stream != null) {
        stream.close()
      }
    }
    logInfo(s"wrote streaming state contract at $contractPath")
  }

  private def read(
      fileSystem: FileSystem,
      path: Path): StateContractDocument = {
    val input = fileSystem.open(path)
    val output = new java.io.ByteArrayOutputStream()
    try {
      IOUtils.copyBytes(input, output, 4096, false)
    } finally {
      input.close()
    }
    val text = new String(output.toByteArray, StandardCharsets.UTF_8)
    val json = try {
      parse(text)
    } catch {
      case NonFatal(_) =>
        throw new PuddingException(
          s"state contract file '$path' is not valid JSON")
    }
    val version = (json \ "version") match {
      case JInt(value) if value.isValidInt && value.toInt > 0 => value.toInt
      case _ =>
        throw new PuddingException(
          s"state contract file '$path' has a missing or invalid version")
    }
    val contract = (json \ "contract") match {
      case JString(value) if value.trim.nonEmpty => value.trim
      case _ =>
        throw new PuddingException(
          s"state contract file '$path' has a missing or invalid contract")
    }
    StateContractDocument(version, contract)
  }

  private def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => f"${byte & 0xff}%02x").mkString
  }

  private case class StateContractDocument(version: Int, contract: String)
}
