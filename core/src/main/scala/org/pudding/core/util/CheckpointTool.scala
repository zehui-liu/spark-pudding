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

import java.io.{File, FileInputStream, PrintStream}
import java.time.Instant
import java.util.{Locale, Properties}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.pudding.core.PuddingException

/**
 * Inspect and remove Spark Structured Streaming checkpoint directories without starting a job.
 *
 * Usage:
 *  - `show <location>` prints existence, checkpoint shape, size and newest modification time;
 *  - `status <location> [--json]` prints the same data plus age and commit/offset counts;
 *  - `delete <location> [--dry-run]` removes one checkpoint;
 *  - `cleanup <root> --older-than <duration> [--keep-last N] [--dry-run]` removes checkpoint
 *    subdirectories that are older than the threshold or beyond the newest N.
 *
 * `cleanup` only scans direct directory children of a parent location. It refuses a root that
 * is itself a checkpoint, and only an operator should run it after stopping writers.
 */
// scalastyle:off println
object CheckpointTool {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private val Usage =
    """Usage:
      |  pudding checkpoint show <location> [--json] [--conf-file <file>] [--hadoop-conf <dir>]
      |  pudding checkpoint status <location> [--json] [--conf-file <file>] [--hadoop-conf <dir>]
      |  pudding checkpoint delete <location> [--dry-run] [--conf-file <file>] [--hadoop-conf <dir>]
      |  pudding checkpoint cleanup <root> --older-than <duration> [--keep-last N] [--dry-run]
      |    [--conf-file <file>] [--hadoop-conf <dir>]
      |  pudding checkpoint --help
      |
      |<duration> is a positive number with a unit: ms, s, m, h or d (for example 7d).
      |`cleanup` scans only direct directory children of <root>. The root must be the parent
      |of the checkpoint directories, not a checkpoint itself. Stop every writer before
      |deleting a checkpoint; deleting it makes the next job start from the source's
      |configured starting position. --conf-file accepts fs.* keys; --hadoop-conf adds
      |the XML files in a Hadoop configuration directory.
      |""".stripMargin

  private final class UsageException(message: String) extends RuntimeException(message)

  private final case class CommandArgs(
      values: Seq[String],
      dryRun: Boolean,
      olderThan: Option[Long],
      keepLast: Option[Int],
      json: Boolean,
      configuration: Configuration)

  def main(args: Array[String]): Unit = {
    System.exit(this.run(args, System.out, System.err))
  }

  def run(args: Array[String], out: PrintStream, err: PrintStream): Int = {
    try {
      args.toList match {
        case Nil | List("-h") | List("--help") =>
          out.println(Usage)
          0
        case "show" :: rest =>
          this.show(
            this.parse(
              rest, allowDryRun = false, allowOlderThan = false, allowKeepLast = false),
            out)
          0
        case "status" :: rest =>
          this.show(
            this.parse(
              rest, allowDryRun = false, allowOlderThan = false, allowKeepLast = false),
            out)
          0
        case "delete" :: rest =>
          this.delete(
            this.parse(
              rest, allowDryRun = true, allowOlderThan = false, allowKeepLast = false),
            out)
          0
        case "cleanup" :: rest =>
          this.cleanup(
            this.parse(
              rest, allowDryRun = true, allowOlderThan = true, allowKeepLast = true),
            out)
          0
        case _ =>
          throw new UsageException("unknown command")
      }
    } catch {
      case error: UsageException =>
        err.println(s"pudding checkpoint: ${error.getMessage}")
        err.println(Usage)
        2
      case error: PuddingException =>
        err.println(s"pudding checkpoint: ${error.getMessage}")
        1
      case NonFatal(error) =>
        val detail = Option(error.getMessage).filter(_.nonEmpty)
          .getOrElse(error.getClass.getSimpleName)
        err.println(s"pudding checkpoint: $detail")
        1
    }
  }

  private def parse(
      args: List[String],
      allowDryRun: Boolean,
      allowOlderThan: Boolean,
      allowKeepLast: Boolean): CommandArgs = {
    var values = Seq.empty[String]
    var dryRun = false
    var olderThan: Option[Long] = None
    var keepLast: Option[Int] = None
    var json = false
    var confFile: Option[String] = None
    var hadoopConf: Option[String] = None
    var remaining = args
    while (remaining.nonEmpty) {
      remaining.head match {
        case "--dry-run" if allowDryRun =>
          dryRun = true
          remaining = remaining.tail
        case "--json" =>
          json = true
          remaining = remaining.tail
        case "--older-than" if allowOlderThan =>
          remaining.tail match {
            case value :: tail =>
              olderThan = Some(this.parseDuration(value))
              remaining = tail
            case Nil =>
              throw new UsageException("--older-than requires a duration")
          }
        case option if option.startsWith("--older-than=") && allowOlderThan =>
          olderThan = Some(this.parseDuration(option.substring("--older-than=".length)))
          remaining = remaining.tail
        case "--keep-last" if allowKeepLast =>
          remaining.tail match {
            case value :: tail =>
              keepLast = Some(this.parsePositiveInt(value, "--keep-last"))
              remaining = tail
            case Nil =>
              throw new UsageException("--keep-last requires a count")
          }
        case option if option.startsWith("--keep-last=") && allowKeepLast =>
          keepLast = Some(this.parsePositiveInt(
            option.substring("--keep-last=".length), "--keep-last"))
          remaining = remaining.tail
        case "--conf-file" =>
          remaining.tail match {
            case value :: tail =>
              confFile = Some(value)
              remaining = tail
            case Nil =>
              throw new UsageException("--conf-file requires a path")
          }
        case option if option.startsWith("--conf-file=") =>
          confFile = Some(option.substring("--conf-file=".length))
          remaining = remaining.tail
        case "--hadoop-conf" =>
          remaining.tail match {
            case value :: tail =>
              hadoopConf = Some(value)
              remaining = tail
            case Nil =>
              throw new UsageException("--hadoop-conf requires a directory")
          }
        case option if option.startsWith("--hadoop-conf=") =>
          hadoopConf = Some(option.substring("--hadoop-conf=".length))
          remaining = remaining.tail
        case option if option.startsWith("-") =>
          throw new UsageException(s"unknown option: $option")
        case value =>
          values :+= value
          remaining = remaining.tail
      }
    }
    if (values.size != 1) {
      throw new UsageException("expected exactly one location")
    }
    if (allowOlderThan && olderThan.isEmpty && keepLast.isEmpty) {
      throw new UsageException("cleanup requires --older-than or --keep-last")
    }
    CommandArgs(
      values, dryRun, olderThan, keepLast, json,
      this.configuration(confFile, hadoopConf))
  }

  private def configuration(
      confFile: Option[String],
      hadoopConf: Option[String]): Configuration = {
    val conf = new Configuration()
    hadoopConf.foreach { value =>
      val directory = new File(value)
      if (!directory.isDirectory) {
        throw new UsageException(s"--hadoop-conf is not a directory: $value")
      }
      Option(directory.listFiles()).getOrElse(Array.empty[File])
        .filter(_.getName.endsWith(".xml"))
        .sortBy(_.getName)
        .foreach(file => conf.addResource(file.toURI.toURL))
    }
    confFile.foreach { value =>
      val properties = new Properties()
      val input = new FileInputStream(value)
      try {
        properties.load(input)
      } catch {
        case NonFatal(error) =>
          throw new UsageException(
            s"failed to read --conf-file $value: ${error.getClass.getSimpleName}")
      } finally {
        input.close()
      }
      properties.stringPropertyNames().asScala.toSeq.sorted.foreach { key =>
        if (!key.startsWith("fs.")) {
          throw new UsageException(
            s"--conf-file only accepts fs.* keys, found: $key")
        }
        conf.set(key, properties.getProperty(key))
      }
    }
    conf
  }

  private def show(args: CommandArgs, out: PrintStream): Unit = {
    val location = args.values.head
    val path = new Path(location)
    val fileSystem = path.getFileSystem(args.configuration)
    if (!fileSystem.exists(path)) {
      throw new PuddingException(s"checkpoint location does not exist: $location")
    }
    val status = fileSystem.getFileStatus(path)
    val summary = fileSystem.getContentSummary(path)
    val modified = CheckpointFiles.lastModified(fileSystem, path)
    val commitNames = this.childNames(fileSystem, path, "commits")
    val offsetNames = this.childNames(fileSystem, path, "offsets")
    val details = Map[String, Any](
      "location" -> location,
      "exists" -> true,
      "type" -> (if (status.isDirectory) "directory" else "file"),
      "checkpoint" -> CheckpointFiles.isCheckpoint(fileSystem, path),
      "lastModified" -> Instant.ofEpochMilli(modified).toString,
      "ageSeconds" -> math.max(0L, (System.currentTimeMillis() - modified) / 1000L),
      "sizeBytes" -> summary.getLength,
      "commits" -> commitNames.size,
      "offsets" -> offsetNames.size,
      "lastCommit" -> this.lastNumericName(commitNames).getOrElse(""),
      "lastOffset" -> this.lastNumericName(offsetNames).getOrElse(""))
    if (args.json) {
      out.println(Serialization.write(details))
    } else {
      details.toSeq.sortBy(_._1).foreach { case (key, value) =>
        out.println(s"$key: $value")
      }
    }
  }

  private def childNames(fileSystem: FileSystem, path: Path, child: String): Seq[String] = {
    val directory = new Path(path, child)
    if (!fileSystem.exists(directory)) {
      Seq.empty
    } else {
      fileSystem.listStatus(directory)
        .filterNot(_.isDirectory)
        .map(_.getPath.getName)
        .toSeq
    }
  }

  private def lastNumericName(names: Seq[String]): Option[String] = {
    names.sortBy(name => Try(name.toLong).getOrElse(Long.MinValue)).lastOption
  }

  private def delete(args: CommandArgs, out: PrintStream): Unit = {
    val location = args.values.head
    val path = new Path(location)
    val fileSystem = path.getFileSystem(args.configuration)
    if (!fileSystem.exists(path)) {
      throw new PuddingException(s"checkpoint location does not exist: $location")
    }
    if (!CheckpointFiles.isCheckpoint(fileSystem, path)) {
      throw new PuddingException(
        s"refusing to delete a location that is not a Spark checkpoint: $location")
    }
    if (args.dryRun) {
      out.println(s"would delete: $location")
    } else {
      CheckpointFiles.deleteCheckpoint(location, args.configuration)
      out.println(s"deleted: $location")
    }
  }

  private def cleanup(args: CommandArgs, out: PrintStream): Unit = {
    val rootText = args.values.head
    val root = new Path(rootText)
    val fileSystem = root.getFileSystem(args.configuration)
    if (!fileSystem.exists(root)) {
      throw new PuddingException(s"checkpoint root does not exist: $rootText")
    }
    if (!fileSystem.getFileStatus(root).isDirectory) {
      throw new PuddingException(s"checkpoint root is not a directory: $rootText")
    }
    val qualifiedRoot = fileSystem.makeQualified(root)
    val rootPath = Option(qualifiedRoot.toUri.getPath).map(_.trim).getOrElse("")
    if (qualifiedRoot.isRoot || rootPath.isEmpty || rootPath == "/") {
      throw new PuddingException(
        s"refusing to use a filesystem root as the cleanup root: $rootText")
    }
    if (CheckpointFiles.isCheckpoint(fileSystem, root)) {
      throw new PuddingException(
        s"cleanup root is itself a checkpoint: $rootText; pass its parent directory")
    }

    val now = System.currentTimeMillis()
    val children = fileSystem.listStatus(root).toSeq
      .filter(_.isDirectory)
      // Only Spark checkpoint directories are deletion candidates. A cleanup root may contain
      // table data, warehouse, archive or other managed directories that must never be removed.
      .filter(status => CheckpointFiles.isCheckpoint(fileSystem, status.getPath))
      .map(status => status -> CheckpointFiles.lastModified(fileSystem, status.getPath))
      .sortBy { case (status, modified) => (-modified, status.getPath.getName) }
    val candidates = args.keepLast.map(count => children.drop(count)).getOrElse(children)
    val expired = candidates.filter { case (_, modified) =>
      args.olderThan.forall(threshold => now - modified >= threshold)
    }
    val kept = children.size - expired.size

    if (expired.isEmpty) {
      out.println(s"no checkpoints selected for cleanup under: $rootText")
    } else {
      expired.foreach { case (status, _) =>
        val location = status.getPath.toString
        if (args.dryRun) {
          out.println(s"would delete: $location")
        } else {
          CheckpointFiles.deleteCheckpoint(location, args.configuration)
          out.println(s"deleted: $location")
        }
      }
    }
    out.println(s"kept: $kept")
  }

  private def parsePositiveInt(value: String, field: String): Int = {
    val parsed = try {
      value.trim.toInt
    } catch {
      case _: NumberFormatException =>
        throw new UsageException(s"$field must be a positive integer")
    }
    if (parsed <= 0) {
      throw new UsageException(s"$field must be greater than zero")
    }
    parsed
  }

  private def parseDuration(value: String): Long = {
    val text = Option(value).map(_.trim).getOrElse("")
    val matchResult = "(?i)^([0-9]+)(ms|s|m|h|d)$".r.findFirstMatchIn(text)
    val (amount, unit) = matchResult match {
      case Some(result) =>
        val parsed = try {
          result.group(1).toLong
        } catch {
          case _: NumberFormatException =>
            throw new UsageException("duration is too large")
        }
        parsed -> result.group(2).toLowerCase(Locale.ROOT)
      case None =>
        throw new UsageException(s"invalid duration: $value; use ms, s, m, h or d")
    }
    if (amount <= 0L) {
      throw new UsageException("duration must be greater than zero")
    }
    val multiplier = unit match {
      case "ms" => 1L
      case "s" => 1000L
      case "m" => 60L * 1000L
      case "h" => 60L * 60L * 1000L
      case "d" => 24L * 60L * 60L * 1000L
    }
    try {
      Math.multiplyExact(amount, multiplier)
    } catch {
      case _: ArithmeticException =>
        throw new UsageException("duration is too large")
    }
  }
}
// scalastyle:on println
