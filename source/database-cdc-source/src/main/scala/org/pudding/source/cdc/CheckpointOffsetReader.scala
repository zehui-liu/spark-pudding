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

package org.pudding.source.cdc

import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.pudding.core.PuddingException

/**
 * Read the last Spark-committed source offset from a streaming checkpoint.
 *
 * Spark commits source offsets one batch behind the offset log. On restart, an offset-log-only
 * offset may be ahead of the batch Spark will replay; starting a driver poller at that newer
 * cursor would make the replay range unavailable. The commit log is the authoritative cursor for
 * the restart, so the poller starts there and lets Spark replay the uncommitted batch.
 *
 * This is read-only: the Spark checkpoint remains the source of truth.
 */
private[cdc] object CheckpointOffsetReader {

  def committed(
      checkpointLocation: String,
      hadoopToken: Option[String]): Option[CdcOffset] = {
    if (checkpointLocation == null || checkpointLocation.trim.isEmpty) {
      None
    } else {
      val configuration = hadoopToken
        .flatMap(HadoopDatabaseHistory.configuration)
        .getOrElse(new HadoopConfiguration())
      val checkpointPath = new Path(checkpointLocation)
      val fileSystem = checkpointPath.getFileSystem(configuration)
      val root = this.checkpointRoot(fileSystem, checkpointPath).getOrElse {
        return None
      }
      val commits = new Path(root, "commits")
      if (!fileSystem.exists(commits)) {
        return None
      }
      val latestBatch = fileSystem.listStatus(commits).flatMap { status =>
        try Some(status.getPath.getName.toLong) catch {
          case _: NumberFormatException => None
        }
      }.sorted.lastOption
      if (latestBatch.isEmpty) {
        return None
      }
      val batchId = latestBatch.get
      val offsetFile = new Path(new Path(root, "offsets"), batchId.toString)
      if (!fileSystem.exists(offsetFile)) {
        throw new PuddingException(
          s"committed CDC checkpoint batch $batchId has no offset file: $offsetFile")
      }
      val text = this.read(fileSystem, offsetFile)
      val lines = text.split("\\r?\\n", -1).drop(2)
      val selected = this.sourceIndex(checkpointPath) match {
        case Some(index) =>
          lines.lift(index).getOrElse {
            throw new PuddingException(
              s"CDC checkpoint batch $latestBatch has no offset for source index $index")
          }
        case None =>
          lines.find(line => line.trim.nonEmpty && line.trim != "-").getOrElse("-")
      }
      val value = selected.trim
      if (value.isEmpty || value == "-") {
        None
      } else {
        try {
          Some(CdcOffset.parse(value))
        } catch {
          case NonFatal(error) =>
            throw new PuddingException(
              s"failed to parse the committed CDC offset in $offsetFile", error)
        }
      }
    }
  }

  /**
   * Spark passes `<checkpoint>/sources/<id>` to each V2 stream. The offset file contains one
   * line per source in plan order, so a multi-stream checkpoint must select this source's line
   * instead of the first parseable line.
   */
  private def sourceIndex(checkpointPath: Path): Option[Int] = {
    val parent = checkpointPath.getParent
    if (parent != null && parent.getName == "sources") {
      try Some(checkpointPath.getName.toInt) catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  }

  private def checkpointRoot(
      fileSystem: FileSystem,
      start: Path): Option[Path] = {
    var candidate: Path = start
    var depth = 0
    while (candidate != null && depth < 4) {
      val commits = new Path(candidate, "commits")
      val offsets = new Path(candidate, "offsets")
      if (fileSystem.exists(commits) || fileSystem.exists(offsets)) {
        return Some(candidate)
      }
      candidate = candidate.getParent
      depth += 1
    }
    None
  }

  private def read(fileSystem: FileSystem, path: Path): String = {
    val input = fileSystem.open(path)
    try {
      val output = new java.io.ByteArrayOutputStream()
      val buffer = new Array[Byte](8192)
      var count = input.read(buffer)
      while (count >= 0) {
        if (count > 0) {
          output.write(buffer, 0, count)
        }
        count = input.read(buffer)
      }
      new String(output.toByteArray, StandardCharsets.UTF_8)
    } catch {
      case NonFatal(error) =>
        throw new PuddingException(
          s"failed to read CDC checkpoint offset at ${path.toString}: " +
            error.getClass.getSimpleName)
    } finally {
      input.close()
    }
  }
}
