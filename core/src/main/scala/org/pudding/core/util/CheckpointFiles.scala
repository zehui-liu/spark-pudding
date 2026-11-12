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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.pudding.core.PuddingException

/**
 * Hadoop-only checkpoint filesystem operations shared by the runtime and the maintenance CLI.
 * This object deliberately has no Spark dependency so `bin/pudding checkpoint` can run with only the
 * Hadoop classpath.
 */
object CheckpointFiles {

  private val LocationKey = "checkpointLocation"

  /**
   * Recursively delete one checkpoint directory. The filesystem root and a path without a
   * concrete name are rejected so a mistyped location cannot remove a filesystem. A missing
   * checkpoint is a no-op.
   */
  def deleteCheckpoint(
      location: String,
      configuration: Configuration = new Configuration()): Boolean = {
    val value = Option(location).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new PuddingException(s"$LocationKey can not be empty")
    }
    try {
      val path = new Path(value)
      val fileSystem = path.getFileSystem(configuration)
      val qualified = fileSystem.makeQualified(path)
      val uriPath = Option(qualified.toUri.getPath).map(_.trim).getOrElse("")
      if (qualified.isRoot || uriPath.isEmpty || uriPath == "/") {
        throw new PuddingException(
          s"refusing to delete a filesystem root as $LocationKey")
      }
      if (!fileSystem.exists(qualified)) {
        false
      } else if (!fileSystem.delete(qualified, true)) {
        throw new PuddingException(s"failed to delete $LocationKey at $value")
      } else {
        true
      }
    } catch {
      case error: PuddingException => throw error
      case NonFatal(error) =>
        throw new PuddingException(
          s"failed to delete $LocationKey at $value: ${error.getMessage}", error)
    }
  }

  /**
   * Spark checkpoints contain both `commits` and `offsets` directories.
   */
  def isCheckpoint(fileSystem: FileSystem, path: Path): Boolean = {
    fileSystem.exists(new Path(path, "commits")) &&
      fileSystem.exists(new Path(path, "offsets"))
  }

  /**
   * Return the highest committed batch id from the checkpoint offset log.
   *
   * Spark names offset log entries with the numeric batch id. A missing offsets directory means
   * that the query has not committed a batch yet.
   */
  def latestOffsetBatchId(fileSystem: FileSystem, path: Path): Option[Long] = {
    val offsets = new Path(path, "offsets")
    if (!fileSystem.exists(offsets)) {
      None
    } else {
      fileSystem.listStatus(offsets).iterator
        .filterNot(_.isDirectory)
        .flatMap { status =>
          try Some(status.getPath.getName.toLong)
          catch {
            case _: NumberFormatException => None
          }
        }
        .reduceOption(_ max _)
    }
  }

  /**
   * New micro-batches add files inside `commits` and `offsets`, so a checkpoint root's own
   * modification time does not reliably reflect activity. Return the newest modification time
   * in the whole tree instead; cleanup must not delete a checkpoint with a recent descendant.
   */
  def lastModified(fileSystem: FileSystem, path: Path): Long = {
    def visit(current: Path, newest: Long): Long = {
      val status = fileSystem.getFileStatus(current)
      var latest = newest
      if (status.isDirectory) {
        val children = fileSystem.listStatus(current)
        if (children.isEmpty) {
          // Keep an empty directory's own time so cleanup can still age it out.
          latest = math.max(latest, status.getModificationTime)
        } else {
          children.foreach { child =>
            if (!child.isSymlink) {
              latest = visit(child.getPath, latest)
            }
          }
        }
      } else {
        // Object stores synthesize directory modification times (S3A reports the current
        // time for a directory), so age must come from files only. Commit and offset entries
        // are files, which makes the newest file a reliable activity marker on every backend.
        latest = math.max(latest, status.getModificationTime)
      }
      latest
    }

    visit(path, Long.MinValue)
  }
}
