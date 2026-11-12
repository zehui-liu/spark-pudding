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

package org.pudding.maintenance.server

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path, StandardCopyOption, StandardOpenOption}
import java.util

import scala.util.control.NonFatal

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.pudding.maintenance.api.MaintenanceException

/**
 * Small durable task store. The file contains only maintenance requests, results, task ids and
 * timestamps; it never contains Spark configuration, catalog credentials or auth tokens.
 */
private[server] class MaintenanceTaskStore(path: Path) {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def load(): Seq[MaintenanceTask] = {
    if (path == null || !Files.isRegularFile(path)) {
      Seq.empty
    } else {
      try {
        val text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
        if (text.trim.isEmpty) Seq.empty
        else Serialization.read[Seq[MaintenanceTask]](text)
      } catch {
        case NonFatal(error) =>
          throw new MaintenanceException(
            s"failed to read maintenance task state '$path': ${error.getClass.getSimpleName}",
            cause = error)
      }
    }
  }

  def save(tasks: Seq[MaintenanceTask]): Unit = {
    val parent = path.toAbsolutePath.getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }
    val directory = Option(parent).getOrElse(path.toAbsolutePath.getParent)
    val temporary = Files.createTempFile(
      directory,
      s".${path.getFileName}.",
      ".tmp")
    val text = Serialization.writePretty(tasks)
    try {
      Files.write(
        temporary,
        text.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING)
      setOwnerOnly(temporary)
      try {
        Files.move(
          temporary,
          path,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE)
      } catch {
        case _: AtomicMoveNotSupportedException =>
          Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING)
      }
      setOwnerOnly(path)
    } catch {
      case NonFatal(error) =>
        try Files.deleteIfExists(temporary) catch {
          case NonFatal(_) => ()
        }
        throw new MaintenanceException(
          s"failed to write maintenance task state '$path': ${error.getClass.getSimpleName}",
          cause = error)
    }
  }

  private def setOwnerOnly(path: Path): Unit = {
    try {
      Files.setPosixFilePermissions(
        path,
        util.EnumSet.of(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE))
    } catch {
      case _: UnsupportedOperationException => ()
      case NonFatal(_) => ()
    }
  }
}

/**
 * Append-only JSON-lines audit log. The auth token, Spark configuration and request options are
 * deliberately not recorded.
 */
private[server] class MaintenanceAuditLog(path: Path) {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def append(event: Map[String, String]): Unit = {
    val parent = path.toAbsolutePath.getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }
    val line = Serialization.write(event) + "\n"
    try {
      Files.write(
        path,
        line.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND)
      try {
        Files.setPosixFilePermissions(
          path,
          util.EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE))
      } catch {
        case NonFatal(_) => ()
      }
    } catch {
      case NonFatal(error) =>
        throw new MaintenanceException(
          s"failed to append maintenance audit log '$path': " +
            error.getClass.getSimpleName,
          cause = error)
    }
  }
}
