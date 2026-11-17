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
import java.nio.file.{Files, Path}

import scala.util.Try

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CheckpointOffsetReaderTest extends AnyFunSuite with Matchers {

  test("read the latest committed source offset from Spark checkpoint logs") {
    val root = Files.createTempDirectory("cdc-checkpoint-offset")
    try {
      val sourcePath = Files.createDirectories(root.resolve("sources").resolve("0"))
      this.writeOffset(root, 0L, CdcOffset.debezium(Map("pos" -> 10L), 1L))
      this.writeOffset(root, 1L, CdcOffset.debezium(Map("pos" -> 20L), 2L))
      this.writeCommit(root, 0L)
      this.writeCommit(root, 1L)
      // The offset log can contain an uncommitted next batch; only committed batch 1 counts.
      this.writeOffset(root, 2L, CdcOffset.debezium(Map("pos" -> 30L), 3L))

      CheckpointOffsetReader.committed(sourcePath.toString, None) shouldBe
        Some(CdcOffset.debezium(Map("pos" -> 20L), 2L))
    } finally {
      this.delete(root)
    }
  }

  test("select the offset line for the stream's own source index") {
    val root = Files.createTempDirectory("cdc-checkpoint-multi-source")
    try {
      val first = Files.createDirectories(root.resolve("sources").resolve("0"))
      val second = Files.createDirectories(root.resolve("sources").resolve("1"))
      val firstOffset = CdcOffset.debezium(Map("pos" -> 10L), 1L)
      val secondOffset = CdcOffset.debezium(Map("pos" -> 99L), 2L)
      val offsets = Files.createDirectories(root.resolve("offsets"))
      Files.write(
        offsets.resolve("0"),
        ("v1\n\n" + firstOffset.json() + "\n" + secondOffset.json() + "\n")
          .getBytes(StandardCharsets.UTF_8))
      this.writeCommit(root, 0L)

      CheckpointOffsetReader.committed(first.toString, None) shouldBe Some(firstOffset)
      CheckpointOffsetReader.committed(second.toString, None) shouldBe Some(secondOffset)
    } finally {
      this.delete(root)
    }
  }

  test("no commit log means no committed offset") {
    val root = Files.createTempDirectory("cdc-checkpoint-no-commit")
    try {
      val sourcePath = Files.createDirectories(root.resolve("sources").resolve("0"))
      this.writeOffset(root, 0L, CdcOffset.mongodb(Map("mode" -> "stream"), 1L))
      CheckpointOffsetReader.committed(sourcePath.toString, None) shouldBe None
    } finally {
      this.delete(root)
    }
  }

  private def writeOffset(root: Path, batchId: Long, offset: CdcOffset): Unit = {
    val offsets = Files.createDirectories(root.resolve("offsets"))
    val text = "v1\n\n" + offset.json() + "\n"
    Files.write(
      offsets.resolve(batchId.toString),
      text.getBytes(StandardCharsets.UTF_8))
  }

  private def writeCommit(root: Path, batchId: Long): Unit = {
    val commits = Files.createDirectories(root.resolve("commits"))
    Files.write(commits.resolve(batchId.toString), Array.emptyByteArray)
  }

  private def delete(root: Path): Unit = {
    if (root != null && Files.exists(root)) {
      import scala.collection.JavaConverters._
      val stream = Files.walk(root)
      try {
        stream.iterator().asScala.toVector
          .sortBy(_.getNameCount)(Ordering.Int.reverse)
          .foreach(path => Try(Files.deleteIfExists(path)))
      } finally {
        stream.close()
      }
    }
  }
}
