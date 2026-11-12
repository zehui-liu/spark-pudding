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

package org.pudding.core.capability

import java.nio.file.Paths

import org.pudding.core.PuddingException

/**
 * Command line entry point for capability manifest verification and inspection.
 *
 * Usage:
 *  - `verify <distribution-dir>` loads the root manifest, merges jar fragments and verifies jars;
 *  - `show <distribution-dir>` prints the merged manifest as YAML;
 *  - `diff <previous-distribution> <current-distribution>` prints capability changes.
 */
// scalastyle:off println
object CapabilityTool {

  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 2) {
      throw new PuddingException(
        "usage: CapabilityTool verify|show <distribution-dir> | " +
          "CapabilityTool diff <previous-distribution> <current-distribution>")
    }

    args(0) match {
      case "verify" => this.verify(args(1))
      case "show" => this.show(args(1))
      case "diff" =>
        if (args.length < 3) {
          throw new PuddingException(
            "usage: CapabilityTool diff <previous-distribution> <current-distribution>")
        }
        this.diff(args(1), args(2))
      case other =>
        throw new PuddingException(s"unknown capability command: $other")
    }
  }

  private def verify(distribution: String): Unit = {
    val manifest = CapabilityManifest.verifyDistribution(Paths.get(distribution))
    println(
      s"capability manifest verified: schema ${manifest.schemaVersion}, " +
        s"${manifest.components.size} components, hash ${manifest.capabilitySetHash}")
    this.printCounts(manifest)
  }

  private def show(distribution: String): Unit = {
    val manifest = CapabilityManifest.load(Paths.get(distribution))
    print(CapabilityManifestYaml.render(manifest))
  }

  private def diff(previousDistribution: String, currentDistribution: String): Unit = {
    val previous = CapabilityManifest.load(Paths.get(previousDistribution))
    val current = CapabilityManifest.load(Paths.get(currentDistribution))
    val diff = CapabilityManifest.diff(previous, current)

    this.printKeys("added", diff.added.map(_.key))
    this.printKeys("removed", diff.removed.map(_.key))
    this.printKeys("changed", diff.changed.map(change => change.after.key))
    if (diff.isEmpty) {
      println("no capability changes")
    }
  }

  private def printKeys(label: String, keys: Seq[String]): Unit = {
    if (keys.nonEmpty) {
      println(s"$label:")
      keys.sorted.foreach(key => println(s"  - $key"))
    }
  }

  private def printCounts(manifest: CapabilityManifest): Unit = {
    CapabilityKind.All.toSeq.sorted.foreach { kind =>
      println(s"  $kind: ${manifest.components.count(_.kind == kind)}")
    }
  }
}
// scalastyle:on println
