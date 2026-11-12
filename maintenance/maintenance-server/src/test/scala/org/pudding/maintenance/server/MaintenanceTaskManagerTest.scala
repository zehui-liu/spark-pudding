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

import java.nio.file.{Files, Path}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.pudding.maintenance.api._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaintenanceTaskManagerTest extends AnyFunSuite with Matchers {

  private def request(id: String = "db.orders"): MaintenanceRequest =
    MaintenanceRequest(
      format = MaintenanceActions.Iceberg,
      action = MaintenanceActions.Status,
      catalog = Some("lake"),
      table = Some(id))

  private def manager(
      executor: MaintenanceExecutor,
      workspace: Path): MaintenanceTaskManager = {
    new MaintenanceTaskManager(
      new MaintenanceTaskStore(workspace.resolve("tasks.json")),
      new MaintenanceAuditLog(workspace.resolve("audit.jsonl")),
      executor)
  }

  private def waitUntil(timeoutMs: Long = 5000)(condition: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!condition && System.currentTimeMillis() < deadline) {
      Thread.sleep(20L)
    }
    condition shouldBe true
  }

  test("run a task and deduplicate by idempotency key") {
    val workspace = Files.createTempDirectory("maintenance-manager-success")
    val manager = this.manager(new MaintenanceExecutor {
      override def execute(request: MaintenanceRequest): MaintenanceResult =
        MaintenanceResult(
          format = request.format,
          action = request.action,
          target = "lake.db.orders",
          dryRun = false,
          executed = true,
          statements = Seq("SELECT 1"))
    }, workspace)
    try {
      val first = manager.submit(this.request(), Some("key-1"))
      this.waitUntil() {
        manager.get(first.id).exists(_.status == MaintenanceTaskStatus.Succeeded)
      }
      val second = manager.submit(this.request("db.other"), Some("key-1"))
      second.id shouldBe first.id
      manager.list(None, 100).size shouldBe 1
      manager.get(first.id).flatMap(_.result).exists(_.executed) shouldBe true
    } finally {
      manager.close()
    }
  }

  test("cancel a queued task without interrupting the running task") {
    val workspace = Files.createTempDirectory("maintenance-manager-cancel")
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val manager = this.manager(new MaintenanceExecutor {
      override def execute(request: MaintenanceRequest): MaintenanceResult = {
        entered.countDown()
        release.await(5, TimeUnit.SECONDS)
        MaintenanceResult(
          format = request.format,
          action = request.action,
          target = "lake.db.orders",
          dryRun = false,
          executed = true,
          statements = Seq.empty)
      }
    }, workspace)
    try {
      val running = manager.submit(this.request("db.running"), None)
      entered.await(5, TimeUnit.SECONDS) shouldBe true
      waitUntil()(manager.get(running.id).exists(_.status == MaintenanceTaskStatus.Running))

      val queued = manager.submit(this.request("db.queued"), None)
      manager.get(queued.id).map(_.status) shouldBe Some(MaintenanceTaskStatus.Queued)
      val cancelled = manager.cancel(queued.id)
      cancelled.map(_.status) shouldBe Right(MaintenanceTaskStatus.Cancelled)

      release.countDown()
      waitUntil()(manager.get(running.id).exists(_.status == MaintenanceTaskStatus.Succeeded))
    } finally {
      release.countDown()
      manager.close()
    }
  }

  test("mark a task failed when the executor raises") {
    val workspace = Files.createTempDirectory("maintenance-manager-failure")
    val manager = this.manager(new MaintenanceExecutor {
      override def execute(request: MaintenanceRequest): MaintenanceResult =
        throw new MaintenanceException("maintenance failed")
    }, workspace)
    try {
      val task = manager.submit(this.request(), None)
      waitUntil()(manager.get(task.id).exists(_.status == MaintenanceTaskStatus.Failed))
      manager.get(task.id).flatMap(_.error) shouldBe Some("maintenance failed")
    } finally {
      manager.close()
    }
  }

  test("recover an interrupted running task as failed") {
    val workspace = Files.createTempDirectory("maintenance-manager-recover")
    val store = new MaintenanceTaskStore(workspace.resolve("tasks.json"))
    val task = MaintenanceTask(
      id = "interrupted",
      idempotencyKey = None,
      request = this.request(),
      status = MaintenanceTaskStatus.Running,
      createdAt = java.time.Instant.now().toString)
    store.save(Seq(task))
    val manager = this.manager(new MaintenanceExecutor {
      override def execute(request: MaintenanceRequest): MaintenanceResult =
        MaintenanceResult(
          format = request.format,
          action = request.action,
          target = "lake.db.orders",
          dryRun = false,
          executed = true,
          statements = Seq.empty)
    }, workspace)
    try {
      manager.get("interrupted").map(_.status) shouldBe Some(MaintenanceTaskStatus.Failed)
      manager.get("interrupted").flatMap(_.error).exists(_.contains("restarted")) shouldBe true
    } finally {
      manager.close()
    }
  }
}
