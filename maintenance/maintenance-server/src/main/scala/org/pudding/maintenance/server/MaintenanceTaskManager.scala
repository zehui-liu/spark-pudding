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

package org.pudding.maintenance.server

import java.time.Instant
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors, Future, ThreadFactory, TimeUnit}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

import org.pudding.maintenance.api._

/**
 * Single-worker task manager for the HTTP control plane.
 *
 * Spark maintenance sessions are heavy and a server should not run concurrent catalog DDL/DML
 * for unrelated requests by default. Tasks are durable, idempotency-keyed and audited. A queued
 * task can be cancelled; a running Spark action is not forcibly interrupted because stopping a
 * Spark session in the middle of a table commit is not a safe cancellation primitive.
 */
private[server] class MaintenanceTaskManager(
    store: MaintenanceTaskStore,
    audit: MaintenanceAuditLog,
    executor: MaintenanceExecutor,
    maxTasks: Int = 1000) extends AutoCloseable with Logging {

  private val tasks = mutable.LinkedHashMap[String, MaintenanceTask]()

  private val idempotencyKeys = mutable.Map[String, String]()

  private val futures = mutable.Map[String, Future[_]]()

  private val lock = new Object

  private val worker: ExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable, "pudding-maintenance-worker")
      thread.setDaemon(true)
      thread
    }
  })

  this.recover()

  def get(id: String): Option[MaintenanceTask] = lock.synchronized {
    tasks.get(id)
  }

  def list(status: Option[String], limit: Int): Seq[MaintenanceTask] = lock.synchronized {
    val normalizedStatus = status.map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .filter(_.nonEmpty)
    val filtered = tasks.values.filter { task =>
      normalizedStatus.forall(_ == task.status)
    }.toSeq
    filtered.sortBy(task => task.createdAt)(Ordering[String].reverse)
      .take(math.max(1, math.min(limit, 1000)))
  }

  def counts: Map[String, Int] = lock.synchronized {
    MaintenanceTaskStatus.All.toSeq.sorted.map { status =>
      status -> tasks.values.count(_.status == status)
    }.toMap
  }

  def submit(
      request: MaintenanceRequest,
      idempotencyKey: Option[String]): MaintenanceTask = lock.synchronized {
    val normalized = MaintenanceValidation.validate(request)
    val key = idempotencyKey.map(_.trim).filter(_.nonEmpty)
    val existing = key.flatMap(idempotencyKeys.get).flatMap(tasks.get)
    existing.getOrElse {
      this.makeRoom()
      val id = UUID.randomUUID().toString
      val now = Instant.now().toString
      val task = MaintenanceTask(
        id = id,
        idempotencyKey = key,
        request = normalized,
        status = MaintenanceTaskStatus.Queued,
        createdAt = now)
      tasks += id -> task
      key.foreach(idempotencyKeys += _ -> id)
      this.persist()
      this.record("submitted", task)
      try {
        futures += id -> worker.submit(new Runnable {
          override def run(): Unit = MaintenanceTaskManager.this.runTask(id)
        })
      } catch {
        case NonFatal(error) =>
          this.fail(id, s"failed to schedule task: ${error.getClass.getSimpleName}")
      }
      tasks(id)
    }
  }

  /**
   * Cancel a queued task. A running Spark action is not interrupted; the caller must wait for it
   * to finish or stop the server process.
   */
  def cancel(id: String): Either[String, MaintenanceTask] = lock.synchronized {
    tasks.get(id) match {
      case None => Left("task not found")
      case Some(task) if task.status == MaintenanceTaskStatus.Queued =>
        val cancelled = futures.get(id).forall(_.cancel(false))
        if (!cancelled) {
          Left("task is already running and cannot be cancelled safely")
        } else {
          futures -= id
          val updated = task.copy(
            status = MaintenanceTaskStatus.Cancelled,
            finishedAt = Some(Instant.now().toString))
          tasks += id -> updated
          this.persist()
          this.record("cancelled", updated)
          Right(updated)
        }
      case Some(task) if task.status == MaintenanceTaskStatus.Running =>
        Left("task is running and cannot be cancelled safely")
      case Some(_) => Left("task is already finished")
    }
  }

  override def close(): Unit = {
    worker.shutdownNow()
    try {
      worker.awaitTermination(5, TimeUnit.SECONDS)
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
    executor.close()
  }

  private def runTask(id: String): Unit = {
    val task = lock.synchronized {
      tasks.get(id).filter(_.status == MaintenanceTaskStatus.Queued).map { value =>
        val running = value.copy(
          status = MaintenanceTaskStatus.Running,
          startedAt = Some(Instant.now().toString))
        tasks += id -> running
        this.persist()
        this.record("started", running)
        running
      }
    }
    task.foreach { running =>
      try {
        val result = executor.execute(running.request)
        this.complete(id, result)
      } catch {
        case error: MaintenanceException =>
          this.fail(id, error.getMessage)
        case NonFatal(error) =>
          this.fail(
            id,
            Option(error.getMessage).map(_.trim).filter(_.nonEmpty)
              .getOrElse(error.getClass.getSimpleName))
      }
    }
  }

  private def complete(id: String, result: MaintenanceResult): Unit = lock.synchronized {
    tasks.get(id).foreach { task =>
      val updated = task.copy(
        status = MaintenanceTaskStatus.Succeeded,
        finishedAt = Some(Instant.now().toString),
        result = Some(result))
      tasks += id -> updated
      this.persist()
      this.record("succeeded", updated)
    }
  }

  private def fail(id: String, message: String): Unit = lock.synchronized {
    tasks.get(id).foreach { task =>
      val updated = task.copy(
        status = MaintenanceTaskStatus.Failed,
        finishedAt = Some(Instant.now().toString),
        error = Some(this.truncate(message)))
      tasks += id -> updated
      this.persist()
      this.record("failed", updated)
    }
  }

  private def recover(): Unit = {
    val loaded = store.load()
    val now = Instant.now().toString
    var recovered = false
    loaded.foreach { task =>
      val value = if (task.status == MaintenanceTaskStatus.Queued ||
        task.status == MaintenanceTaskStatus.Running) {
        recovered = true
        task.copy(
          status = MaintenanceTaskStatus.Failed,
          finishedAt = Some(now),
          error = Some("server restarted before the task completed"))
      } else {
        task
      }
      tasks += value.id -> value
      value.idempotencyKey.foreach(idempotencyKeys += _ -> value.id)
    }
    if (recovered) {
      this.persist()
      loaded.filter(task =>
        task.status == MaintenanceTaskStatus.Queued ||
          task.status == MaintenanceTaskStatus.Running).foreach { task =>
        this.record("recovered-as-failed", tasks(task.id))
      }
    }
  }

  private def makeRoom(): Unit = {
    if (tasks.size >= maxTasks) {
      val finished = tasks.values.filter(task =>
        task.status != MaintenanceTaskStatus.Queued &&
          task.status != MaintenanceTaskStatus.Running)
        .toSeq.sortBy(_.createdAt)
      val toRemove = (tasks.size - maxTasks + 1).min(finished.size)
      finished.take(toRemove).foreach { task =>
        tasks -= task.id
        futures -= task.id
        task.idempotencyKey.foreach(idempotencyKeys -= _)
      }
      if (tasks.size >= maxTasks) {
        throw new MaintenanceUsageException(
          s"maintenance task limit reached ($maxTasks); wait for or cancel queued tasks")
      }
    }
  }

  private def persist(): Unit = {
    try {
      store.save(tasks.values.toSeq)
    } catch {
      case error: MaintenanceException => throw error
    }
  }

  private def record(event: String, task: MaintenanceTask): Unit = {
    val target = MaintenanceValidation.target(task.request)
    try {
      audit.append(Map(
        "event" -> event,
        "taskId" -> task.id,
        "format" -> task.request.format,
        "action" -> task.request.action,
        "target" -> target,
        "status" -> task.status,
        "at" -> Instant.now().toString))
    } catch {
      case NonFatal(error) =>
        logWarning(
          s"failed to append maintenance audit event '$event' for task '${task.id}': " +
            error.getClass.getSimpleName)
    }
  }

  private def truncate(value: String): String = {
    val text = Option(value).getOrElse("maintenance task failed").trim
    if (text.length <= 1000) text else text.substring(0, 1000) + "..."
  }
}
