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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{
  MicroBatchStream,
  Offset,
  ReadAllAvailable,
  ReadLimit,
  SupportsAdmissionControl
}
import org.apache.spark.unsafe.types.UTF8String

import org.pudding.core.PuddingException

/**
 * Spark 3.4 micro-batch stream backed by a driver-side CDC poller.
 *
 * Each Spark partition is one ordered slice of JSON rows. The data source emits a single raw
 * string column; the Pudding adapter turns it into the user-declared typed columns using
 * `from_json`. Keeping the Spark V2 layer this small avoids shipping connector classes to
 * executors and keeps the checkpoint schema stable.
 */
private[cdc] final class CdcMicroBatchStream(
    config: CdcConfig,
    checkpointLocation: String)
    extends MicroBatchStream with SupportsAdmissionControl {

  private var startOffset: CdcOffset = null

  private var availableOffset: CdcOffset = null

  private var buffer: CdcRecordBuffer = null

  private var poller: CdcPoller = null

  private var started = false

  private var stopped = false

  override def initialOffset(): Offset = this.synchronized {
    if (startOffset == null) {
      startOffset = CdcOffset.initial()
    }
    startOffset
  }

  override def deserializeOffset(json: String): Offset = this.synchronized {
    val offset = CdcOffset.parse(json)
    this.validateKind(offset)
    if (!started) {
      startOffset = offset
    }
    availableOffset = offset
    offset
  }

  override def latestOffset(): Offset = {
    val after = Option(availableOffset)
      .orElse(Option(startOffset))
      .getOrElse(CdcOffset.initial())
    this.latestOffset(after, ReadLimit.maxRows(config.maxRecordsPerBatch.toLong))
  }

  /**
   * Spark calls this overload when a stream implements SupportsAdmissionControl. The start
   * offset is the last available or committed checkpoint offset; using it (rather than a
   * stream-local commit marker) keeps micro-batch construction correct because Spark commits
   * source offsets one batch behind.
   */
  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = this.synchronized {
    // once/available-now ask for all currently available data immediately after construction,
    // before the driver-side poller has produced its first record. Returning the start offset
    // would make Spark complete a successful zero-row query (and delete-on-success would then
    // remove the checkpoint). Native CDC needs a continuous trigger.
    if (readLimit.isInstanceOf[ReadAllAvailable]) {
      throw new PuddingException(
        s"${config.label} requires a continuous trigger; once and available-now are not " +
          "supported because the source has no bounded end offset")
    }
    this.ensureStarted()
    val after = this.asCdcOffset(start, "start")
    availableOffset = after
    buffer.latestOffset(after)
  }

  override def getDefaultReadLimit(): ReadLimit = {
    ReadLimit.maxRows(config.maxRecordsPerBatch.toLong)
  }

  override def reportLatestOffset(): Offset = this.synchronized {
    if (!started) {
      Option(availableOffset).orElse(Option(startOffset)).getOrElse(CdcOffset.initial())
    } else {
      buffer.latestAvailable(Option(availableOffset).getOrElse(buffer.committedOffset))
    }
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val currentBuffer = this.synchronized {
      this.ensureStarted()
      buffer
    }
    val startOffset = this.asCdcOffset(start, "start")
    val endOffset = this.asCdcOffset(end, "end")
    // The buffer may wait for the poller to reach a checkpointed end offset. Do not hold the
    // stream monitor while waiting, or stop() could not close the buffer.
    val rows = currentBuffer.plan(startOffset, endOffset)
    Array[InputPartition](CdcInputPartition(rows))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new CdcPartitionReaderFactory()
  }

  override def commit(end: Offset): Unit = {
    this.synchronized {
      if (started) {
        val offset = this.asCdcOffset(end, "commit")
        buffer.commit(offset)
        poller.commit(offset)
      }
    }
  }

  override def stop(): Unit = {
    val (currentBuffer, currentPoller) = this.synchronized {
      if (!started || stopped) {
        (null, null)
      } else {
        stopped = true
        val result = buffer -> poller
        (result._1, result._2)
      }
    }
    if (currentBuffer != null) {
      currentBuffer.close()
    }
    if (currentPoller != null) {
      currentPoller.stop()
    } else {
      config.hadoopToken.foreach(HadoopDatabaseHistory.releaseToken)
    }
    this.synchronized {
      buffer = null
      poller = null
      started = false
    }
  }

  private def ensureStarted(): Unit = {
    if (stopped) {
      throw new PuddingException(s"${config.label} stream has been stopped")
    }
    if (!started) {
      // Spark may report the offset-log offset before the corresponding commit has advanced.
      // Starting from the committed cursor lets Spark replay the uncommitted batch from data
      // that is still available at the source.
      val offset = CheckpointOffsetReader.committed(
        this.checkpointLocation, config.hadoopToken)
        .getOrElse(CdcOffset.initial())
      this.validateKind(offset)
      startOffset = offset
      val nextBuffer = new CdcRecordBuffer(
        config.label, config.queueCapacity, config.maxRecordsPerBatch, offset)
      val nextPoller = config match {
        case value: DebeziumConfig =>
          new DebeziumCdcPoller(value, nextBuffer, offset, this.checkpointLocation)
        case value: MongodbConfig => new MongodbCdcPoller(value, nextBuffer, offset)
        case other =>
          throw new PuddingException(
            s"unsupported CDC source configuration: ${other.getClass.getSimpleName}")
      }
      nextPoller.start()
      buffer = nextBuffer
      poller = nextPoller
      started = true
    }
  }

  private def validateKind(offset: CdcOffset): Unit = {
    val valid = config match {
      case _: DebeziumConfig =>
        offset.kind == CdcOffset.InitialKind || offset.kind == CdcOffset.DebeziumKind
      case _: MongodbConfig =>
        offset.kind == CdcOffset.InitialKind || offset.kind == CdcOffset.MongodbKind
    }
    if (!valid) {
      throw new PuddingException(
        s"${config.label} checkpoint belongs to another CDC source type")
    }
  }

  private def asCdcOffset(value: Offset, role: String): CdcOffset = {
    value match {
      case offset: CdcOffset =>
        this.validateKind(offset)
        offset
      case _ =>
        throw new PuddingException(
          s"${config.label} received an unsupported $role offset")
    }
  }
}

private[cdc] final case class CdcInputPartition(rows: Array[String])
    extends InputPartition {

  override def preferredLocations(): Array[String] = Array.empty
}

private[cdc] final class CdcPartitionReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case value: CdcInputPartition => new CdcPartitionReader(value.rows)
      case other =>
        throw new IllegalArgumentException(
          s"unsupported CDC input partition: ${other.getClass.getSimpleName}")
    }
  }
}

private[cdc] final class CdcPartitionReader(rows: Array[String])
    extends PartitionReader[InternalRow] {

  private var index = -1

  override def next(): Boolean = {
    index += 1
    index < rows.length
  }

  override def get(): InternalRow = {
    new GenericInternalRow(Array[Any](UTF8String.fromString(rows(index))))
  }

  override def close(): Unit = ()
}
