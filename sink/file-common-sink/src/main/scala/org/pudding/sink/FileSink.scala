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

package org.pudding.sink

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.{DataSink, SinkContext}
import org.pudding.core.util.{StreamingCheckpoint, StreamingTrigger}

/**
 * Writes a DataFrame to CSV, text, JSON, Parquet or ORC files in batch or streaming mode.
 */
class FileSink extends DataSink {

  private val PATH_KEY = "path"

  /** One of `csv`, `text`, `json`, `parquet` or `orc`. */
  private val FILE_TYPE_KEY = "fileType"

  private val OPTIONS_KEY = "options"

  /** Spark save mode: `append`, `overwrite`, `ignore`, `error` or `errorifexists`. */
  private val SAVE_MODE_KEY = "saveMode"

  /** Explicit streaming flag; the job execution mode is authoritative when present. */
  private val IS_STREAMING_KEY = "isStreaming"

  /** Legacy streaming aliases kept for existing job configurations. */
  private val STREAM_WRITE_KEY = "streamWrite"

  private val STREAM_WRITER_KEY = "streamWriter"

  private val STREAMING_WRITER_KEY = "streamingWriter"

  /** Streaming output mode; a file sink supports only append. */
  private val OUTPUT_MODE_KEY = "outputMode"

  /** Streaming checkpoint location; defaults to `<path>_checkpoint`. */
  private val CHECKPOINT_LOCATION_KEY = "checkpointLocation"

  /** Optional wait before a streaming query is stopped. */
  private val TIMEOUT_MS_KEY = "timeoutMs"

  private val FileTypes = Set("csv", "text", "orc", "parquet", "json")

  /**
   * These filesystems support the staged backup/swap replacement. Object stores emulate rename
   * with copy/delete and require an explicit non-atomic opt-in.
   */
  private val StagedSchemes = Set("file", "hdfs", "viewfs")

  override def identifier: String = "file-sink"

  override def supportedExecutionModes: Set[ExecutionMode] =
    Set(ExecutionMode.Batch, ExecutionMode.Streaming)

  override def supportsFullRefresh: Boolean = true

  /**
   * A plain file directory has no atomic rename-with-overwrite on the supported Hadoop
   * filesystems. The staged implementation below is crash-safe but readers can briefly observe
   * a missing target, so it is not advertised as an atomic replacement.
   */
  override def supportsAtomicReplace: Boolean = false

  /**
   * Write a complete snapshot to a sibling staging path and replace the target with a
   * backup/swap sequence. A previous target is restored if the swap fails. This is crash-safe,
   * not reader-atomic; callers must opt in for `sync full` with
   * `allowNonAtomicReplace=true`.
   */
  override def writeFullRefresh(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config match {
      case Some(value) => value
      case None => throw new PuddingException("config can not be null!")
    }
    val path = this.requiredString(cfg, PATH_KEY)
    val fileType = this.fileType(cfg)
    val options = this.getStringOptions(cfg, OPTIONS_KEY)
    val target = new Path(path)
    val hadoopConf = dataFrame.sparkSession.sessionState.newHadoopConf()
    val fileSystem = target.getFileSystem(hadoopConf)
    val scheme = Option(fileSystem.getScheme)
      .map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    if (!StagedSchemes.contains(scheme)) {
      if (this.booleanValue(cfg, DataSink.AllowNonAtomicReplaceKey).getOrElse(false)) {
        this.writer(dataFrame, context)
      } else {
        throw new PuddingException(
          s"file-sink cannot safely replace a target on scheme '$scheme'; " +
            s"set ${DataSink.AllowNonAtomicReplaceKey}=true to use an in-place overwrite")
      }
    } else {
      val parent = Option(target.getParent).getOrElse(new Path("."))
      val staging = new Path(parent, s".${target.getName}.pudding-staging")
      val backup = new Path(parent, s".${target.getName}.pudding-backup")
      // Recover a backup left by a process crash between the two renames. If the previous
      // target exists, the swap already published and the backup is stale; otherwise restore it.
      if (!fileSystem.exists(target) && fileSystem.exists(backup)) {
        if (!fileSystem.rename(backup, target)) {
          throw new PuddingException(
            s"failed to restore the previous file target from a stale backup: $backup")
        }
      }
      if (fileSystem.exists(target) && fileSystem.exists(backup)) {
        fileSystem.delete(backup, true)
      }
      fileSystem.delete(staging, true)
      var published = false
      try {
        dataFrame.write.options(options).mode("overwrite").format(fileType).save(staging.toString)
        val hadTarget = fileSystem.exists(target)
        if (hadTarget && !fileSystem.rename(target, backup)) {
          throw new PuddingException(
            s"failed to stage the previous file target for replacement: $target")
        }
        if (!fileSystem.rename(staging, target)) {
          if (hadTarget && fileSystem.exists(backup)) {
            fileSystem.rename(backup, target)
          }
          throw new PuddingException(s"failed to publish the file full refresh at $target")
        }
        published = true
        if (hadTarget) {
          fileSystem.delete(backup, true)
        }
      } finally {
        fileSystem.delete(staging, true)
        // Keep a backup when publication failed so the next run can restore it. Restore was
        // attempted above; if it failed, leaving the backup is safer than deleting old data.
        if (published) {
          fileSystem.delete(backup, true)
        }
      }
    }
  }

  override def writer(dataFrame: DataFrame, config: Option[Map[String, Any]]): Unit = {
    this.writer(dataFrame, SinkContext(config))
  }

  override def writer(dataFrame: DataFrame, context: SinkContext): Unit = {
    val cfg = context.config match {
      case Some(cfg) => cfg
      case None => throw new PuddingException("config can not be null!")
    }

    val path = this.requiredString(cfg, PATH_KEY)
    val fileType = this.requiredString(cfg, FILE_TYPE_KEY).toLowerCase(Locale.ROOT)

    val streamWriter = streamingEnabled(cfg, context.executionMode)

    val options = this.getStringOptions(cfg, OPTIONS_KEY)

    fileType match {
      case "csv" | "text" | "orc" | "parquet" | "json" =>
        if (streamWriter) {
          val outputMode = this.outputMode(cfg)
          val configuredCheckpoint = this.optionalString(cfg, CHECKPOINT_LOCATION_KEY)
            .map(_.trim)
          val optionCheckpoint = options.collectFirst {
            case (key, value)
              if key.equalsIgnoreCase(CHECKPOINT_LOCATION_KEY) && value.trim.nonEmpty =>
              value.trim
          }
          if (configuredCheckpoint.isDefined && optionCheckpoint.isDefined &&
            this.normalizedCheckpoint(configuredCheckpoint.get) !=
              this.normalizedCheckpoint(optionCheckpoint.get)) {
            throw new PuddingException(
              "file sink checkpoint location is configured twice: " +
                s"$CHECKPOINT_LOCATION_KEY and options.$CHECKPOINT_LOCATION_KEY")
          }
          val checkpointLocation = configuredCheckpoint
            .orElse(optionCheckpoint)
            .getOrElse(s"${path.stripSuffix("/")}_checkpoint")
          val timeout = timeoutMs(cfg)
          StreamingCheckpoint.validate(
            cfg, context.trigger, Some(checkpointLocation), context.stateContract)
          val dataStreamWriter = dataFrame.writeStream
            .options(options)
            .outputMode(outputMode)
            .option(CHECKPOINT_LOCATION_KEY, checkpointLocation)
          val triggeredWriter = StreamingTrigger.applyTo(dataStreamWriter, context.trigger)
          val streamingQuery = triggeredWriter.format(fileType).start(path)

          timeout match {
            case Some(value) =>
              StreamingCheckpoint.awaitTermination(
                streamingQuery, cfg, context.trigger, Some(checkpointLocation), value)
            case None =>
              StreamingCheckpoint.awaitTermination(
                streamingQuery, cfg, context.trigger, Some(checkpointLocation))
          }
          streamingQuery.stop()
        } else {
          // batch writer handle
          val saveMode = this.requiredString(cfg, SAVE_MODE_KEY)
          val dataFrameWriter = dataFrame.write.options(options).mode(saveMode)
          dataFrameWriter.format(fileType).save(path)
        }
      case _ => throw new PuddingException(s"unsupported file type: $fileType")
    }
  }

  private def normalizedCheckpoint(value: String): String = {
    value.trim.stripSuffix("/")
  }

  private[sink] def streamingEnabled(cfg: Map[String, Any]): Boolean = {
    this.streamingEnabled(cfg, None)
  }

  private def streamingEnabled(
      cfg: Map[String, Any],
      executionMode: Option[ExecutionMode]): Boolean = {
    val configured = Seq(IS_STREAMING_KEY, STREAM_WRITE_KEY, STREAM_WRITER_KEY, STREAMING_WRITER_KEY)
      .iterator
      .flatMap(key => booleanValue(cfg, key))
      .exists(_ == true)
    this.effectiveStreaming(configured, executionMode)
  }

  private def fileType(cfg: Map[String, Any]): String = {
    val value = this.requiredString(cfg, FILE_TYPE_KEY).toLowerCase(Locale.ROOT)
    if (!FileTypes.contains(value)) {
      throw new PuddingException(s"unsupported file type: $value")
    }
    value
  }

  private def requiredString(cfg: Map[String, Any], key: String): String = {
    this.optionalString(cfg, key).getOrElse(
      throw new PuddingException(s"can not find param: $key, $key must be exist!"))
  }

  private def optionalString(cfg: Map[String, Any], key: String): Option[String] = {
    cfg.get(key).filter(_ != null) match {
      case Some(value: String) if value.trim.nonEmpty => Some(value.trim)
      case Some(_: String) => throw new PuddingException(s"$key can not be empty!")
      case Some(_) => throw new PuddingException(s"$key must be a string!")
      case None => None
    }
  }

  private def outputMode(cfg: Map[String, Any]): String = {
    val mode = this.optionalString(cfg, OUTPUT_MODE_KEY)
      .getOrElse("append").toLowerCase(Locale.ROOT)
    if (mode != "append") {
      throw new PuddingException(
        s"unsupported $OUTPUT_MODE_KEY: $mode, a streaming file sink supports only append")
    }
    mode
  }

  /**
   * Configuration values can be typed (YAML/JSON) or strings (HOCON), so accept both.
   */
  private def booleanValue(cfg: Map[String, Any], key: String): Option[Boolean] = {
    cfg.get(key).filter(_ != null).map {
      case value: Boolean => value
      case value: String =>
        value.trim.toLowerCase(Locale.ROOT) match {
          case "true" => true
          case "false" => false
          case other => throw new PuddingException(s"$key must be true or false, got '$other'")
        }
      case value =>
        throw new PuddingException(s"$key must be a boolean, got ${value.getClass.getSimpleName}")
    }
  }

  private def timeoutMs(cfg: Map[String, Any]): Option[Long] = {
    val timeout = cfg.get(TIMEOUT_MS_KEY).filter(_ != null).map {
      case value: Number => value.longValue()
      case value: String =>
        try {
          value.trim.toLong
        } catch {
          case _: NumberFormatException =>
            throw new PuddingException(s"$TIMEOUT_MS_KEY must be a number, got '$value'")
        }
      case value =>
        throw new PuddingException(
          s"$TIMEOUT_MS_KEY must be a number, got ${value.getClass.getSimpleName}")
    }
    timeout.foreach { value =>
      if (value <= 0) {
        throw new PuddingException(s"$TIMEOUT_MS_KEY must be greater than zero, got $value")
      }
    }
    timeout
  }
}
