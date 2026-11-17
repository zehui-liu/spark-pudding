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

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.pudding.core.PuddingException
import org.pudding.core.configure.ExecutionMode
import org.pudding.core.definition.SourceContext
import org.pudding.core.incremental.StreamingCdc
import org.pudding.core.util.StreamingTrigger

/**
 * Common DataFrame construction for the MySQL, PostgreSQL and MongoDB native CDC sources.
 *
 * The Spark V2 provider returns one JSON string column. The typed schema is applied with
 * `from_json`, which keeps the executor side connector-free and makes the output schema explicit
 * and stable for checkpoint validation.
 */
private[cdc] object CdcSourceSupport {

  private val ParsedColumn = "_pudding_cdc_document"

  def read(
      sparkSession: SparkSession,
      context: SourceContext,
      kind: String): DataFrame = {
    this.validateExecutionMode(context, kind)
    val (configuredOptions, schema, opColumn) = CdcSourceOptions.resolve(context, kind)
    val token = java.util.UUID.randomUUID().toString
    HadoopDatabaseHistory.registerToken(
      token, sparkSession.sparkContext.hadoopConfiguration)
    val options = configuredOptions + (CdcConfig.HadoopTokenKey -> token)
    val envelope = CdcSchema.envelope(schema, opColumn)
    val raw = sparkSession.readStream
      .format(CdcTableProvider.Format)
      .options(options)
      .load()
    val parsed = raw.withColumn(
      ParsedColumn,
      from_json(col(CdcSchema.ValueColumn), envelope, Map("mode" -> "FAILFAST")))
    val dataColumns = schema.fieldNames.map { name =>
      col(ParsedColumn).getField(name).as(name)
    }
    val internalColumns = Seq(
      col(ParsedColumn).getField(opColumn).as(opColumn),
      col(ParsedColumn).getField(CdcSchema.TimestampColumn).as(CdcSchema.TimestampColumn),
      col(ParsedColumn).getField(CdcSchema.SequenceColumn).as(CdcSchema.SequenceColumn),
      col(ParsedColumn).getField(CdcSchema.TableColumn).as(CdcSchema.TableColumn))
    val selected = parsed.select((dataColumns ++ internalColumns): _*)
    StreamingCdc.normalizeOps(selected, opColumn)
  }

  private def validateExecutionMode(context: SourceContext, kind: String): Unit = {
    context.executionMode match {
      case Some(ExecutionMode.Streaming) | None => ()
      case Some(other) =>
        throw new PuddingException(
          s"$kind-cdc-source supports streaming mode only, but got: ${other.name}")
    }
    if (StreamingTrigger.isFinite(context.trigger)) {
      throw new PuddingException(
        s"$kind-cdc-source requires a continuous trigger; once and available-now are not " +
          "supported because the source has no bounded end offset")
    }
  }
}
