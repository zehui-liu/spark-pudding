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

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Spark V2 provider behind the `pudding-cdc` format. The public Pudding source types construct
 * it with already-resolved connection options; users normally do not call this format directly.
 */
class CdcTableProvider extends TableProvider with DataSourceRegister {

  override def shortName(): String = CdcTableProvider.Format

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    CdcConfig.fromOptions(options)
    CdcTableProvider.RawSchema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    new CdcTable(CdcConfig.fromOptions(options))
  }

  override def supportsExternalMetadata(): Boolean = false
}

private[cdc] object CdcTableProvider {

  val Format = "pudding-cdc"

  val RawSchema: StructType = StructType(Seq(
    StructField(CdcSchema.ValueColumn, StringType, nullable = false)))
}

private[cdc] final class CdcTable(config: CdcConfig)
    extends Table with SupportsRead {

  override def name(): String = s"${CdcTableProvider.Format}:${config.table}"

  override def schema(): StructType = CdcTableProvider.RawSchema

  override def capabilities(): util.Set[TableCapability] = {
    util.Collections.singleton(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new CdcScanBuilder(config)
  }
}

private[cdc] final class CdcScanBuilder(config: CdcConfig) extends ScanBuilder {

  override def build(): Scan = new CdcScan(config)
}

private[cdc] final class CdcScan(config: CdcConfig) extends Scan {

  override def readSchema(): StructType = CdcTableProvider.RawSchema

  override def toBatch: Batch = {
    throw new UnsupportedOperationException(
      "pudding-cdc is a native streaming source; use job.mode=streaming")
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new CdcMicroBatchStream(config, checkpointLocation)
  }

  override def description(): String = {
    s"pudding-cdc[${config.kind}](${config.table})"
  }
}
