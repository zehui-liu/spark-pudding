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

package org.pudding.core.spark.mock

import org.pudding.core.configure.{JobConf, SinkConf, SourceConf, TransformConf}


/**
 * for test mock parameter
 */
object MockConf {

  val mockJobCfg: JobConf = JobConf(
    "local test",
    Option("describe"),
    Option("true"),
    Option(Map("spark.master" -> "local[2]"))
  )

  val mockSourceCfg: SourceConf = SourceConf(
    "mock_source",
    Option("just for test"),
    "source_1",
    Option(Map.empty[String, Object])
  )

  val mockTransformCfg: TransformConf = TransformConf(
    "mock_filter",
    Option("just for test"),
    "trans_1",
    Option(Map.empty[String, Object]),
    Seq("source_1")
  )

  val mockSinkCfg: SinkConf = SinkConf(
    "mock_sink",
    Option("just for test"),
    "sink_1",
    Option(Map.empty[String, Object]),
    "trans_1"
  )

}
