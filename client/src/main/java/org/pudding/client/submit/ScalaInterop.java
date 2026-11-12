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

package org.pudding.client.submit;

import org.pudding.core.JobConfigureParser;
import org.pudding.core.configure.CatalogConf;
import org.pudding.core.configure.JobConf;
import org.pudding.core.configure.JobPipelineConf;
import org.pudding.core.configure.SinkConf;
import org.pudding.core.configure.SourceConf;
import org.pudding.core.configure.TransformConf;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Small, package-private bridge between the Java SDK and the Scala configuration model.
 */
final class ScalaInterop {

    /**
     * The basic-transform module hosts several operators under one jar. Keep this list in sync
     * with its DataTransform implementations so a job using any of them gets the jar.
     */
    private static final Set<String> BASIC_TRANSFORM_TYPES = new HashSet<>(Arrays.asList(
            "filter-transform",
            "aggregate-transform",
            "union-transform",
            "column-transform",
            "limit-transform",
            "join-transform",
            "validate-transform",
            "dedup-transform",
            "schema-contract-transform"));

    private ScalaInterop() {
    }

    static JobPipelineConf parse(JobConfiguration configuration) {
        try {
            JobPipelineConf pipeline = JobConfigureParser.apply(configuration.getType())
                    .parseFromString(configuration.getContent());
            if (pipeline == null || pipeline.job() == null) {
                throw new SubmissionException(
                        "job configuration does not contain a job section");
            }
            return pipeline;
        } catch (SubmissionException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to parse job configuration: " + message(e), e);
        }
    }

    static List<String> connectorTypes(JobPipelineConf pipeline) {
        Set<String> types = new LinkedHashSet<>();

        Iterator<SourceConf> sources = pipeline.sources().iterator();
        while (sources.hasNext()) {
            SourceConf source = sources.next();
            String sourceType = source.type();
            addType(types, sourceType);
            addConnectorRuntime(types, sourceType);
            if ("kafka-source".equals(sourceType) && usesSchemaRegistry(source)) {
                addType(types, "kafka-schema-registry-runtime");
            }
        }

        Option<scala.collection.immutable.Seq<TransformConf>> transforms = pipeline.transforms();
        if (transforms != null && transforms.isDefined() && transforms.get() != null) {
            Iterator<TransformConf> iterator = transforms.get().iterator();
            while (iterator.hasNext()) {
                String transformType = iterator.next().type();
                addType(types, transformType);
                if (BASIC_TRANSFORM_TYPES.contains(transformType)) {
                    addType(types, "basic-transform");
                }
            }
        }

        Iterator<SinkConf> sinks = pipeline.sinks().iterator();
        while (sinks.hasNext()) {
            String sinkType = sinks.next().type();
            addType(types, sinkType);
            addConnectorRuntime(types, sinkType);
        }

        // Format and connector runtimes are shared jars, selected at most once per job.
        // A catalog-only job still needs the format runtime for its Spark extension and data
        // source, even when no Pudding source or sink of that format is used.
        Iterator<CatalogConf> catalogs = pipeline.catalogs().iterator();
        while (catalogs.hasNext()) {
            String catalogType = catalogs.next().type();
            if ("iceberg".equals(catalogType)) {
                addType(types, "iceberg-runtime");
            } else if ("jdbc".equals(catalogType)) {
                addFormatRuntime(types, "jdbc-source", "jdbc-sink");
            } else if ("hudi".equals(catalogType)) {
                addType(types, "hudi-runtime");
            } else if ("paimon".equals(catalogType)) {
                addType(types, "paimon-runtime");
            } else if ("gravitino".equals(catalogType)) {
                // The Gravitino catalog is resolved on the driver; it may be backed by any
                // supported table format, so make every possible runtime available.
                addType(types, "iceberg-runtime");
                addType(types, "hudi-runtime");
                addType(types, "paimon-runtime");
                addFormatRuntime(types, "jdbc-source", "jdbc-sink");
            }
        }

        // Built-in connection and catalog providers live outside core. They are small and are
        // required by any connection/catalog reference, including jobs with only a SQL transform,
        // so request both provider jars unconditionally.
        addType(types, "connection-providers");
        addType(types, "catalog-providers");

        return new ArrayList<>(types);
    }

    static Map<String, String> sparkConf(JobConf jobConf) {
        Map<String, String> result = new LinkedHashMap<>();
        Option<scala.collection.immutable.Map<String, String>> config = jobConf.config();
        if (config != null && config.isDefined() && config.get() != null) {
            Iterator<Tuple2<String, String>> iterator = config.get().iterator();
            while (iterator.hasNext()) {
                Tuple2<String, String> entry = iterator.next();
                result.put(entry._1(), entry._2());
            }
        }
        return result;
    }

    static String optionValue(Option<String> option) {
        if (option == null || option.isEmpty()) {
            return null;
        }
        return option.get();
    }

    /**
     * The Schema Registry runtime is optional and is only needed when Kafka values are decoded
     * with the Confluent Avro/Protobuf wire format.
     */
    private static boolean usesSchemaRegistry(SourceConf source) {
        Option<scala.collection.immutable.Map<String, Object>> config = source.config();
        if (config == null || config.isEmpty() || config.get() == null) {
            return false;
        }
        Option<Object> format = config.get().get("valueFormat");
        if (format == null || format.isEmpty() || format.get() == null) {
            return false;
        }
        String value = format.get().toString().trim().toLowerCase(java.util.Locale.ROOT);
        return "avro".equals(value) || "protobuf".equals(value) || "proto".equals(value);
    }

    private static void addType(Set<String> types, String type) {
        if (type != null && !type.trim().isEmpty()) {
            types.add(type.trim());
        }
    }

    /**
     * Add the shared runtime or support jar required by a source or sink connector type. The
     * connector jar contains only the Pudding adapter; third-party runtimes and shared connector
     * helpers are packaged separately and selected once per job.
     */
    private static void addConnectorRuntime(Set<String> types, String connectorType) {
        if (connectorType == null) {
            return;
        }
        String type = connectorType.trim();
        if ("jdbc-source".equals(type) || "jdbc-sink".equals(type)
                || "mysql-source".equals(type) || "mysql-sink".equals(type)) {
            addType(types, "jdbc-common");
        } else if ("hudi-source".equals(type) || "hudi-sink".equals(type)) {
            addType(types, "hudi-runtime");
        } else if ("paimon-source".equals(type) || "paimon-sink".equals(type)) {
            addType(types, "paimon-runtime");
        } else if ("iceberg-source".equals(type) || "iceberg-sink".equals(type)) {
            addType(types, "iceberg-runtime");
        } else if ("elasticsearch-source".equals(type) || "elasticsearch-sink".equals(type)) {
            addType(types, "elasticsearch-runtime");
        } else if ("mongodb-source".equals(type) || "mongodb-sink".equals(type)) {
            addType(types, "mongodb-runtime");
        } else if ("kafka-source".equals(type) || "kafka-sink".equals(type)) {
            addType(types, "kafka-runtime");
        } else if ("mysql-cdc-source".equals(type) || "postgres-cdc-source".equals(type)
                || "mongodb-cdc-source".equals(type)) {
            addType(types, "cdc-runtime");
        }
    }

    /**
     * Add the JDBC source/sink driver jar and its shared option helper for a JDBC catalog. Keep
     * the existing source/sink preference when the pipeline already selected one.
     */
    private static void addFormatRuntime(
            Set<String> types,
            String sourceType,
            String sinkType) {
        if (!types.contains(sourceType) && !types.contains(sinkType)) {
            addType(types, sourceType);
        }
        // A JDBC catalog uses the connector jar for its bundled drivers and may also load the
        // shared JDBC option helper.
        addType(types, "jdbc-common");
    }

    private static String message(RuntimeException e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }
}
