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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * An immutable job configuration that can be submitted by any {@link ExecutionTarget}.
 *
 * <p>The content is kept as text so the same configuration can be sent to a remote driver
 * unchanged. Supported configuration types are {@code yaml}, {@code json} and {@code hocon}
 * ({@code conf} is an alias for {@code hocon}).</p>
 */
public final class JobConfiguration {

    public static final String TYPE_YAML = "yaml";

    public static final String TYPE_JSON = "json";

    public static final String TYPE_HOCON = "hocon";

    private final String type;

    private final String content;

    private final String source;

    private JobConfiguration(String type, String content, String source) {
        this.type = type;
        this.content = content;
        this.source = source;
    }

    /**
     * Read a job configuration from a {@code .yaml}, {@code .json}, {@code .hocon} or
     * {@code .conf} file.
     *
     * @param path configuration file path
     * @return job configuration
     * @throws SubmissionException if the file cannot be read or its extension is unsupported
     */
    public static JobConfiguration fromFile(Path path) {
        if (path == null) {
            throw new SubmissionException("job configuration file can not be null");
        }
        Path absolutePath = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(absolutePath)) {
            throw new SubmissionException("job configuration file does not exist: " + absolutePath);
        }
        String fileName = absolutePath.getFileName().toString().toLowerCase(Locale.ROOT);
        int extensionIndex = fileName.lastIndexOf('.');
        if (extensionIndex < 0) {
            throw new SubmissionException(
                    "configure file must end with .json, .hocon, .conf or .yaml: "
                            + absolutePath);
        }
        String type = normalizeType(
                fileName.substring(extensionIndex + 1), absolutePath.toString());
        try {
            String content = new String(Files.readAllBytes(absolutePath), StandardCharsets.UTF_8);
            return create(type, content, absolutePath.toString());
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to read job configuration file: " + absolutePath, e);
        }
    }

    /**
     * Create a job configuration from in-memory text.
     *
     * @param type    {@code yaml}, {@code json}, {@code hocon} or {@code conf}
     * @param content configuration text
     * @return job configuration
     */
    public static JobConfiguration fromText(String type, String content) {
        return create(normalizeType(type, "in-memory job configuration"), content, "<memory>");
    }

    private static JobConfiguration create(String type, String content, String source) {
        if (content == null || content.trim().isEmpty()) {
            throw new SubmissionException("job configuration content can not be empty");
        }
        return new JobConfiguration(type, content, source);
    }

    private static String normalizeType(String type, String location) {
        if (type == null || type.trim().isEmpty()) {
            throw new SubmissionException(
                    "configure type can not be empty for " + location);
        }
        String normalized = type.trim().toLowerCase(Locale.ROOT);
        if (normalized.startsWith(".")) {
            normalized = normalized.substring(1);
        }
        switch (normalized) {
            case TYPE_YAML:
            case TYPE_JSON:
                return normalized;
            case "conf":
            case TYPE_HOCON:
                return TYPE_HOCON;
            default:
                throw new SubmissionException(
                        "unsupported configure file type: " + type
                                + ", configure file must end with .json, .hocon, .conf or .yaml");
        }
    }

    /**
     * @return normalized configuration type
     */
    public String getType() {
        return type;
    }

    /**
     * Return a read-only view of the job's Spark configuration.
     *
     * <p>The view is parsed on each call. It contains only values under {@code job.config}, not
     * the job pipeline or deployment settings supplied by an execution target.</p>
     *
     * @return immutable Spark configuration map
     * @throws SubmissionException if the job configuration can not be parsed
     */
    public Map<String, String> getSparkConf() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(
                ScalaInterop.sparkConf(ScalaInterop.parse(this).job())));
    }

    /**
     * @return raw configuration text
     */
    public String getContent() {
        return content;
    }

    /**
     * @return file path, or {@code <memory>} for in-memory configurations
     */
    public String getSource() {
        return source;
    }

    @Override
    public String toString() {
        return "JobConfiguration{type='" + type + "', source='" + source + "'}";
    }
}
