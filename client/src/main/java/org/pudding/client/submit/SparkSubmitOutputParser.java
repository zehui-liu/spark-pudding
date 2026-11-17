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

package org.pudding.client.submit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the application id and driver pod phase from spark-submit output.
 *
 * <p>Spark writes this output for cluster deployments, but it is only a fallback for lifecycle
 * tracking. YARN application ids are parsed for client-mode submissions, where the exit status of
 * the spark-submit process is the authoritative result. Error messages never include the parsed
 * line.</p>
 */
final class SparkSubmitOutputParser {

    private static final Pattern APPLICATION_STATUS = Pattern.compile(
            "Application status for (\\S+) \\(phase: (\\w+)\\)");

    private static final Pattern POD_PHASE = Pattern.compile(
            "^\\s*phase: (Pending|Running|Succeeded|Failed|Unknown)\\s*$");

    private static final Pattern YARN_SUBMITTING = Pattern.compile(
            "Submitting application (application_\\d+_\\d+) to ResourceManager");

    private static final Pattern YARN_REPORT = Pattern.compile(
            "Application report for (application_\\d+_\\d+)");

    static final ParsedLine EMPTY = new ParsedLine(null, null);

    private SparkSubmitOutputParser() {
    }

    /**
     * @param line spark-submit output line
     * @return parsed values; fields that were not present are null
     */
    static ParsedLine parse(String line) {
        if (line == null) {
            return EMPTY;
        }
        Matcher status = APPLICATION_STATUS.matcher(line);
        if (status.find()) {
            return new ParsedLine(status.group(1), status.group(2));
        }
        Matcher podPhase = POD_PHASE.matcher(line);
        if (podPhase.find()) {
            return new ParsedLine(null, podPhase.group(1));
        }
        Matcher yarnSubmitting = YARN_SUBMITTING.matcher(line);
        if (yarnSubmitting.find()) {
            return new ParsedLine(yarnSubmitting.group(1), null);
        }
        Matcher yarnReport = YARN_REPORT.matcher(line);
        if (yarnReport.find()) {
            return new ParsedLine(yarnReport.group(1), null);
        }
        return EMPTY;
    }

    /**
     * Parsed spark-submit output values.
     */
    static final class ParsedLine {

        private final String applicationId;

        private final String phase;

        private ParsedLine(String applicationId, String phase) {
            this.applicationId = applicationId;
            this.phase = phase;
        }

        String getApplicationId() {
            return applicationId;
        }

        String getPhase() {
            return phase;
        }
    }
}
