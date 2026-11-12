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

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Names and inference rules for the built-in execution targets.
 */
public final class TargetNames {

    public static final String LOCAL = "local";

    public static final String YARN = "yarn";

    public static final String KUBERNETES = "kubernetes";

    /** Maximum length of a named target alias. */
    public static final int MAX_NAME_LENGTH = 64;

    private static final Pattern NAMED_TARGET =
            Pattern.compile("[a-z0-9][a-z0-9-]*");

    private TargetNames() {
    }

    /**
     * Validate and return a named target alias.
     *
     * <p>Unlike {@link #normalize(String)}, this accepts arbitrary Admin target ids. The name is
     * immutable and path-safe because it is also used in server-side file names.</p>
     *
     * @param target target alias
     * @return trimmed alias
     */
    public static String requireName(String target) {
        if (target == null || target.trim().isEmpty()) {
            throw new SubmissionException("execution target name can not be empty");
        }
        String name = target.trim();
        if (name.length() > MAX_NAME_LENGTH || !NAMED_TARGET.matcher(name).matches()) {
            throw new SubmissionException(
                    "execution target name must match [a-z0-9][a-z0-9-]* and be at most "
                            + MAX_NAME_LENGTH + " characters");
        }
        return name;
    }

    /**
     * Normalize a user supplied target name.
     *
     * @param target target name or alias
     * @return canonical target name
     */
    public static String normalize(String target) {
        if (target == null || target.trim().isEmpty()) {
            throw new SubmissionException("execution target can not be empty");
        }
        switch (target.trim().toLowerCase(Locale.ROOT)) {
            case LOCAL:
                return LOCAL;
            case YARN:
            case "yarn-client":
            case "yarn-cluster":
                return YARN;
            case "k8s":
            case KUBERNETES:
                return KUBERNETES;
            default:
                throw new SubmissionException(
                        "unsupported execution target: " + target
                                + ", supported targets are local, yarn, kubernetes");
        }
    }

    /**
     * Infer a target from a Spark master URL.
     *
     * @param master Spark master
     * @return canonical target name
     */
    public static String inferFromMaster(String master) {
        if (master == null || master.trim().isEmpty()) {
            throw new SubmissionException(
                    "cannot infer execution target from an empty Spark master; "
                            + "set job.master or pass --target");
        }
        String normalized = master.trim().toLowerCase(Locale.ROOT);
        if (normalized.equals(LOCAL)
                || normalized.startsWith("local[")
                || normalized.startsWith("local-")) {
            return LOCAL;
        }
        if (normalized.equals(YARN) || normalized.startsWith("yarn-")) {
            return YARN;
        }
        if (normalized.startsWith("k8s://")) {
            return KUBERNETES;
        }
        throw new SubmissionException(
                "cannot infer execution target from Spark master '" + master
                        + "'; pass --target explicitly");
    }
}
