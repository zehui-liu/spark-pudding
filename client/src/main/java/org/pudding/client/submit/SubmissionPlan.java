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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Fully resolved description of a Spark submission.
 *
 * <p>A plan contains only the effective values used by the launcher. It deliberately does not
 * expose job configuration arguments in {@link #toString()} because they may contain
 * credentials.</p>
 *
 * <p>The job Secret fields are metadata only. The Secret content is never part of a plan.
 * {@link #isJobSecretOwned()} marks a per-submission Secret that the SDK must create before
 * launch and delete once the submission reaches a final state. Managed job Secrets are only
 * provisioned by {@link SubmissionManager}.</p>
 */
public final class SubmissionPlan {

    private final String targetName;

    private final String submissionId;

    private final String appName;

    private final String sparkHome;

    private final String master;

    private final String deployMode;

    private final String appResource;

    private final String mainClass;

    private final List<String> jars;

    private final Map<String, String> sparkConf;

    private final Map<String, String> environment;

    private final List<String> appArgs;

    private final List<Path> temporaryPaths;

    private final String jobSecret;

    private final String jobSecretKey;

    private final boolean jobSecretOwned;

    private final boolean verbose;

    private SubmissionPlan(Builder builder) {
        this.targetName = builder.targetName;
        this.submissionId = builder.submissionId;
        this.appName = builder.appName;
        this.sparkHome = builder.sparkHome;
        this.master = builder.master;
        this.deployMode = builder.deployMode;
        this.appResource = builder.appResource;
        this.mainClass = builder.mainClass;
        this.jars = Collections.unmodifiableList(new ArrayList<>(builder.jars));
        this.sparkConf = Collections.unmodifiableMap(new LinkedHashMap<>(builder.sparkConf));
        this.environment = Collections.unmodifiableMap(new LinkedHashMap<>(builder.environment));
        this.appArgs = Collections.unmodifiableList(new ArrayList<>(builder.appArgs));
        this.temporaryPaths = Collections.unmodifiableList(
                new ArrayList<>(builder.temporaryPaths));
        this.jobSecret = builder.jobSecret;
        this.jobSecretKey = builder.jobSecretKey;
        this.jobSecretOwned = builder.jobSecretOwned;
        this.verbose = builder.verbose;
    }

    /**
     * Start building a plan for a target.
     *
     * @param targetName target name
     * @return plan builder
     */
    public static Builder builder(String targetName) {
        return new Builder(targetName);
    }

    public String getTargetName() {
        return targetName;
    }

    /**
     * @return unique id assigned to this submission, or null for a manually built plan
     */
    public String getSubmissionId() {
        return submissionId;
    }

    public String getAppName() {
        return appName;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String getMaster() {
        return master;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public String getAppResource() {
        return appResource;
    }

    public String getMainClass() {
        return mainClass;
    }

    public List<String> getJars() {
        return jars;
    }

    public Map<String, String> getSparkConf() {
        return sparkConf;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public List<String> getAppArgs() {
        return appArgs;
    }

    /**
     * @return temporary files or directories that must be removed after the application finishes
     */
    public List<Path> getTemporaryPaths() {
        return temporaryPaths;
    }

    /**
     * @return Kubernetes job configuration Secret name, if this plan uses one
     */
    public Optional<String> getJobSecret() {
        return Optional.ofNullable(jobSecret);
    }

    /**
     * @return key inside the Kubernetes job configuration Secret, if this plan uses one
     */
    public Optional<String> getJobSecretKey() {
        return Optional.ofNullable(jobSecretKey);
    }

    /**
     * @return true when this plan uses a per-submission Secret owned by the SDK
     */
    public boolean isJobSecretOwned() {
        return jobSecretOwned;
    }

    public boolean isVerbose() {
        return verbose;
    }

    @Override
    public String toString() {
        return "SubmissionPlan{target='" + targetName
                + "', appName='" + appName
                + "', master='" + master
                + "', deployMode='" + deployMode
                + "', appResource='" + appResource
                + "', jarCount=" + jars.size()
                + ", sparkConfKeys=" + sparkConf.keySet()
                + '}';
    }

    /**
     * Builder for {@link SubmissionPlan}.
     */
    public static final class Builder {

        private final String targetName;

        private String submissionId;

        private String appName;

        private String sparkHome;

        private String master;

        private String deployMode;

        private String appResource;

        private String mainClass;

        private final List<String> jars = new ArrayList<>();

        private final Map<String, String> sparkConf = new LinkedHashMap<>();

        private final Map<String, String> environment = new LinkedHashMap<>();

        private final List<String> appArgs = new ArrayList<>();

        private final List<Path> temporaryPaths = new ArrayList<>();

        private String jobSecret;

        private String jobSecretKey;

        private boolean jobSecretOwned;

        private boolean verbose;

        private Builder(String targetName) {
            this.targetName = requireText(targetName, "target name");
        }

        /**
         * @param submissionId unique id for lifecycle tracking
         * @return this builder
         */
        public Builder submissionId(String submissionId) {
            this.submissionId = submissionId;
            return this;
        }

        public Builder appName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder sparkHome(String sparkHome) {
            this.sparkHome = sparkHome;
            return this;
        }

        public Builder master(String master) {
            this.master = master;
            return this;
        }

        public Builder deployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }

        public Builder appResource(String appResource) {
            this.appResource = appResource;
            return this;
        }

        public Builder mainClass(String mainClass) {
            this.mainClass = mainClass;
            return this;
        }

        /**
         * Replace the jar list.
         *
         * @param jars jar paths or URIs
         * @return this builder
         */
        public Builder jars(Collection<String> jars) {
            this.jars.clear();
            if (jars != null) {
                this.jars.addAll(jars);
            }
            return this;
        }

        /**
         * Add one jar path or URI.
         *
         * @param jar jar path or URI
         * @return this builder
         */
        public Builder addJar(String jar) {
            this.jars.add(requireText(jar, "jar path"));
            return this;
        }

        /**
         * Replace the Spark configuration supplied to the launcher.
         *
         * @param values Spark configuration
         * @return this builder
         */
        public Builder sparkConf(Map<String, String> values) {
            this.sparkConf.clear();
            if (values != null) {
                values.forEach(this::sparkConf);
            }
            return this;
        }

        /**
         * Add one Spark configuration entry.
         *
         * @param key   configuration key
         * @param value configuration value
         * @return this builder
         */
        public Builder sparkConf(String key, String value) {
            this.sparkConf.put(
                    requireText(key, "spark configuration key"),
                    requireNonNull(value, "spark configuration value for '" + key + "'"));
            return this;
        }

        /**
         * Replace the environment passed to the Spark submission process.
         *
         * @param values environment variables
         * @return this builder
         */
        public Builder environment(Map<String, String> values) {
            this.environment.clear();
            if (values != null) {
                values.forEach(this::environment);
            }
            return this;
        }

        /**
         * Add one environment variable.
         *
         * @param key   variable name
         * @param value variable value
         * @return this builder
         */
        public Builder environment(String key, String value) {
            this.environment.put(
                    requireText(key, "environment variable name"),
                    requireNonNull(value, "environment variable value for '" + key + "'"));
            return this;
        }

        /**
         * Add application arguments.
         *
         * @param args arguments
         * @return this builder
         */
        public Builder addAppArgs(String... args) {
            if (args != null) {
                for (String arg : args) {
                    this.appArgs.add(requireNonNull(arg, "application argument"));
                }
            }
            return this;
        }

        /**
         * Append to a comma-separated Spark configuration value.
         *
         * @param key   configuration key
         * @param value value to append
         * @return this builder
         */
        public Builder appendSparkConfValue(String key, String value) {
            requireText(key, "spark configuration key");
            requireText(value, "spark configuration value for '" + key + "'");
            String existing = sparkConf.get(key);
            if (existing == null || existing.trim().isEmpty()) {
                return sparkConf(key, value);
            }
            return sparkConf(key, existing + "," + value);
        }

        /**
         * Register a temporary path that is removed when the application finishes.
         *
         * @param path temporary file or directory
         * @return this builder
         */
        public Builder addTemporaryPath(Path path) {
            temporaryPaths.add(Objects.requireNonNull(path, "temporary path"));
            return this;
        }

        List<Path> temporaryPaths() {
            return new ArrayList<>(temporaryPaths);
        }

        /**
         * Set the Kubernetes job configuration Secret metadata.
         *
         * @param jobSecret    Secret name
         * @param jobSecretKey key inside the Secret
         * @param owned        whether the SDK owns and must delete the Secret
         * @return this builder
         */
        public Builder jobSecret(String jobSecret, String jobSecretKey, boolean owned) {
            this.jobSecret = trimToNull(jobSecret);
            this.jobSecretKey = trimToNull(jobSecretKey);
            this.jobSecretOwned = owned;
            return this;
        }

        /**
         * @param verbose whether to print the generated Spark submit command
         * @return this builder
         */
        public Builder verbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Validate and create the plan.
         *
         * @return immutable plan
         */
        public SubmissionPlan build() {
            requireText(appName, "application name");
            requireText(sparkHome, "Spark home");
            requireText(master, "Spark master");
            requireText(deployMode, "Spark deploy mode");
            requireText(appResource, "application resource");
            requireText(mainClass, "main class");
            for (String jar : jars) {
                requireText(jar, "jar path");
            }
            for (Path path : temporaryPaths) {
                Objects.requireNonNull(path, "temporary path");
            }
            if (jobSecret != null && jobSecretKey == null) {
                throw new SubmissionException(
                        "a Kubernetes job Secret requires a Secret key in the plan");
            }
            if (jobSecret == null && (jobSecretKey != null || jobSecretOwned)) {
                throw new SubmissionException(
                        "a Kubernetes job Secret key or owner flag requires a Secret name");
            }
            return new SubmissionPlan(this);
        }

        private static String trimToNull(String value) {
            if (value == null || value.trim().isEmpty()) {
                return null;
            }
            return value.trim();
        }

        private static String requireText(String value, String field) {
            if (value == null || value.trim().isEmpty()) {
                throw new SubmissionException(field + " can not be empty");
            }
            return value.trim();
        }

        private static String requireNonNull(String value, String field) {
            if (value == null) {
                throw new SubmissionException(field + " can not be null");
            }
            return value;
        }
    }
}
