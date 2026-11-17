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

package org.pudding.client.target;

import org.apache.spark.launcher.SparkLauncher;
import org.pudding.client.submit.AbstractSparkExecutionTarget;
import org.pudding.client.submit.RunningApplication;
import org.pudding.client.submit.SubmissionException;
import org.pudding.client.submit.SubmissionPlan;
import org.pudding.client.submit.TargetNames;
import org.pudding.core.configure.JobConf;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Launches a separate local {@code spark-submit} child process using a local Spark master.
 *
 * <p>This target always uses client deploy mode. It is intended for development, tests and small
 * local jobs; production submissions should use {@link YarnTarget} or {@link KubernetesTarget}.</p>
 */
public final class LocalTarget extends AbstractSparkExecutionTarget {

    public static final String NAME = TargetNames.LOCAL;

    private static final String DEFAULT_MASTER = "local[*]";

    private final String configuredMaster;

    private LocalTarget(Builder builder) {
        super(
                builder.sparkHome,
                builder.sparkPuddingHome,
                builder.resourceBase,
                builder.sparkConf);
        this.configuredMaster = trimToNull(builder.master);
    }

    /**
     * @return target builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected String resolveMaster(JobConf jobConf) {
        if (!isBlank(configuredMaster)) {
            return configuredMaster;
        }
        String jobMaster = jobConf.master();
        if (!isBlank(jobMaster)
                && jobMaster.trim().toLowerCase(Locale.ROOT).startsWith(TargetNames.LOCAL)) {
            return jobMaster.trim();
        }
        return DEFAULT_MASTER;
    }

    @Override
    protected String resolveDeployMode(JobConf jobConf) {
        return "client";
    }

    /**
     * Use the {@code spark-submit} process exit code for local jobs. SparkLauncher's
     * application state can report a finished application even when the driver failed, which
     * would turn a failed local job into a successful client result.
     */
    @Override
    protected RunningApplication start(SparkLauncher launcher, SubmissionPlan plan) {
        return startProcess(launcher);
    }

    @Override
    protected void validateTarget(
            JobConf jobConf,
            Map<String, String> sparkConf,
            String master,
            String deployMode,
            String resourceBase) {
        if (!master.toLowerCase(Locale.ROOT).startsWith(TargetNames.LOCAL)) {
            throw new SubmissionException(
                    "local target requires a local Spark master, got: " + master);
        }
        if (!"client".equalsIgnoreCase(deployMode)) {
            throw new SubmissionException(
                    "local target only supports client deploy mode, got: " + deployMode);
        }
    }

    /**
     * Builder for {@link LocalTarget}.
     */
    public static final class Builder {

        private String master;

        private String sparkHome;

        private String sparkPuddingHome;

        private String resourceBase;

        private final Map<String, String> sparkConf = new LinkedHashMap<>();

        private Builder() {
        }

        /**
         * @param master local Spark master, for example {@code local[4]}
         * @return this builder
         */
        public Builder master(String master) {
            this.master = master;
            return this;
        }

        /**
         * @param sparkHome Spark installation directory
         * @return this builder
         */
        public Builder sparkHome(String sparkHome) {
            this.sparkHome = sparkHome;
            return this;
        }

        /**
         * @param sparkPuddingHome local spark-pudding distribution root
         * @return this builder
         */
        public Builder sparkPuddingHome(String sparkPuddingHome) {
            this.sparkPuddingHome = sparkPuddingHome;
            return this;
        }

        /**
         * @param resourceBase base location of {@code libs}, defaults to the distribution root
         * @return this builder
         */
        public Builder resourceBase(String resourceBase) {
            this.resourceBase = resourceBase;
            return this;
        }

        /**
         * Replace extra Spark configuration.
         *
         * @param values Spark configuration
         * @return this builder
         */
        public Builder sparkConf(Map<String, String> values) {
            this.sparkConf.clear();
            if (values != null) {
                this.sparkConf.putAll(values);
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
            this.sparkConf.put(key, value);
            return this;
        }

        /**
         * @return local target
         */
        public LocalTarget build() {
            return new LocalTarget(this);
        }
    }
}
