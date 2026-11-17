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
import org.pudding.client.submit.JobConfiguration;
import org.pudding.client.submit.RunningApplication;
import org.pudding.client.submit.SubmissionException;
import org.pudding.client.submit.SubmissionPlan;
import org.pudding.client.submit.TargetNames;
import org.pudding.core.configure.JobConf;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Submits a Spark application to YARN.
 *
 * <p>Cluster deploy mode is used by default for platform submissions. Artifacts may live on the
 * submission host, but a versioned HDFS resource base is recommended so every application uses
 * the same immutable distribution. Cluster applications are tracked through the ResourceManager
 * REST API because YARN does not use the Spark launcher state protocol.</p>
 */
public final class YarnTarget extends AbstractSparkExecutionTarget {

    public static final String NAME = TargetNames.YARN;

    private static final String DEFAULT_DEPLOY_MODE = "cluster";

    private static final String DIST_FILES_KEY = "spark.yarn.dist.files";

    private static final String JOB_CONFIG_ALIAS = "pudding-job.conf";

    private final String configuredDeployMode;

    private final String queue;

    private YarnTarget(Builder builder) {
        super(
                builder.sparkHome,
                builder.sparkPuddingHome,
                builder.resourceBase,
                builder.sparkConf);
        this.configuredDeployMode = trimToNull(builder.deployMode);
        this.queue = trimToNull(builder.queue);
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
        return TargetNames.YARN;
    }

    @Override
    protected String resolveDeployMode(JobConf jobConf) {
        String deployMode = firstNonBlank(configuredDeployMode, jobConf.deployMode());
        if (deployMode == null) {
            return DEFAULT_DEPLOY_MODE;
        }
        String normalized = deployMode.toLowerCase(Locale.ROOT);
        if (!"client".equals(normalized) && !"cluster".equals(normalized)) {
            throw new SubmissionException(
                    "yarn target deploy mode must be client or cluster, got: " + deployMode);
        }
        return normalized;
    }

    @Override
    protected RunningApplication start(SparkLauncher launcher, SubmissionPlan plan) {
        if (usesProcessMonitoring(plan.getDeployMode())) {
            // YARN client mode runs the driver inside spark-submit. YARN does not implement the
            // Spark launcher state protocol for this mode, so a successful exit of the child
            // process is disposed as LOST by Spark's ChildProcAppHandle. The process exit status
            // is the authoritative result and also keeps the application id from the output.
            return startProcess(launcher);
        }
        // Cluster mode exits spark-submit after submission. Track the application through the
        // ResourceManager instead of treating the launcher handle's LOST state as a failure.
        return startYarnCluster(launcher, plan.getEnvironment());
    }

    static boolean usesProcessMonitoring(String deployMode) {
        return "client".equalsIgnoreCase(deployMode);
    }

    @Override
    protected void applyTargetSparkConf(Map<String, String> sparkConf, JobConf jobConf) {
        if (!isBlank(queue)) {
            sparkConf.put("spark.yarn.queue", queue);
        }
    }

    @Override
    protected void addJobArguments(
            SubmissionPlan.Builder plan,
            JobConfiguration jobConfiguration,
            String deployMode) {
        Path configFile = stageJobConfigFile(plan, jobConfiguration);
        String localPath = configFile.toAbsolutePath().normalize().toString();
        if ("cluster".equalsIgnoreCase(deployMode)) {
            // In cluster mode the driver runs on a NodeManager host. Ship the owner-only local
            // file through Spark's distributed-file mechanism and pass the alias instead of the
            // absolute submission-host path. The file is removed when the application ends.
            plan.appendSparkConfValue(DIST_FILES_KEY, localPath + "#" + JOB_CONFIG_ALIAS);
            plan.addAppArgs(
                    "--confType", jobConfiguration.getType(),
                    "--jobFile", JOB_CONFIG_ALIAS);
        } else {
            plan.addAppArgs(
                    "--confType", jobConfiguration.getType(),
                    "--jobFile", localPath);
        }
    }

    @Override
    protected void validateTarget(
            JobConf jobConf,
            Map<String, String> sparkConf,
            String master,
            String deployMode,
            String resourceBase) {
        if (!TargetNames.YARN.equals(master.toLowerCase(Locale.ROOT))) {
            throw new SubmissionException(
                    "yarn target requires Spark master 'yarn', got: " + master);
        }
        if (!"client".equalsIgnoreCase(deployMode)
                && !"cluster".equalsIgnoreCase(deployMode)) {
            throw new SubmissionException(
                    "yarn target deploy mode must be client or cluster, got: " + deployMode);
        }
    }

    /**
     * Builder for {@link YarnTarget}.
     */
    public static final class Builder {

        private String deployMode;

        private String queue;

        private String sparkHome;

        private String sparkPuddingHome;

        private String resourceBase;

        private final Map<String, String> sparkConf = new LinkedHashMap<>();

        private Builder() {
        }

        /**
         * @param deployMode {@code client} or {@code cluster}; defaults to cluster
         * @return this builder
         */
        public Builder deployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }

        /**
         * @param queue YARN queue, mapped to {@code spark.yarn.queue}
         * @return this builder
         */
        public Builder queue(String queue) {
            this.queue = queue;
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
         * @param resourceBase base location of {@code libs}, for example an HDFS directory
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
         * @return YARN target
         */
        public YarnTarget build() {
            return new YarnTarget(this);
        }
    }
}
