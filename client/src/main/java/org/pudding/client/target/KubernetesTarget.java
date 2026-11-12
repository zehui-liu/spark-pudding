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

package org.pudding.client.target;

import org.apache.spark.launcher.SparkLauncher;
import org.pudding.client.submit.AbstractSparkExecutionTarget;
import org.pudding.client.submit.JobConfiguration;
import org.pudding.client.submit.KubernetesRunningApplication;
import org.pudding.client.submit.RunningApplication;
import org.pudding.client.submit.SubmissionException;
import org.pudding.client.submit.SubmissionPlan;
import org.pudding.client.submit.SubmissionRequest;
import org.pudding.client.submit.TargetNames;
import org.pudding.client.submit.k8s.KubernetesJobSecretManager;
import org.pudding.client.submit.k8s.KubernetesPodMonitor;
import org.pudding.core.configure.JobConf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Submits a Spark application to Kubernetes.
 *
 * <p>Kubernetes submissions use cluster deploy mode. The resource base must be visible to the
 * cluster: {@code local:///...} for jars baked into the image, or a remote URI such as
 * {@code hdfs://...} or {@code s3a://...}. Plain local paths are rejected because the driver and
 * executors run in containers.</p>
 *
 * <p>The job configuration is delivered through a Kubernetes Secret. The Secret can be set on the
 * target, supplied per request, or generated per submission when the request asks for SDK-managed
 * delivery. The driver reads the configuration from the {@value #JOB_CONFIG_ENV} environment
 * variable injected by Spark, so the configuration does not appear in the submit command or the
 * driver Pod specification.</p>
 *
 * <p>A managed Secret is created and deleted by {@link
 * org.pudding.client.submit.SubmissionManager}; submitting such a plan directly through
 * {@link #submit(SubmissionPlan)} does not provision it.</p>
 */
public final class KubernetesTarget extends AbstractSparkExecutionTarget {

    public static final String NAME = TargetNames.KUBERNETES;

    private static final String KUBERNETES_MASTER_PREFIX = "k8s://";

    private static final String IMAGE_KEY = "spark.kubernetes.container.image";

    private static final String NAMESPACE_KEY = "spark.kubernetes.namespace";

    private static final String SERVICE_ACCOUNT_KEY =
            "spark.kubernetes.authenticate.driver.serviceAccountName";

    private static final String IMAGE_PULL_SECRETS_KEY =
            "spark.kubernetes.container.image.pullSecrets";

    private static final String KUBECONFIG_ENV = "KUBECONFIG";

    /**
     * Environment variable that contains the job configuration inside the driver container.
     */
    public static final String JOB_CONFIG_ENV = "PUDDING_JOB_CONFIG";

    /**
     * Default key inside the job configuration Secret.
     */
    public static final String DEFAULT_JOB_SECRET_KEY = "job.conf";

    /**
     * Spark configuration key that adds the submission id label to the driver pod.
     */
    public static final String SUBMISSION_LABEL_CONF =
            "spark.kubernetes.driver.label." + KubernetesPodMonitor.SUBMISSION_LABEL;

    private static final Logger LOGGER =
            Logger.getLogger("org.pudding.client.target.kubernetes");

    private static final String DEFAULT_NAMESPACE = "default";

    private static final String DRIVER_SECRET_KEY_REF_PREFIX =
            "spark.kubernetes.driver.secretKeyRef.";

    private static final String MANAGED_SECRET_PREFIX = "pudding-job-";

    private static final Pattern SECRET_NAME_PATTERN =
            Pattern.compile("[a-z0-9]([-a-z0-9.]*[a-z0-9])?");

    private static final Pattern SECRET_KEY_PATTERN =
            Pattern.compile("[-._a-zA-Z0-9]+");

    private static final int MAX_SECRET_NAME_LENGTH = 253;

    private final String configuredMaster;

    private final String image;

    private final String namespace;

    private final String serviceAccount;

    private final List<String> imagePullSecrets;

    private final String kubeconfig;

    private final String jobSecret;

    private final String jobSecretKey;

    private KubernetesTarget(Builder builder) {
        super(
                builder.sparkHome,
                builder.sparkPuddingHome,
                builder.resourceBase,
                builder.sparkConf);
        this.configuredMaster = trimToNull(builder.master);
        this.image = trimToNull(builder.image);
        this.namespace = trimToNull(builder.namespace);
        this.serviceAccount = trimToNull(builder.serviceAccount);
        this.imagePullSecrets = Collections.unmodifiableList(
                new ArrayList<>(builder.imagePullSecrets));
        this.kubeconfig = trimToNull(builder.kubeconfig);
        this.jobSecret = trimToNull(builder.jobSecret);
        this.jobSecretKey = firstNonBlank(builder.jobSecretKey, DEFAULT_JOB_SECRET_KEY);
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
        return firstNonBlank(configuredMaster, jobConf.master());
    }

    @Override
    protected String resolveDeployMode(JobConf jobConf) {
        return "cluster";
    }

    @Override
    protected String resolveResourceBase(String localDistribution) {
        if (!hasResourceBase()) {
            throw new SubmissionException(
                    "kubernetes target requires resourceBase pointing to a cluster-visible "
                            + "location, for example local:///opt/spark-pudding or "
                            + "hdfs:///apps/spark-pudding");
        }
        return configuredResourceBase();
    }

    @Override
    protected void applyTargetSparkConf(Map<String, String> sparkConf, JobConf jobConf) {
        applyTargetSparkConf(sparkConf, jobConf, null);
    }

    @Override
    protected void applyTargetSparkConf(
            Map<String, String> sparkConf, JobConf jobConf, SubmissionRequest request) {
        if (!isBlank(image)) {
            sparkConf.put(IMAGE_KEY, image);
        }
        if (!isBlank(namespace)) {
            sparkConf.put(NAMESPACE_KEY, namespace);
        }
        if (!isBlank(serviceAccount)) {
            sparkConf.put(SERVICE_ACCOUNT_KEY, serviceAccount);
        }
        if (!imagePullSecrets.isEmpty()) {
            sparkConf.put(IMAGE_PULL_SECRETS_KEY, String.join(",", imagePullSecrets));
        }
    }

    @Override
    protected void addJobArguments(
            SubmissionPlan.Builder plan,
            JobConfiguration jobConfiguration,
            String deployMode) {
        plan.addAppArgs(
                "--confType", jobConfiguration.getType(),
                "--jobEnv", JOB_CONFIG_ENV);
    }

    @Override
    protected void applyTargetEnvironment(Map<String, String> environment, JobConf jobConf) {
        if (!isBlank(kubeconfig)) {
            environment.put(KUBECONFIG_ENV, kubeconfig);
        }
    }

    @Override
    protected void customizePlan(
            SubmissionPlan.Builder plan, JobConf jobConf, String submissionId) {
        customizePlan(plan, jobConf, submissionId, null);
    }

    @Override
    protected void customizePlan(
            SubmissionPlan.Builder plan,
            JobConf jobConf,
            String submissionId,
            SubmissionRequest request) {
        String secretName = effectiveJobSecret(request, submissionId);
        String secretKey = effectiveJobSecretKey(request);
        validateSecretName(secretName);
        validateSecretKey(secretKey);

        boolean owned = request != null && request.isManagedJobSecret();
        if (owned && request.getJob().getContent().getBytes(StandardCharsets.UTF_8).length
                > KubernetesJobSecretManager.MAX_CONTENT_BYTES) {
            throw new SubmissionException(
                    "job configuration is larger than "
                            + KubernetesJobSecretManager.MAX_CONTENT_BYTES
                            + " bytes; a Kubernetes Secret can not hold it");
        }
        plan.sparkConf(SUBMISSION_LABEL_CONF, submissionId)
                .sparkConf(
                        DRIVER_SECRET_KEY_REF_PREFIX + JOB_CONFIG_ENV,
                        secretName + ":" + secretKey)
                .jobSecret(secretName, secretKey, owned);
    }

    @Override
    protected RunningApplication start(SparkLauncher launcher, SubmissionPlan plan) {
        // Kubernetes cluster submissions do not report launcher protocol states, so track the
        // driver pod through the API. spark-submit output remains a fallback when the API client
        // can not be configured.
        String submissionId = plan.getSubmissionId();
        if (isBlank(submissionId)) {
            LOGGER.warning("kubernetes submission id is missing; using spark-submit output");
            return startProcess(launcher);
        }

        KubernetesPodMonitor monitor;
        try {
            monitor = KubernetesPodMonitor.create(plan);
        } catch (RuntimeException e) {
            LOGGER.log(
                    Level.WARNING,
                    "Kubernetes API lifecycle tracking is unavailable; using spark-submit "
                            + "output: " + message(e));
            return startProcess(launcher);
        }

        Process process;
        try {
            process = launchProcess(launcher);
        } catch (RuntimeException e) {
            try {
                monitor.close();
            } catch (RuntimeException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }

        LOGGER.info(
                "tracking Kubernetes driver pod through the API (namespace="
                        + resolveNamespace(plan) + ", submission=" + submissionId + ")");
        return KubernetesRunningApplication.start(process, getName(), submissionId, monitor);
    }

    private static String resolveNamespace(SubmissionPlan plan) {
        String namespace = plan.getSparkConf().get(NAMESPACE_KEY);
        return isBlank(namespace) ? DEFAULT_NAMESPACE : namespace.trim();
    }

    @Override
    protected void validateTarget(
            JobConf jobConf,
            Map<String, String> sparkConf,
            String master,
            String deployMode,
            String resourceBase) {
        validateTarget(jobConf, sparkConf, master, deployMode, resourceBase, null);
    }

    @Override
    protected void validateTarget(
            JobConf jobConf,
            Map<String, String> sparkConf,
            String master,
            String deployMode,
            String resourceBase,
            SubmissionRequest request) {
        if (!master.toLowerCase(Locale.ROOT).startsWith(KUBERNETES_MASTER_PREFIX)) {
            throw new SubmissionException(
                    "kubernetes target requires a master starting with 'k8s://', got: " + master);
        }
        if (!"cluster".equalsIgnoreCase(deployMode)) {
            throw new SubmissionException(
                    "kubernetes target only supports cluster deploy mode, got: " + deployMode);
        }
        if (!hasScheme(resourceBase) || resourceBase.toLowerCase(Locale.ROOT).startsWith("file:")) {
            throw new SubmissionException(
                    "kubernetes target requires a cluster-visible resource base "
                            + "(local://, hdfs://, s3a://, ...), got: " + resourceBase);
        }
        if (isBlank(sparkConf.get(IMAGE_KEY))) {
            throw new SubmissionException(
                    "kubernetes target requires a container image; set image(...) or "
                            + "spark.kubernetes.container.image");
        }
        boolean managed = request != null && request.isManagedJobSecret();
        boolean requestSecret = request != null && request.getJobSecret().isPresent();
        if (managed) {
            validateSecretKey(effectiveJobSecretKey(request));
            return;
        }
        if (!requestSecret && isBlank(jobSecret)) {
            throw new SubmissionException(
                    "kubernetes target requires a job configuration Secret; create one with "
                            + "kubectl and set jobSecret(...), call managedJobSecret() on the "
                            + "request, or use bin/pudding k8s submit --job-secret");
        }
        validateSecretName(effectiveJobSecret(request, null));
        validateSecretKey(effectiveJobSecretKey(request));
    }

    private String effectiveJobSecret(SubmissionRequest request, String submissionId) {
        if (request != null) {
            if (request.isManagedJobSecret()) {
                if (isBlank(submissionId)) {
                    throw new SubmissionException(
                            "a managed Kubernetes job Secret requires a submission id");
                }
                return MANAGED_SECRET_PREFIX + submissionId;
            }
            Optional<String> requested = request.getJobSecret();
            if (requested.isPresent()) {
                return requested.get();
            }
        }
        return jobSecret;
    }

    private String effectiveJobSecretKey(SubmissionRequest request) {
        if (request != null) {
            Optional<String> requested = request.getJobSecretKey();
            if (requested.isPresent()) {
                return requested.get();
            }
        }
        return jobSecretKey;
    }

    private static void validateSecretName(String name) {
        if (name.length() > MAX_SECRET_NAME_LENGTH
                || !SECRET_NAME_PATTERN.matcher(name).matches()) {
            throw new SubmissionException(
                    "invalid Kubernetes Secret name: " + name
                            + " (use lowercase alphanumeric, '-', or '.', at most "
                            + MAX_SECRET_NAME_LENGTH + " characters)");
        }
    }

    private static void validateSecretKey(String key) {
        if (key.length() > MAX_SECRET_NAME_LENGTH || !SECRET_KEY_PATTERN.matcher(key).matches()) {
            throw new SubmissionException(
                    "invalid Kubernetes Secret key: " + key
                            + " (use alphanumeric, '-', '_', or '.', at most "
                            + MAX_SECRET_NAME_LENGTH + " characters)");
        }
    }

    /**
     * Builder for {@link KubernetesTarget}.
     */
    public static final class Builder {

        private String master;

        private String image;

        private String namespace;

        private String serviceAccount;

        private final List<String> imagePullSecrets = new ArrayList<>();

        private String kubeconfig;

        private String jobSecret;

        private String jobSecretKey = DEFAULT_JOB_SECRET_KEY;

        private String sparkHome;

        private String sparkPuddingHome;

        private String resourceBase;

        private final Map<String, String> sparkConf = new LinkedHashMap<>();

        private Builder() {
        }

        /**
         * @param master Kubernetes master, for example {@code k8s://https://cluster:443}
         * @return this builder
         */
        public Builder master(String master) {
            this.master = master;
            return this;
        }

        /**
         * @param image Spark container image, mapped to
         *              {@code spark.kubernetes.container.image}
         * @return this builder
         */
        public Builder image(String image) {
            this.image = image;
            return this;
        }

        /**
         * @param namespace Kubernetes namespace
         * @return this builder
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * @param serviceAccount driver service account
         * @return this builder
         */
        public Builder serviceAccount(String serviceAccount) {
            this.serviceAccount = serviceAccount;
            return this;
        }

        /**
         * @param secrets image pull secret names
         * @return this builder
         */
        public Builder imagePullSecrets(Collection<String> secrets) {
            this.imagePullSecrets.clear();
            if (secrets != null) {
                for (String secret : secrets) {
                    if (secret == null || secret.trim().isEmpty()) {
                        throw new SubmissionException("image pull secret name can not be empty");
                    }
                    this.imagePullSecrets.add(secret.trim());
                }
            }
            return this;
        }

        /**
         * @param kubeconfig path exported as {@code KUBECONFIG} for the submission process
         * @return this builder
         */
        public Builder kubeconfig(String kubeconfig) {
            this.kubeconfig = kubeconfig;
            return this;
        }

        /**
         * Set the Secret that contains the job configuration. The Secret must exist in the
         * submission namespace before the job starts.
         *
         * @param jobSecret Secret name
         * @return this builder
         */
        public Builder jobSecret(String jobSecret) {
            this.jobSecret = jobSecret;
            return this;
        }

        /**
         * @param jobSecretKey key inside the job configuration Secret
         * @return this builder
         */
        public Builder jobSecretKey(String jobSecretKey) {
            this.jobSecretKey = jobSecretKey;
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
         * @param sparkPuddingHome local spark-pudding distribution root used for jar discovery
         * @return this builder
         */
        public Builder sparkPuddingHome(String sparkPuddingHome) {
            this.sparkPuddingHome = sparkPuddingHome;
            return this;
        }

        /**
         * @param resourceBase cluster-visible base location of {@code libs}
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
         * @return Kubernetes target
         */
        public KubernetesTarget build() {
            return new KubernetesTarget(this);
        }
    }

    private static String message(RuntimeException e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }
}
