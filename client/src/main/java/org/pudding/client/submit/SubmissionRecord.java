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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable, durable metadata for one submission.
 *
 * <p>A record deliberately contains only fields needed to list, reconcile, cancel, clean up and
 * read logs for a submission. It never contains job configuration, Spark configuration values,
 * environment value contents or kubeconfig contents. The kubeconfig field stores only a path, and
 * the Kubernetes job Secret fields store only its name, key and ownership flag.</p>
 *
 * <p>Records are value objects. State transitions are only performed through
 * {@link #withStatus(ApplicationState, String, Instant)} and
 * {@link #withFailure(ApplicationState, String, Instant)}; both return the same instance once a
 * final state has been reached.</p>
 */
public final class SubmissionRecord {

    /**
     * Maximum number of characters kept from an error message.
     */
    public static final int MAX_ERROR_LENGTH = 4096;

    private static final String TRUNCATED_SUFFIX = "...";

    private final String submissionId;

    private final String targetName;

    private final String appName;

    private final String master;

    private final String deployMode;

    private final String namespace;

    private final String kubeconfig;

    private final String kubernetesJobSecret;

    private final String kubernetesJobSecretKey;

    private final boolean kubernetesJobSecretOwned;

    private final String applicationId;

    private final ApplicationState state;

    private final String error;

    private final Instant createdAt;

    private final Instant updatedAt;

    private final Instant finishedAt;

    private SubmissionRecord(Builder builder) {
        this.submissionId = builder.submissionId;
        this.targetName = builder.targetName;
        this.appName = builder.appName;
        this.master = builder.master;
        this.deployMode = builder.deployMode;
        this.namespace = builder.namespace;
        this.kubeconfig = builder.kubeconfig;
        this.kubernetesJobSecret = builder.kubernetesJobSecret;
        this.kubernetesJobSecretKey = builder.kubernetesJobSecretKey;
        this.kubernetesJobSecretOwned = builder.kubernetesJobSecretOwned;
        this.applicationId = builder.applicationId;
        this.state = builder.state;
        this.error = builder.error;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
        this.finishedAt = builder.finishedAt;
    }

    /**
     * Start building a record.
     *
     * @param submissionId unique submission id
     * @param targetName   execution target
     * @param state        initial state
     * @param createdAt    creation time
     * @return record builder
     */
    public static Builder builder(
            String submissionId,
            String targetName,
            ApplicationState state,
            Instant createdAt) {
        return new Builder(submissionId, targetName, state, createdAt);
    }

    public String getSubmissionId() {
        return submissionId;
    }

    public String getTargetName() {
        return targetName;
    }

    public String getAppName() {
        return appName;
    }

    /**
     * @return Spark master, if the record has one
     */
    public Optional<String> getMaster() {
        return Optional.ofNullable(master);
    }

    /**
     * @return Spark deploy mode, if the record has one
     */
    public Optional<String> getDeployMode() {
        return Optional.ofNullable(deployMode);
    }

    /**
     * @return Kubernetes namespace used for driver pod lookup, if applicable
     */
    public Optional<String> getNamespace() {
        return Optional.ofNullable(namespace);
    }

    /**
     * @return kubeconfig file path used for driver pod lookup, if applicable
     */
    public Optional<String> getKubeconfig() {
        return Optional.ofNullable(kubeconfig);
    }

    /**
     * @return Kubernetes job configuration Secret name, if the submission uses one
     */
    public Optional<String> getKubernetesJobSecret() {
        return Optional.ofNullable(kubernetesJobSecret);
    }

    /**
     * @return key inside the Kubernetes job configuration Secret, if the submission uses one
     */
    public Optional<String> getKubernetesJobSecretKey() {
        return Optional.ofNullable(kubernetesJobSecretKey);
    }

    /**
     * @return true when the SDK created the Secret and is responsible for deleting it
     */
    public boolean isKubernetesJobSecretOwned() {
        return kubernetesJobSecretOwned;
    }

    /**
     * @return Spark application id, available once the cluster has assigned one
     */
    public Optional<String> getApplicationId() {
        return Optional.ofNullable(applicationId);
    }

    public ApplicationState getState() {
        return state;
    }

    /**
     * @return bounded, single-line error text for failed or lost submissions
     */
    public Optional<String> getError() {
        return Optional.ofNullable(error);
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    /**
     * @return time the final state was reached, empty for a non-final submission
     */
    public Optional<Instant> getFinishedAt() {
        return Optional.ofNullable(finishedAt);
    }

    /**
     * @return true when no further state transition is expected
     */
    public boolean isFinal() {
        return state.isFinal();
    }

    /**
     * @return true when the submission finished successfully
     */
    public boolean isSuccessful() {
        return state.isSuccessful();
    }

    /**
     * Return a copy with an updated state and application id.
     *
     * <p>The error text is cleared unless the new state is {@link ApplicationState#FAILED}. A
     * final record never changes. {@link ApplicationState#PENDING} can not be entered through a
     * transition.</p>
     *
     * @param newState         new state
     * @param newApplicationId newly discovered application id, or null to keep the current one
     * @param updatedAt        transition time
     * @return updated record, or this instance when already final
     */
    public SubmissionRecord withStatus(
            ApplicationState newState,
            String newApplicationId,
            Instant updatedAt) {
        if (newState == ApplicationState.PENDING) {
            throw new SubmissionException(
                    "PENDING can only be used when a submission record is created");
        }
        return transition(newState, newApplicationId, null, updatedAt, false);
    }

    /**
     * Return a copy in a failed or lost state with bounded error text.
     *
     * @param failureState     {@link ApplicationState#FAILED} or {@link ApplicationState#LOST}
     * @param errorMessage     error message; a generic message is used when blank
     * @param updatedAt        transition time
     * @return updated record, or this instance when already final
     */
    public SubmissionRecord withFailure(
            ApplicationState failureState, String errorMessage, Instant updatedAt) {
        return withFailure(failureState, errorMessage, null, updatedAt);
    }

    /**
     * Return a copy in a failed or lost state with bounded error text.
     *
     * @param failureState     {@link ApplicationState#FAILED} or {@link ApplicationState#LOST}
     * @param errorMessage     error message; a generic message is used when blank
     * @param newApplicationId newly discovered application id, or null to keep the current one
     * @param updatedAt        transition time
     * @return updated record, or this instance when already final
     */
    public SubmissionRecord withFailure(
            ApplicationState failureState,
            String errorMessage,
            String newApplicationId,
            Instant updatedAt) {
        if (failureState != ApplicationState.FAILED
                && failureState != ApplicationState.LOST) {
            throw new SubmissionException(
                    "failure state must be FAILED or LOST, got " + failureState);
        }
        String bounded = sanitizeError(errorMessage);
        if (bounded == null) {
            bounded = "submission finished with state " + failureState;
        }
        return transition(failureState, newApplicationId, bounded, updatedAt, true);
    }

    private SubmissionRecord transition(
            ApplicationState newState,
            String newApplicationId,
            String newError,
            Instant updatedAt,
            boolean failure) {
        Objects.requireNonNull(newState, "new state");
        Objects.requireNonNull(updatedAt, "transition time");
        if (isFinal()) {
            return this;
        }
        String nextApplicationId = trimToNull(newApplicationId);
        if (nextApplicationId == null) {
            nextApplicationId = applicationId;
        }
        Instant effectiveUpdatedAt = updatedAt.isBefore(this.updatedAt)
                ? this.updatedAt
                : updatedAt;
        String nextError;
        if (failure) {
            nextError = newError;
        } else if (newState == ApplicationState.FAILED) {
            nextError = error;
        } else {
            nextError = null;
        }
        Instant nextFinishedAt = newState.isFinal() ? effectiveUpdatedAt : null;
        return new Builder(submissionId, targetName, newState, createdAt)
                .appName(appName)
                .master(master)
                .deployMode(deployMode)
                .namespace(namespace)
                .kubeconfig(kubeconfig)
                .kubernetesJobSecret(kubernetesJobSecret)
                .kubernetesJobSecretKey(kubernetesJobSecretKey)
                .kubernetesJobSecretOwned(kubernetesJobSecretOwned)
                .applicationId(nextApplicationId)
                .error(nextError)
                .updatedAt(effectiveUpdatedAt)
                .finishedAt(nextFinishedAt)
                .build();
    }

    static String sanitizeError(String message) {
        if (message == null) {
            return null;
        }
        String cleaned = message.replace('\r', ' ').replace('\n', ' ').trim();
        if (cleaned.isEmpty()) {
            return null;
        }
        if (cleaned.length() > MAX_ERROR_LENGTH) {
            cleaned = cleaned.substring(0, MAX_ERROR_LENGTH - TRUNCATED_SUFFIX.length())
                    + TRUNCATED_SUFFIX;
        }
        return cleaned;
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SubmissionRecord)) {
            return false;
        }
        SubmissionRecord that = (SubmissionRecord) other;
        return submissionId.equals(that.submissionId)
                && targetName.equals(that.targetName)
                && appName.equals(that.appName)
                && Objects.equals(master, that.master)
                && Objects.equals(deployMode, that.deployMode)
                && Objects.equals(namespace, that.namespace)
                && Objects.equals(kubeconfig, that.kubeconfig)
                && Objects.equals(kubernetesJobSecret, that.kubernetesJobSecret)
                && Objects.equals(kubernetesJobSecretKey, that.kubernetesJobSecretKey)
                && kubernetesJobSecretOwned == that.kubernetesJobSecretOwned
                && Objects.equals(applicationId, that.applicationId)
                && state == that.state
                && Objects.equals(error, that.error)
                && createdAt.equals(that.createdAt)
                && updatedAt.equals(that.updatedAt)
                && Objects.equals(finishedAt, that.finishedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                submissionId,
                targetName,
                appName,
                master,
                deployMode,
                namespace,
                kubeconfig,
                kubernetesJobSecret,
                kubernetesJobSecretKey,
                kubernetesJobSecretOwned,
                applicationId,
                state,
                error,
                createdAt,
                updatedAt,
                finishedAt);
    }

    @Override
    public String toString() {
        return "SubmissionRecord{submissionId='" + submissionId
                + "', target='" + targetName
                + "', state=" + state
                + ", applicationId='" + (applicationId == null ? "" : applicationId)
                + "', updatedAt=" + updatedAt
                + '}';
    }

    /**
     * Builder for {@link SubmissionRecord}.
     */
    public static final class Builder {

        private final String submissionId;

        private final String targetName;

        private final ApplicationState state;

        private final Instant createdAt;

        private String appName;

        private String master;

        private String deployMode;

        private String namespace;

        private String kubeconfig;

        private String kubernetesJobSecret;

        private String kubernetesJobSecretKey;

        private boolean kubernetesJobSecretOwned;

        private String applicationId;

        private String error;

        private Instant updatedAt;

        private Instant finishedAt;

        private Builder(
                String submissionId,
                String targetName,
                ApplicationState state,
                Instant createdAt) {
            this.submissionId = SubmissionIds.requireValid(submissionId);
            this.targetName = requireText(targetName, "target name");
            this.state = Objects.requireNonNull(state, "application state");
            this.createdAt = Objects.requireNonNull(createdAt, "creation time");
        }

        public Builder appName(String appName) {
            this.appName = trimToNull(appName);
            return this;
        }

        public Builder master(String master) {
            this.master = trimToNull(master);
            return this;
        }

        public Builder deployMode(String deployMode) {
            this.deployMode = trimToNull(deployMode);
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = trimToNull(namespace);
            return this;
        }

        public Builder kubeconfig(String kubeconfig) {
            this.kubeconfig = trimToNull(kubeconfig);
            return this;
        }

        /**
         * Set the Kubernetes job configuration Secret metadata. Secret contents are never stored
         * in a record.
         *
         * @param secret Secret name
         * @return this builder
         */
        public Builder kubernetesJobSecret(String secret) {
            this.kubernetesJobSecret = trimToNull(secret);
            return this;
        }

        /**
         * @param secretKey key inside the Kubernetes job configuration Secret
         * @return this builder
         */
        public Builder kubernetesJobSecretKey(String secretKey) {
            this.kubernetesJobSecretKey = trimToNull(secretKey);
            return this;
        }

        /**
         * @param owned whether the SDK owns and must delete the Secret
         * @return this builder
         */
        public Builder kubernetesJobSecretOwned(boolean owned) {
            this.kubernetesJobSecretOwned = owned;
            return this;
        }

        public Builder applicationId(String applicationId) {
            this.applicationId = trimToNull(applicationId);
            return this;
        }

        public Builder error(String error) {
            this.error = sanitizeError(error);
            return this;
        }

        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder finishedAt(Instant finishedAt) {
            this.finishedAt = finishedAt;
            return this;
        }

        /**
         * Validate and create the record.
         *
         * @return immutable record
         */
        public SubmissionRecord build() {
            if (appName == null) {
                throw new SubmissionException("application name can not be empty");
            }
            if (kubernetesJobSecret == null
                    && (kubernetesJobSecretKey != null || kubernetesJobSecretOwned)) {
                throw new SubmissionException(
                        "a Kubernetes job Secret key or owner flag requires a Secret name");
            }
            Instant effectiveUpdatedAt = updatedAt == null ? createdAt : updatedAt;
            if (effectiveUpdatedAt.isBefore(createdAt)) {
                throw new SubmissionException("update time can not be before creation time");
            }
            Instant effectiveFinishedAt;
            if (state.isFinal()) {
                effectiveFinishedAt = finishedAt == null ? effectiveUpdatedAt : finishedAt;
                if (effectiveFinishedAt.isBefore(createdAt)) {
                    throw new SubmissionException(
                            "finish time can not be before creation time");
                }
            } else {
                if (finishedAt != null) {
                    throw new SubmissionException(
                            "a non-final submission can not have a finish time");
                }
                effectiveFinishedAt = null;
            }
            this.updatedAt = effectiveUpdatedAt;
            this.finishedAt = effectiveFinishedAt;
            return new SubmissionRecord(this);
        }

        private static String requireText(String value, String field) {
            if (value == null || value.trim().isEmpty()) {
                throw new SubmissionException(field + " can not be empty");
            }
            return value.trim();
        }

        private static String trimToNull(String value) {
            if (value == null || value.trim().isEmpty()) {
                return null;
            }
            return value.trim();
        }
    }
}
