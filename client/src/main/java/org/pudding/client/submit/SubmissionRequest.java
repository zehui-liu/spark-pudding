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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A submission request independent of any execution target.
 *
 * <p>Create one instance and reuse it when preparing or submitting the same job. The request is
 * immutable; use {@link #builder(JobConfiguration)} to construct it.</p>
 *
 * <p>Kubernetes targets can use a job configuration Secret supplied by the target, supplied by
 * this request, or generated and owned by the SDK. A managed Secret is created by
 * {@link SubmissionManager} after the pending record is durable and before the launcher starts.
 * The job configuration content itself is never part of a request description or a durable
 * record.</p>
 */
public final class SubmissionRequest {

    private final JobConfiguration job;

    private final String submissionId;

    private final Map<String, String> environment;

    private final String jobSecret;

    private final String jobSecretKey;

    private final boolean managedJobSecret;

    private final boolean verbose;

    private SubmissionRequest(
            JobConfiguration job,
            String submissionId,
            Map<String, String> environment,
            String jobSecret,
            String jobSecretKey,
            boolean managedJobSecret,
            boolean verbose) {
        this.job = job;
        this.submissionId = submissionId;
        this.environment = Collections.unmodifiableMap(new LinkedHashMap<>(environment));
        this.jobSecret = jobSecret;
        this.jobSecretKey = jobSecretKey;
        this.managedJobSecret = managedJobSecret;
        this.verbose = verbose;
    }

    /**
     * Create a builder for a job configuration.
     *
     * @param job job configuration
     * @return request builder
     */
    public static Builder builder(JobConfiguration job) {
        return new Builder(job);
    }

    /**
     * @return job configuration
     */
    public JobConfiguration getJob() {
        return job;
    }

    /**
     * @return caller-supplied submission id, if one was assigned
     */
    public Optional<String> getSubmissionId() {
        return Optional.ofNullable(submissionId);
    }

    /**
     * @return environment variables passed to the Spark submission process
     */
    public Map<String, String> getEnvironment() {
        return environment;
    }

    /**
     * @return request-level Kubernetes job configuration Secret name, if supplied
     */
    public Optional<String> getJobSecret() {
        return Optional.ofNullable(jobSecret);
    }

    /**
     * @return request-level key inside the job configuration Secret, if supplied
     */
    public Optional<String> getJobSecretKey() {
        return Optional.ofNullable(jobSecretKey);
    }

    /**
     * @return true when the SDK should generate, create and delete a dedicated Secret
     */
    public boolean isManagedJobSecret() {
        return managedJobSecret;
    }

    /**
     * @return whether the launcher should print the generated Spark submit command
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Builder for {@link SubmissionRequest}.
     */
    public static final class Builder {

        private final JobConfiguration job;

        private String submissionId;

        private final Map<String, String> environment = new LinkedHashMap<>();

        private String jobSecret;

        private String jobSecretKey;

        private boolean managedJobSecret;

        private boolean verbose;

        private Builder(JobConfiguration job) {
            this.job = job;
        }

        /**
         * Assign the submission id instead of letting the target generate a UUID.
         *
         * <p>This lets a service persist ownership and idempotency metadata before launch while
         * still returning a queryable id when launch fails.</p>
         *
         * @param submissionId 1 to 128 letters, digits, '.', '_' or '-', starting with a letter
         *                     or digit
         * @return this builder
         */
        public Builder submissionId(String submissionId) {
            this.submissionId = SubmissionIds.requireValid(submissionId);
            return this;
        }

        /**
         * Replace the environment passed to the submission process.
         *
         * @param values environment variables, may be null
         * @return this builder
         */
        public Builder environment(Map<String, String> values) {
            environment.clear();
            if (values != null) {
                values.forEach(this::putEnvironment);
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
        public Builder putEnvironment(String key, String value) {
            if (key == null || key.trim().isEmpty()) {
                throw new SubmissionException("environment variable name can not be empty");
            }
            if (value == null) {
                throw new SubmissionException(
                        "environment variable '" + key + "' can not be null");
            }
            environment.put(key, value);
            return this;
        }

        /**
         * Use an existing Kubernetes Secret for the job configuration instead of the target-level
         * default. The caller owns the Secret; the SDK never deletes it.
         *
         * @param jobSecret Secret name
         * @return this builder
         */
        public Builder jobSecret(String jobSecret) {
            this.jobSecret = trimToNull(jobSecret);
            return this;
        }

        /**
         * Set the key inside the request-level or managed job configuration Secret. The default
         * is inherited from the Kubernetes target.
         *
         * @param jobSecretKey Secret key
         * @return this builder
         */
        public Builder jobSecretKey(String jobSecretKey) {
            this.jobSecretKey = trimToNull(jobSecretKey);
            return this;
        }

        /**
         * Ask the SDK manager to generate, create and delete a dedicated Secret for this
         * submission. This mode requires {@link SubmissionManager}.
         *
         * @param managedJobSecret whether to use an SDK-managed Secret
         * @return this builder
         */
        public Builder managedJobSecret(boolean managedJobSecret) {
            this.managedJobSecret = managedJobSecret;
            return this;
        }

        /**
         * Ask the SDK manager to generate, create and delete a dedicated Secret for this
         * submission. This mode requires {@link SubmissionManager}.
         *
         * @return this builder
         */
        public Builder managedJobSecret() {
            return managedJobSecret(true);
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
         * @return immutable request
         */
        public SubmissionRequest build() {
            if (job == null) {
                throw new SubmissionException("job configuration can not be null");
            }
            if (managedJobSecret && jobSecret != null) {
                throw new SubmissionException(
                        "a managed job Secret can not be combined with an explicit job Secret");
            }
            if (jobSecretKey != null && jobSecret == null && !managedJobSecret) {
                throw new SubmissionException(
                        "a job Secret key requires a job Secret or managed job Secret mode");
            }
            return new SubmissionRequest(
                    job, submissionId, environment, jobSecret, jobSecretKey,
                    managedJobSecret, verbose);
        }

        private static String trimToNull(String value) {
            if (value == null || value.trim().isEmpty()) {
                return null;
            }
            return value.trim();
        }
    }
}
