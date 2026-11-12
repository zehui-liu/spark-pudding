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

package org.pudding.client.submit.k8s;

import java.util.Optional;

/**
 * Snapshot of the Spark driver pod reported by the Kubernetes API.
 */
public final class DriverPodStatus {

    /**
     * Pod succeeded phase.
     */
    public static final String PHASE_SUCCEEDED = "Succeeded";

    /**
     * Pod failed phase.
     */
    public static final String PHASE_FAILED = "Failed";

    private final String name;

    private final String phase;

    private final String applicationId;

    /**
     * @param name          pod name
     * @param phase         Kubernetes pod phase
     * @param applicationId Spark application id, if the pod exposes it
     */
    public DriverPodStatus(String name, String phase, String applicationId) {
        this.name = name;
        this.phase = phase;
        this.applicationId = applicationId;
    }

    /**
     * @return pod name
     */
    public String getName() {
        return name;
    }

    /**
     * @return Kubernetes pod phase, or {@code Unknown} when it is absent
     */
    public String getPhase() {
        return phase;
    }

    /**
     * @return Spark application id from the pod labels, if present
     */
    public Optional<String> getApplicationId() {
        return Optional.ofNullable(applicationId);
    }

    /**
     * @return true when the pod reached {@code Succeeded} or {@code Failed}
     */
    public boolean isFinal() {
        return isSucceeded() || isFailed();
    }

    /**
     * @return true when the pod succeeded
     */
    public boolean isSucceeded() {
        return PHASE_SUCCEEDED.equals(phase);
    }

    /**
     * @return true when the pod failed
     */
    public boolean isFailed() {
        return PHASE_FAILED.equals(phase);
    }
}
