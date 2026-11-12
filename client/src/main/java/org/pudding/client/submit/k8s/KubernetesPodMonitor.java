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

import org.pudding.client.submit.ApplicationLog;
import org.pudding.client.submit.SubmissionPlan;

import java.util.Optional;

/**
 * Finds and deletes the Spark driver pod for a submission.
 */
public interface KubernetesPodMonitor extends AutoCloseable {

    /**
     * Label added to driver pods for this submission.
     */
    String SUBMISSION_LABEL = "pudding-submission-id";

    /**
     * Create the default monitor backed by the Kubernetes API.
     *
     * @param plan resolved submission plan
     * @return pod monitor
     */
    static KubernetesPodMonitor create(SubmissionPlan plan) {
        return Fabric8PodMonitor.create(plan);
    }

    /**
     * Create a monitor for an existing submission, for example after a client restart.
     *
     * @param master     Kubernetes master, or null to use the kubeconfig/current context
     * @param namespace  driver pod namespace, or null for {@code default}
     * @param kubeconfig path to a kubeconfig file, or null for the standard lookup
     * @return pod monitor
     */
    static KubernetesPodMonitor create(String master, String namespace, String kubeconfig) {
        return Fabric8PodMonitor.create(master, namespace, kubeconfig);
    }

    /**
     * @param submissionId unique submission id
     * @return current driver pod status, or empty before the pod exists
     */
    Optional<DriverPodStatus> findDriverPod(String submissionId);

    /**
     * Delete the driver pod. Executors owned by the driver are removed by Kubernetes garbage
     * collection.
     *
     * @param submissionId unique submission id
     * @return true when at least one pod was deleted
     */
    boolean deleteDriverPod(String submissionId);

    /**
     * Read recent driver container log lines through the Kubernetes API.
     *
     * <p>The default returns an empty result for monitor implementations without log access.</p>
     *
     * @param submissionId unique submission id
     * @param maxLines     maximum number of trailing lines
     * @return log snapshot, or empty when the driver pod or its logs are not available
     */
    default Optional<ApplicationLog> getDriverLogs(String submissionId, int maxLines) {
        return Optional.empty();
    }

    /**
     * Release API client resources.
     */
    @Override
    void close();
}
