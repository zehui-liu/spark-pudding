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

package org.pudding.client.submit.k8s;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.pudding.client.submit.ApplicationLog;
import org.pudding.client.submit.SubmissionException;
import org.pudding.client.submit.SubmissionPlan;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Kubernetes API pod monitor backed by the Fabric8 client.
 */
final class Fabric8PodMonitor implements KubernetesPodMonitor {

    private static final String NAMESPACE_KEY = "spark.kubernetes.namespace";

    private static final String KUBECONFIG_ENV = "KUBECONFIG";

    private static final String APP_ID_LABEL = "spark-app-selector";

    private static final String ROLE_LABEL = "spark-role";

    private static final String DRIVER_ROLE = "driver";

    private final KubernetesClient client;

    private final String namespace;

    private Fabric8PodMonitor(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    static KubernetesPodMonitor create(SubmissionPlan plan) {
        if (plan == null) {
            throw new SubmissionException("submission plan can not be null");
        }
        String namespace = Fabric8Clients.namespace(plan.getSparkConf().get(NAMESPACE_KEY));
        KubernetesClient client = Fabric8Clients.create(
                plan.getMaster(), namespace, trimToNull(plan.getEnvironment().get(KUBECONFIG_ENV)));
        return new Fabric8PodMonitor(client, namespace);
    }

    static KubernetesPodMonitor create(String master, String namespace, String kubeconfig) {
        String effectiveNamespace = Fabric8Clients.namespace(namespace);
        KubernetesClient client = Fabric8Clients.create(
                master, effectiveNamespace, Fabric8Clients.trimToNull(kubeconfig));
        return new Fabric8PodMonitor(client, effectiveNamespace);
    }

    @Override
    public Optional<DriverPodStatus> findDriverPod(String submissionId) {
        List<Pod> pods = driverPods(submissionId);
        if (pods.isEmpty()) {
            return Optional.empty();
        }
        Pod pod = pods.get(0);
        String name = pod.getMetadata() == null ? null : pod.getMetadata().getName();
        String phase = pod.getStatus() == null || pod.getStatus().getPhase() == null
                ? "Unknown"
                : pod.getStatus().getPhase();
        return Optional.of(new DriverPodStatus(name, phase, applicationId(pod)));
    }

    @Override
    public boolean deleteDriverPod(String submissionId) {
        List<Pod> pods = driverPods(submissionId);
        if (pods.isEmpty()) {
            // Deletion is idempotent: an already absent driver pod satisfies cancellation.
            return true;
        }
        boolean deleted = false;
        for (Pod pod : pods) {
            if (pod.getMetadata() == null || pod.getMetadata().getName() == null) {
                continue;
            }
            Boolean result = client.pods()
                    .inNamespace(namespace)
                    .withName(pod.getMetadata().getName())
                    .delete();
            deleted = deleted || Boolean.TRUE.equals(result);
        }
        return deleted;
    }

    @Override
    public Optional<ApplicationLog> getDriverLogs(String submissionId, int maxLines) {
        ApplicationLog.requirePositive(maxLines);
        List<Pod> pods = driverPods(submissionId);
        if (pods.isEmpty()) {
            return Optional.empty();
        }
        Pod pod = pods.get(0);
        String name = pod.getMetadata() == null ? null : pod.getMetadata().getName();
        String container = driverContainer(pod);
        if (name == null || container == null) {
            return Optional.empty();
        }
        try {
            // Request one extra line so ApplicationLog.tail can tell whether older lines exist.
            int requested = maxLines == Integer.MAX_VALUE ? maxLines : maxLines + 1;
            String text = client.pods()
                    .inNamespace(namespace)
                    .withName(name)
                    .inContainer(container)
                    .tailingLines(requested)
                    .getLog();
            return Optional.of(ApplicationLog.tail(text, maxLines));
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to read Kubernetes driver logs for submission " + submissionId
                            + ": " + message(e), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private static String driverContainer(Pod pod) {
        if (pod.getSpec() == null || pod.getSpec().getContainers() == null
                || pod.getSpec().getContainers().isEmpty()) {
            return null;
        }
        Container container = pod.getSpec().getContainers().get(0);
        return container == null ? null : trimToNull(container.getName());
    }

    private List<Pod> driverPods(String submissionId) {
        if (submissionId == null || submissionId.trim().isEmpty()) {
            throw new SubmissionException("kubernetes submission id can not be empty");
        }
        PodList list = client.pods()
                .inNamespace(namespace)
                .withLabel(SUBMISSION_LABEL, submissionId)
                .withLabel(ROLE_LABEL, DRIVER_ROLE)
                .list();
        return list.getItems();
    }

    private static String applicationId(Pod pod) {
        Map<String, String> labels = pod.getMetadata() == null
                ? null
                : pod.getMetadata().getLabels();
        return labels == null ? null : trimToNull(labels.get(APP_ID_LABEL));
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static String message(RuntimeException e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }
}
