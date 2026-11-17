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

/**
 * Creates and deletes Kubernetes Secrets that carry a job configuration.
 *
 * <p>Implementations must never log or otherwise expose the configuration content. The submitter
 * identity needs {@code get}, {@code create} and {@code delete} on {@code secrets} in the
 * submission namespace. The default implementation is backed by the Fabric8 client and uses the
 * same master, namespace and kubeconfig path as driver pod tracking.</p>
 */
public interface KubernetesJobSecretManager {

    /**
     * Largest job configuration accepted for a Kubernetes Secret. The wrapper script uses the
     * same limit to stay clear of the 1 MiB Secret object limit after base64 encoding.
     */
    int MAX_CONTENT_BYTES = 716800;

    /**
     * Create a Secret containing one job configuration value.
     *
     * @param master     Kubernetes master, or null to use the kubeconfig/current context
     * @param namespace  target namespace, or null for {@code default}
     * @param kubeconfig kubeconfig path, or null for the standard lookup
     * @param secretName Secret name
     * @param secretKey  key inside the Secret
     * @param content    job configuration; never logged
     * @throws RuntimeException if the Secret can not be created
     */
    void create(
            String master,
            String namespace,
            String kubeconfig,
            String secretName,
            String secretKey,
            String content);

    /**
     * Delete a Secret. Deleting a missing Secret is not an error.
     *
     * @param master     Kubernetes master, or null to use the kubeconfig/current context
     * @param namespace  target namespace, or null for {@code default}
     * @param kubeconfig kubeconfig path, or null for the standard lookup
     * @param secretName Secret name
     * @throws RuntimeException if the Secret can not be deleted
     */
    void delete(
            String master,
            String namespace,
            String kubeconfig,
            String secretName);

    /**
     * @return manager backed by the Fabric8 Kubernetes client
     */
    static KubernetesJobSecretManager defaultManager() {
        return new Fabric8JobSecretManager();
    }
}
