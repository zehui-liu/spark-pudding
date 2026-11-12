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

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.pudding.client.submit.SubmissionException;

import java.nio.charset.StandardCharsets;

/**
 * Fabric8-backed {@link KubernetesJobSecretManager}.
 */
final class Fabric8JobSecretManager implements KubernetesJobSecretManager {

    @Override
    public void create(
            String master,
            String namespace,
            String kubeconfig,
            String secretName,
            String secretKey,
            String content) {
        String effectiveNamespace = Fabric8Clients.namespace(namespace);
        String name = requireName(secretName);
        String key = requireKey(secretKey);
        if (content == null) {
            throw new SubmissionException("job configuration content can not be null");
        }
        if (content.getBytes(StandardCharsets.UTF_8).length > MAX_CONTENT_BYTES) {
            throw new SubmissionException(
                    "job configuration is larger than " + MAX_CONTENT_BYTES
                            + " bytes; Kubernetes Secrets can not hold it");
        }

        KubernetesClient client = Fabric8Clients.create(
                master, effectiveNamespace, Fabric8Clients.trimToNull(kubeconfig));
        try {
            Secret secret = new SecretBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace(effectiveNamespace)
                    .endMetadata()
                    .addToStringData(key, content)
                    .build();
            client.secrets()
                    .inNamespace(effectiveNamespace)
                    .withName(name)
                    .create(secret);
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to create Kubernetes job Secret '" + name + "' in namespace '"
                            + effectiveNamespace + "': " + Fabric8Clients.message(e), e);
        } finally {
            closeQuietly(client);
        }
    }

    @Override
    public void delete(
            String master,
            String namespace,
            String kubeconfig,
            String secretName) {
        String effectiveNamespace = Fabric8Clients.namespace(namespace);
        String name = requireName(secretName);
        KubernetesClient client = Fabric8Clients.create(
                master, effectiveNamespace, Fabric8Clients.trimToNull(kubeconfig));
        try {
            client.secrets()
                    .inNamespace(effectiveNamespace)
                    .withName(name)
                    .delete();
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to delete Kubernetes job Secret '" + name + "' in namespace '"
                            + effectiveNamespace + "': " + Fabric8Clients.message(e), e);
        } finally {
            closeQuietly(client);
        }
    }

    private static String requireName(String secretName) {
        String name = Fabric8Clients.trimToNull(secretName);
        if (name == null) {
            throw new SubmissionException("Kubernetes job Secret name can not be empty");
        }
        return name;
    }

    private static String requireKey(String secretKey) {
        String key = Fabric8Clients.trimToNull(secretKey);
        if (key == null) {
            throw new SubmissionException("Kubernetes job Secret key can not be empty");
        }
        return key;
    }

    private static void closeQuietly(KubernetesClient client) {
        try {
            client.close();
        } catch (RuntimeException ignored) {
            // The operation result is more important than a client close failure.
        }
    }
}
