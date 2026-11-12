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

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.pudding.client.submit.SubmissionException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Shared Fabric8 client construction for lifecycle tracking and job Secret management.
 */
final class Fabric8Clients {

    static final String DEFAULT_NAMESPACE = "default";

    private static final String MASTER_PREFIX = "k8s://";

    private static final int CONNECT_TIMEOUT_MILLIS = 5000;

    private static final int REQUEST_TIMEOUT_MILLIS = 5000;

    private Fabric8Clients() {
    }

    static String namespace(String namespace) {
        return trimToNull(namespace) == null ? DEFAULT_NAMESPACE : namespace.trim();
    }

    static KubernetesClient create(String master, String namespace, String kubeconfig) {
        Config config = clientConfig(master, namespace(namespace), trimToNull(kubeconfig));
        try {
            return new DefaultKubernetesClient(config);
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to create Kubernetes API client: " + message(e), e);
        }
    }

    private static Config clientConfig(String master, String namespace, String kubeconfig) {
        Config config;
        try {
            if (kubeconfig != null) {
                config = fromKubeconfig(kubeconfig);
            } else {
                config = Config.autoConfigure(null);
            }
        } catch (SubmissionException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to load Kubernetes client configuration: " + message(e), e);
        }
        if (config == null) {
            throw new SubmissionException(
                    "no Kubernetes client configuration found; set KUBECONFIG or kubeconfig(...)");
        }

        config.setNamespace(namespace);
        config.setConnectionTimeout(CONNECT_TIMEOUT_MILLIS);
        config.setRequestTimeout(REQUEST_TIMEOUT_MILLIS);

        String effectiveMaster = trimToNull(master);
        if (effectiveMaster != null) {
            config.setMasterUrl(
                    effectiveMaster.startsWith(MASTER_PREFIX)
                            ? effectiveMaster.substring(MASTER_PREFIX.length())
                            : effectiveMaster);
        }
        return config;
    }

    private static Config fromKubeconfig(String kubeconfig) {
        if (kubeconfig.contains(File.pathSeparator)) {
            throw new SubmissionException(
                    "Kubernetes lifecycle tracking requires a single kubeconfig file; "
                            + "KUBECONFIG contains multiple paths");
        }
        Path path = Paths.get(kubeconfig).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path)) {
            throw new SubmissionException("kubeconfig file does not exist: " + path);
        }
        try {
            String contents = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            return Config.fromKubeconfig(null, contents, path.toString());
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to read kubeconfig file: " + path, e);
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to parse kubeconfig file: " + path + " (" + message(e) + ")", e);
        }
    }

    static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    static String message(RuntimeException e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }
}
