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

import org.pudding.client.submit.k8s.KubernetesPodMonitor;

/**
 * Creates Kubernetes pod monitors for restored submissions.
 */
@FunctionalInterface
public interface KubernetesMonitorFactory {

    /**
     * @param master     Kubernetes master, or null for the kubeconfig/current context
     * @param namespace  driver pod namespace, or null for {@code default}
     * @param kubeconfig kubeconfig path, or null for the standard lookup
     * @return pod monitor
     */
    KubernetesPodMonitor create(String master, String namespace, String kubeconfig);

    /**
     * @return factory backed by the Kubernetes API client
     */
    static KubernetesMonitorFactory defaultFactory() {
        return new KubernetesMonitorFactory() {
            @Override
            public KubernetesPodMonitor create(
                    String master, String namespace, String kubeconfig) {
                return KubernetesPodMonitor.create(master, namespace, kubeconfig);
            }
        };
    }
}
