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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/**
 * Minimal YARN ResourceManager REST client for cluster lifecycle tracking.
 *
 * <p>Only the application status and state endpoints are used. Authentication is inherited from
 * the endpoint (normally an internal HTTP address); Kerberos-protected ResourceManagers need an
 * authenticated gateway or a later security integration.</p>
 */
final class YarnResourceManagerClient {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final String baseUrl;

    private final HttpClient http;

    private final ObjectMapper mapper = new ObjectMapper();

    YarnResourceManagerClient(String baseUrl) {
        String normalized = baseUrl == null ? "" : baseUrl.trim();
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        this.baseUrl = normalized;
        this.http = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
    }

    Optional<YarnApplicationStatus> status(String applicationId) {
        URI uri = URI.create(this.baseUrl + "/ws/v1/cluster/apps/" + applicationId);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = this.http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubmissionException(
                    "interrupted while reading YARN application " + applicationId + " state", e);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to read YARN application " + applicationId + " state: "
                            + e.getClass().getSimpleName(), e);
        }
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        if (response.statusCode() / 100 != 2) {
            throw new SubmissionException(
                    "YARN ResourceManager returned HTTP " + response.statusCode()
                            + " for application " + applicationId);
        }
        try {
            JsonNode app = this.mapper.readTree(response.body()).path("app");
            if (app.isMissingNode() || app.isNull()) {
                throw new SubmissionException(
                        "YARN ResourceManager response has no app object for application "
                                + applicationId);
            }
            return Optional.of(new YarnApplicationStatus(
                    text(app, "state"),
                    text(app, "finalStatus"),
                    text(app, "diagnostics")));
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to parse YARN application " + applicationId + " state response", e);
        }
    }

    void kill(String applicationId) {
        URI uri = URI.create(this.baseUrl + "/ws/v1/cluster/apps/" + applicationId + "/state");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"state\":\"KILLED\"}"))
                .build();
        try {
            HttpResponse<String> response = this.http.send(
                    request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new SubmissionException(
                        "YARN ResourceManager returned HTTP " + response.statusCode()
                                + " while killing application " + applicationId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubmissionException(
                    "interrupted while killing YARN application " + applicationId, e);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to kill YARN application " + applicationId + ": "
                            + e.getClass().getSimpleName(), e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? "" : value.asText("");
    }
}
