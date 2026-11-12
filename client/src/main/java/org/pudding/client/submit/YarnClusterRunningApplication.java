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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks a YARN cluster submission through the ResourceManager REST API.
 *
 * <p>In cluster deploy mode spark-submit exits after handing the application to YARN and never
 * connects to the Spark launcher state protocol. The process exit therefore means "submitted",
 * not "finished". This handle reads the application id from the spark-submit output and polls
 * the ResourceManager until YARN reports a final state.</p>
 */
final class YarnClusterRunningApplication implements RunningApplication {

    private static final Logger LOGGER =
            Logger.getLogger(YarnClusterRunningApplication.class.getName());

    private static final long DEFAULT_POLL_MILLIS = 5000L;

    private static final int MISSING_STATUS_LIMIT = 12;

    private static final int STATUS_ERROR_LIMIT = 60;

    private final String targetName;

    private final Process process;

    private final YarnResourceManagerClient client;

    private final long pollMillis;

    private final BoundedLogBuffer logs = new BoundedLogBuffer();

    private final CountDownLatch finished = new CountDownLatch(1);

    private final AtomicBoolean cancelRequested = new AtomicBoolean();

    private final AtomicBoolean finalStateSet = new AtomicBoolean();

    private volatile ApplicationState state = ApplicationState.UNKNOWN;

    private volatile String applicationId;

    private volatile Throwable error;

    private YarnClusterRunningApplication(
            String targetName,
            Process process,
            YarnResourceManagerClient client,
            long pollMillis) {
        this.targetName = targetName;
        this.process = process;
        this.client = client;
        this.pollMillis = pollMillis;
        Thread monitor = new Thread(this::monitor, "pudding-yarn-monitor");
        monitor.setDaemon(true);
        monitor.start();
    }

    static RunningApplication start(
            Process process,
            String targetName,
            String resourceManagerUrl) {
        return start(process, targetName, resourceManagerUrl, DEFAULT_POLL_MILLIS);
    }

    static RunningApplication start(
            Process process,
            String targetName,
            String resourceManagerUrl,
            long pollMillis) {
        return new YarnClusterRunningApplication(
                Objects.requireNonNull(targetName, "target name"),
                Objects.requireNonNull(process, "spark-submit process"),
                new YarnResourceManagerClient(resourceManagerUrl),
                Math.max(10L, pollMillis));
    }

    @Override
    public String getTargetName() {
        return targetName;
    }

    @Override
    public Optional<String> getApplicationId() {
        return Optional.ofNullable(applicationId);
    }

    @Override
    public ApplicationState getState() {
        return state;
    }

    @Override
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    @Override
    public ApplicationLog getRecentLogs(int maxLines) {
        return logs.snapshot(maxLines);
    }

    @Override
    public SubmissionResult await() throws InterruptedException {
        finished.await();
        return new SubmissionResult(targetName, state, applicationId, error);
    }

    @Override
    public void cancel() {
        cancelRequested.set(true);
        String appId = applicationId;
        if (appId != null) {
            try {
                client.kill(appId);
                finish(ApplicationState.KILLED, null);
            } catch (RuntimeException e) {
                error = new SubmissionException(
                        "failed to kill YARN application " + appId + ": " + message(e), e);
                finish(ApplicationState.LOST, null);
            }
        } else {
            process.destroy();
            finish(ApplicationState.KILLED, null);
        }
    }

    private void monitor() {
        readOutput();
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = new SubmissionException(
                    "interrupted while waiting for spark-submit of target '" + targetName + "'", e);
            finish(ApplicationState.FAILED, null);
            return;
        }
        if (cancelRequested.get()) {
            finish(ApplicationState.KILLED, null);
            return;
        }
        if (exitCode != 0) {
            error = new SubmissionException(
                    "spark-submit for target '" + targetName + "' exited with code " + exitCode
                            + "; the application may not have been submitted");
            finish(ApplicationState.FAILED, null);
            return;
        }
        String appId = applicationId;
        if (appId == null) {
            error = new SubmissionException(
                    "spark-submit for target '" + targetName + "' exited without a YARN "
                            + "application id; ensure the YARN client logs are not disabled");
            finish(ApplicationState.LOST, null);
            return;
        }
        state = ApplicationState.SUBMITTED;
        trackYarn(appId);
    }

    private void trackYarn(String appId) {
        int missing = 0;
        int errors = 0;
        while (!cancelRequested.get()) {
            try {
                Optional<YarnApplicationStatus> status = client.status(appId);
                if (!status.isPresent()) {
                    missing++;
                    if (missing >= MISSING_STATUS_LIMIT) {
                        error = new SubmissionException(
                                "YARN ResourceManager does not report application " + appId
                                        + " after submission");
                        finish(ApplicationState.LOST, null);
                        return;
                    }
                } else {
                    missing = 0;
                    errors = 0;
                    if (applyStatus(status.get())) {
                        return;
                    }
                }
            } catch (RuntimeException e) {
                errors++;
                LOGGER.log(Level.WARNING, "failed to poll YARN application " + appId + " state", e);
                if (errors >= STATUS_ERROR_LIMIT) {
                    error = new SubmissionException(
                            "lost track of YARN application " + appId + ": " + message(e), e);
                    finish(ApplicationState.LOST, null);
                    return;
                }
            }
            try {
                Thread.sleep(pollMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                error = new SubmissionException(
                        "interrupted while tracking YARN application " + appId, e);
                finish(ApplicationState.LOST, null);
                return;
            }
        }
    }

    /**
     * @return true when the YARN status is final
     */
    private boolean applyStatus(YarnApplicationStatus status) {
        String yarnState = upper(status.getState());
        String finalStatus = upper(status.getFinalStatus());
        switch (yarnState) {
            case "NEW":
            case "NEW_SAVING":
            case "SUBMITTED":
            case "ACCEPTED":
                state = ApplicationState.SUBMITTED;
                return false;
            case "RUNNING":
                state = ApplicationState.RUNNING;
                return false;
            case "FINISHED":
                if ("SUCCEEDED".equals(finalStatus)) {
                    finish(ApplicationState.FINISHED, null);
                } else if ("KILLED".equals(finalStatus)) {
                    finish(ApplicationState.KILLED, null);
                } else {
                    error = failure("YARN application " + applicationId
                            + " finished with final status " + finalStatus, status);
                    finish(ApplicationState.FAILED, null);
                }
                return true;
            case "FAILED":
                error = failure("YARN application " + applicationId + " failed", status);
                finish(ApplicationState.FAILED, null);
                return true;
            case "KILLED":
                finish(ApplicationState.KILLED, null);
                return true;
            default:
                state = ApplicationState.SUBMITTED;
                return false;
        }
    }

    private SubmissionException failure(String message, YarnApplicationStatus status) {
        String diagnostics = trimToNull(status.getDiagnostics());
        if (diagnostics == null) {
            return new SubmissionException(message);
        }
        return new SubmissionException(message + ": " + bounded(diagnostics));
    }

    private void readOutput() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logs.append(line);
                SparkSubmitOutputParser.ParsedLine parsed = SparkSubmitOutputParser.parse(line);
                String parsedId = parsed.getApplicationId();
                if (parsedId != null) {
                    applicationId = parsedId;
                    state = ApplicationState.SUBMITTED;
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINE, "failed to read spark-submit output for " + targetName, e);
        }
    }

    private void finish(ApplicationState finalState, String failureMessage) {
        if (!finalStateSet.compareAndSet(false, true)) {
            return;
        }
        if (failureMessage != null && error == null) {
            error = new SubmissionException(failureMessage);
        }
        state = finalState;
        finished.countDown();
    }

    private static String message(Throwable error) {
        String message = error.getMessage();
        return message == null || message.trim().isEmpty()
                ? error.getClass().getSimpleName()
                : message.trim();
    }

    private static String bounded(String value) {
        String sanitized = value.replaceAll("\\p{Cntrl}", " ").trim();
        return sanitized.length() <= 500 ? sanitized : sanitized.substring(0, 497) + "...";
    }

    private static String upper(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
