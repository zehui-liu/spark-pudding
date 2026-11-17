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

package org.pudding.client.submit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link RunningApplication} that derives the result from the waiting spark-submit process.
 *
 * <p>Kubernetes cluster submissions do not connect to the Spark launcher state protocol, so a
 * {@code SparkAppHandle} ends in {@link ApplicationState#LOST} even when the driver pod succeeds.
 * Spark also exits spark-submit with status zero when the driver pod fails, so the process exit
 * code alone is not sufficient. The final driver pod phase, which Spark writes to the submission
 * output, is used instead.</p>
 */
final class ProcessRunningApplication implements RunningApplication {

    private static final String MONITOR_THREAD_NAME = "pudding-spark-submit-monitor";

    private static final String LOGGER_NAME_PREFIX = "org.pudding.client.submit.spark";

    private static final String PHASE_SUCCEEDED = "Succeeded";

    private static final String PHASE_FAILED = "Failed";

    private final String targetName;

    private final Process process;

    private final Logger logger;

    private final BoundedLogBuffer logs = new BoundedLogBuffer();

    private final CountDownLatch finished = new CountDownLatch(1);

    private final AtomicBoolean cancelRequested = new AtomicBoolean();

    private volatile ApplicationState state = ApplicationState.RUNNING;

    private volatile String applicationId;

    private volatile String finalPhase;

    private volatile Throwable error;

    private ProcessRunningApplication(String targetName, Process process) {
        this.targetName = targetName;
        this.process = process;
        this.logger = Logger.getLogger(LOGGER_NAME_PREFIX + "." + targetName);
        Thread monitor = new Thread(this::monitor, MONITOR_THREAD_NAME);
        monitor.setDaemon(true);
        monitor.start();
    }

    static RunningApplication monitor(Process process, String targetName) {
        return new ProcessRunningApplication(
                Objects.requireNonNull(targetName, "target name"),
                Objects.requireNonNull(process, "process"));
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
        process.destroy();
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
            state = ApplicationState.FAILED;
            finished.countDown();
            return;
        }

        if (cancelRequested.get()) {
            state = ApplicationState.KILLED;
        } else if (PHASE_FAILED.equals(finalPhase)) {
            error = new SubmissionException(
                    "kubernetes application for target '" + targetName
                            + "' finished with phase " + PHASE_FAILED);
            state = ApplicationState.FAILED;
        } else if (PHASE_SUCCEEDED.equals(finalPhase)) {
            if (exitCode == 0) {
                state = ApplicationState.FINISHED;
            } else {
                error = exitError(exitCode);
                state = ApplicationState.FAILED;
            }
        } else if (exitCode != 0) {
            error = exitError(exitCode);
            state = ApplicationState.FAILED;
        } else if (TargetNames.KUBERNETES.equals(targetName)) {
            // In Kubernetes spark-submit may exit 0 even when the driver failed; the
            // final pod phase is required. Local processes have no such phase.
            error = new SubmissionException(
                    "could not determine the final phase of the kubernetes application for target '"
                            + targetName + "'");
            state = ApplicationState.LOST;
        } else {
            state = ApplicationState.FINISHED;
        }
        finished.countDown();
    }

    private void readOutput() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logs.append(line);
                logger.info(line);
                parse(line);
            }
        } catch (IOException e) {
            logger.log(Level.FINE, "failed to read spark-submit output", e);
        }
    }

    private void parse(String line) {
        SparkSubmitOutputParser.ParsedLine parsed = SparkSubmitOutputParser.parse(line);
        if (parsed.getApplicationId() != null) {
            applicationId = parsed.getApplicationId();
        }
        if (parsed.getPhase() != null) {
            recordPhase(parsed.getPhase());
        }
    }

    private void recordPhase(String phase) {
        if (PHASE_SUCCEEDED.equals(phase) || PHASE_FAILED.equals(phase)) {
            finalPhase = phase;
        }
    }

    private SubmissionException exitError(int exitCode) {
        return new SubmissionException(
                "spark-submit for target '" + targetName + "' exited with code " + exitCode);
    }
}
