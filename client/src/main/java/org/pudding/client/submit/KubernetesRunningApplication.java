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

import org.pudding.client.submit.k8s.DriverPodStatus;
import org.pudding.client.submit.k8s.KubernetesPodMonitor;

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
 * {@link RunningApplication} that tracks the Spark driver pod through the Kubernetes API.
 *
 * <p>The driver pod carries a unique submission label, so status lookup does not depend on
 * spark-submit output. spark-submit output is still logged and used as a fallback when the API
 * is unavailable. Cancellation deletes the driver pod; Kubernetes garbage collection removes
 * executors owned by it.</p>
 */
public final class KubernetesRunningApplication implements RunningApplication {

    private static final String LOGGER_NAME = "org.pudding.client.submit.kubernetes";

    private static final String PHASE_RUNNING = "Running";

    private static final long DEFAULT_POLL_MILLIS = 2000L;

    private static final long DEFAULT_FINAL_GRACE_MILLIS = 10000L;

    private static final long DEFAULT_CANCEL_TIMEOUT_MILLIS = 30000L;

    private static final long CANCEL_RETRY_MILLIS = 200L;

    private static final long OUTPUT_JOIN_MILLIS = 1000L;

    private final String targetName;

    private final String submissionId;

    private final Process process;

    private final KubernetesPodMonitor podMonitor;

    private final BoundedLogBuffer logs = new BoundedLogBuffer();

    private final long pollMillis;

    private final long finalGraceMillis;

    private final long cancelTimeoutMillis;

    private final Logger logger;

    private final CountDownLatch finished = new CountDownLatch(1);

    private final AtomicBoolean finalized = new AtomicBoolean();

    private final AtomicBoolean cancelRequested = new AtomicBoolean();

    private final AtomicBoolean apiWarningLogged = new AtomicBoolean();

    private volatile ApplicationState state = ApplicationState.SUBMITTED;

    private volatile String applicationId;

    private volatile Throwable error;

    private volatile String outputFinalPhase;

    private volatile long cancelSince;

    private volatile Thread outputThread;

    private volatile Thread cancelThread;

    private KubernetesRunningApplication(
            Process process,
            String targetName,
            String submissionId,
            KubernetesPodMonitor podMonitor,
            long pollMillis,
            long finalGraceMillis,
            long cancelTimeoutMillis) {
        this.process = Objects.requireNonNull(process, "process");
        this.targetName = Objects.requireNonNull(targetName, "target name");
        this.submissionId = Objects.requireNonNull(submissionId, "submission id");
        this.podMonitor = Objects.requireNonNull(podMonitor, "pod monitor");
        this.pollMillis = pollMillis;
        this.finalGraceMillis = finalGraceMillis;
        this.cancelTimeoutMillis = cancelTimeoutMillis;
        this.logger = Logger.getLogger(LOGGER_NAME + "." + targetName);
    }

    /**
     * Start tracking a launched spark-submit process.
     *
     * @param process      launched spark-submit process
     * @param targetName   target name
     * @param submissionId unique submission id used as the driver pod label
     * @param podMonitor   Kubernetes pod monitor
     * @return running application handle
     */
    public static RunningApplication start(
            Process process,
            String targetName,
            String submissionId,
            KubernetesPodMonitor podMonitor) {
        return start(
                process,
                targetName,
                submissionId,
                podMonitor,
                DEFAULT_POLL_MILLIS,
                DEFAULT_FINAL_GRACE_MILLIS,
                DEFAULT_CANCEL_TIMEOUT_MILLIS);
    }

    static RunningApplication start(
            Process process,
            String targetName,
            String submissionId,
            KubernetesPodMonitor podMonitor,
            long pollMillis,
            long finalGraceMillis,
            long cancelTimeoutMillis) {
        if (pollMillis <= 0 || finalGraceMillis <= 0 || cancelTimeoutMillis <= 0) {
            throw new SubmissionException("kubernetes lifecycle timings must be positive");
        }
        KubernetesRunningApplication application = new KubernetesRunningApplication(
                process,
                targetName,
                submissionId,
                podMonitor,
                pollMillis,
                finalGraceMillis,
                cancelTimeoutMillis);
        application.startThreads();
        return application;
    }

    private void startThreads() {
        outputThread = new Thread(this::readOutput, "pudding-k8s-output");
        outputThread.setDaemon(true);
        outputThread.start();

        Thread monitor = new Thread(this::monitor, "pudding-k8s-monitor");
        monitor.setDaemon(true);
        monitor.start();
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
        int limit = ApplicationLog.requirePositive(maxLines);
        try {
            Optional<ApplicationLog> driverLogs = podMonitor.getDriverLogs(submissionId, limit);
            if (driverLogs != null && driverLogs.isPresent()) {
                return driverLogs.get();
            }
        } catch (RuntimeException e) {
            logger.log(
                    Level.FINE,
                    "failed to read kubernetes driver logs for submission " + submissionId,
                    e);
        }
        return logs.snapshot(limit);
    }

    @Override
    public SubmissionResult await() throws InterruptedException {
        finished.await();
        return new SubmissionResult(targetName, state, applicationId, error);
    }

    @Override
    public void cancel() {
        if (!cancelRequested.compareAndSet(false, true)) {
            return;
        }
        cancelSince = System.currentTimeMillis();
        process.destroy();

        Thread thread = new Thread(this::deleteDriverPod, "pudding-k8s-cancel");
        thread.setDaemon(true);
        cancelThread = thread;
        thread.start();
    }

    private void readOutput() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logs.append(line);
                logger.info(line);
                recordOutput(SparkSubmitOutputParser.parse(line));
            }
        } catch (IOException e) {
            logger.log(Level.FINE, "failed to read spark-submit output", e);
        }
    }

    private void recordOutput(SparkSubmitOutputParser.ParsedLine parsed) {
        if (parsed.getApplicationId() != null) {
            applicationId = parsed.getApplicationId();
        }
        String phase = parsed.getPhase();
        if (DriverPodStatus.PHASE_SUCCEEDED.equals(phase)
                || DriverPodStatus.PHASE_FAILED.equals(phase)) {
            outputFinalPhase = phase;
        }
    }

    private void monitor() {
        long processExitAt = 0L;
        long podFinalAt = 0L;
        String finalPodPhase = null;
        String finalPodName = null;
        boolean exited = false;
        boolean outputDrained = false;
        int exitCode = -1;

        try {
            while (true) {
                long now = System.currentTimeMillis();

                if (!exited) {
                    try {
                        exitCode = process.exitValue();
                        exited = true;
                        processExitAt = now;
                    } catch (IllegalThreadStateException ignored) {
                        // spark-submit is still running
                    }
                }
                if (exited && !outputDrained) {
                    awaitOutputReader();
                    outputDrained = true;
                }

                DriverPodStatus pod = pollPod();
                if (pod != null) {
                    pod.getApplicationId().ifPresent(id -> applicationId = id);
                    if (pod.isFinal()) {
                        finalPodPhase = pod.getPhase();
                        finalPodName = pod.getName();
                        if (podFinalAt == 0L) {
                            podFinalAt = now;
                        }
                    } else {
                        updateRunningState(pod.getPhase());
                    }
                }

                if (cancelRequested.get()) {
                    long started = cancelSince == 0L ? now : cancelSince;
                    if (cancelSince == 0L) {
                        cancelSince = now;
                    }
                    if (exited) {
                        finishCanceled();
                        break;
                    }
                    if (now - started >= cancelTimeoutMillis) {
                        process.destroyForcibly();
                        finishCanceled();
                        break;
                    }
                } else if (exited && finalPodPhase != null) {
                    finishFromPod(finalPodPhase, finalPodName, exitCode);
                    break;
                } else if (exited) {
                    if (now - processExitAt >= finalGraceMillis) {
                        finishFromOutput(exitCode);
                        break;
                    }
                } else if (finalPodPhase != null
                        && now - podFinalAt >= finalGraceMillis) {
                    // The driver pod finished but spark-submit has not exited. The pod is the
                    // source of truth; stop waiting for the launcher process.
                    process.destroy();
                    finishFromPod(finalPodPhase, finalPodName, 0);
                    break;
                }

                Thread.sleep(pollMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finish(
                    ApplicationState.FAILED,
                    new SubmissionException(
                            "interrupted while waiting for the kubernetes application for target '"
                                    + targetName + "'", e));
        } catch (RuntimeException e) {
            finish(
                    ApplicationState.FAILED,
                    new SubmissionException(
                            "failed to monitor the kubernetes application for target '"
                                    + targetName + "': " + message(e), e));
        } finally {
            interruptCancelThread();
            closePodMonitor();
        }
    }

    private DriverPodStatus pollPod() {
        try {
            return podMonitor.findDriverPod(submissionId).orElse(null);
        } catch (RuntimeException e) {
            if (apiWarningLogged.compareAndSet(false, true)) {
                logger.warning(
                        "Kubernetes API status lookup failed for submission " + submissionId
                                + "; falling back to spark-submit output for the final state");
            }
            logger.log(
                    Level.FINE,
                    "Kubernetes API status lookup failed for submission " + submissionId, e);
            return null;
        }
    }

    private void finishCanceled() {
        if (deletePodOnCancel()) {
            finish(ApplicationState.KILLED, null);
        } else {
            finish(
                    ApplicationState.FAILED,
                    new SubmissionException(
                            "cancellation was requested, but deletion of the kubernetes driver "
                                    + "pod for submission '" + submissionId
                                    + "' could not be confirmed; the pod may still be running"));
        }
    }

    private boolean deletePodOnCancel() {
        try {
            return podMonitor.deleteDriverPod(submissionId);
        } catch (RuntimeException e) {
            logger.log(
                    Level.FINE,
                    "failed to delete kubernetes driver pod for submission " + submissionId,
                    e);
            return false;
        }
    }

    private void updateRunningState(String phase) {
        if (PHASE_RUNNING.equals(phase)) {
            state = ApplicationState.RUNNING;
        }
    }

    private void finishFromPod(String phase, String podName, int exitCode) {
        if (DriverPodStatus.PHASE_SUCCEEDED.equals(phase)) {
            if (exitCode != 0 && exitCode != -1) {
                finish(ApplicationState.FAILED, exitError(exitCode));
            } else {
                finish(ApplicationState.FINISHED, null);
            }
            return;
        }
        String pod = podName == null ? "" : " (driver pod " + podName + ")";
        finish(
                ApplicationState.FAILED,
                new SubmissionException(
                        "kubernetes application for target '" + targetName
                                + "' finished with phase " + phase + pod));
    }

    private void finishFromOutput(int exitCode) {
        if (DriverPodStatus.PHASE_SUCCEEDED.equals(outputFinalPhase)) {
            if (exitCode == 0) {
                finish(ApplicationState.FINISHED, null);
            } else {
                finish(ApplicationState.FAILED, exitError(exitCode));
            }
            return;
        }
        if (DriverPodStatus.PHASE_FAILED.equals(outputFinalPhase)) {
            finish(
                    ApplicationState.FAILED,
                    new SubmissionException(
                            "kubernetes application for target '" + targetName
                                    + "' finished with phase " + DriverPodStatus.PHASE_FAILED));
            return;
        }
        if (exitCode != 0) {
            finish(ApplicationState.FAILED, exitError(exitCode));
            return;
        }
        finish(
                ApplicationState.LOST,
                new SubmissionException(
                        "could not determine the final phase of the kubernetes application "
                                + "for target '" + targetName + "'"));
    }

    private void finish(ApplicationState finalState, Throwable failure) {
        if (finalized.compareAndSet(false, true)) {
            state = finalState;
            error = failure;
            finished.countDown();
        }
    }

    private void deleteDriverPod() {
        long deadline = System.currentTimeMillis() + cancelTimeoutMillis;
        while (System.currentTimeMillis() < deadline && finished.getCount() > 0) {
            try {
                if (podMonitor.deleteDriverPod(submissionId)) {
                    return;
                }
            } catch (RuntimeException e) {
                logger.log(
                        Level.FINE,
                        "failed to delete kubernetes driver pod for submission " + submissionId,
                        e);
            }
            try {
                Thread.sleep(CANCEL_RETRY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (finished.getCount() > 0) {
            logger.warning(
                    "could not confirm deletion of the kubernetes driver pod for submission "
                            + submissionId);
        }
    }

    private void awaitOutputReader() {
        Thread thread = outputThread;
        if (thread == null) {
            return;
        }
        try {
            thread.join(OUTPUT_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void interruptCancelThread() {
        Thread thread = cancelThread;
        if (thread == null) {
            return;
        }
        thread.interrupt();
        try {
            thread.join(OUTPUT_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void closePodMonitor() {
        try {
            podMonitor.close();
        } catch (RuntimeException e) {
            logger.log(Level.FINE, "failed to close Kubernetes API client", e);
        }
    }

    private SubmissionException exitError(int exitCode) {
        return new SubmissionException(
                "spark-submit for target '" + targetName + "' exited with code " + exitCode);
    }

    private static String message(RuntimeException e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }
}
