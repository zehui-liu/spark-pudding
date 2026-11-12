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

import org.pudding.client.submit.k8s.DriverPodStatus;
import org.pudding.client.submit.k8s.KubernetesPodMonitor;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Durable submission handle returned by {@link SubmissionManager}.
 *
 * <p>The handle combines a persisted {@link SubmissionRecord} with the live tracking object. A
 * submission launched by this process follows its {@link RunningApplication}. A Kubernetes
 * submission restored after a client restart is reconciled through the Kubernetes API by its
 * submission label. Local and YARN submissions can not be reattached and are marked
 * {@link ApplicationState#LOST} by the manager.</p>
 *
 * <p>All state-changing operations are serialized. State is persisted before it is published to
 * callers, so a successful read never observes a newer state than the durable store.</p>
 */
public final class ManagedSubmission {

    private static final Logger LOGGER =
            Logger.getLogger("org.pudding.client.submit.manager");

    private static final String PHASE_RUNNING = "Running";

    private final SubmissionStore store;

    private final KubernetesMonitorFactory monitorFactory;

    private final long reattachGraceMillis;

    private final boolean reattachable;

    private final Consumer<SubmissionRecord> finalStateListener;

    private final Object lock = new Object();

    private SubmissionRecord record;

    private RunningApplication application;

    private KubernetesPodMonitor monitor;

    private Instant reattachDeadline;

    private Instant podMissingSince;

    private boolean podSeen;

    private boolean closed;

    private boolean monitorWarningLogged;

    private boolean apiWarningLogged;

    private ManagedSubmission(
            SubmissionRecord record,
            SubmissionStore store,
            KubernetesMonitorFactory monitorFactory,
            long reattachGraceMillis,
            boolean reattachable,
            Consumer<SubmissionRecord> finalStateListener,
            Instant now) {
        this.record = Objects.requireNonNull(record, "submission record");
        this.store = Objects.requireNonNull(store, "submission store");
        this.monitorFactory = Objects.requireNonNull(monitorFactory, "monitor factory");
        this.reattachGraceMillis = reattachGraceMillis;
        this.reattachable = reattachable;
        this.finalStateListener = Objects.requireNonNull(
                finalStateListener, "final state listener");
        this.reattachDeadline = now.plusMillis(reattachGraceMillis);
    }

    static ManagedSubmission launched(
            SubmissionRecord record,
            SubmissionStore store,
            KubernetesMonitorFactory monitorFactory,
            long reattachGraceMillis,
            Consumer<SubmissionRecord> finalStateListener) {
        return new ManagedSubmission(
                record,
                store,
                monitorFactory,
                reattachGraceMillis,
                false,
                finalStateListener,
                Instant.now());
    }

    static ManagedSubmission restored(
            SubmissionRecord record,
            SubmissionStore store,
            KubernetesMonitorFactory monitorFactory,
            long reattachGraceMillis,
            Consumer<SubmissionRecord> finalStateListener,
            Instant now) {
        boolean kubernetes = TargetNames.KUBERNETES.equals(record.getTargetName());
        return new ManagedSubmission(
                record,
                store,
                monitorFactory,
                reattachGraceMillis,
                kubernetes,
                finalStateListener,
                now);
    }

    /**
     * Attach the application returned by an execution target.
     *
     * @param runningApplication running application
     */
    void attach(RunningApplication runningApplication) {
        Objects.requireNonNull(runningApplication, "running application");
        synchronized (lock) {
            if (closed) {
                throw new SubmissionException(
                        "can not attach application to a closed submission manager");
            }
            this.application = runningApplication;
            applyAttachedState(Instant.now());
        }
    }

    /**
     * Persist a failure that happened before the launch process was started.
     *
     * @param failure launch failure
     */
    void failToStart(Throwable failure) {
        synchronized (lock) {
            if (record.isFinal()) {
                return;
            }
            SubmissionRecord candidate = record.withFailure(
                    ApplicationState.FAILED, errorMessage(failure), Instant.now());
            persist(candidate);
            notifyFinalState(candidate);
        }
    }

    public String getSubmissionId() {
        return getRecord().getSubmissionId();
    }

    public String getTargetName() {
        return getRecord().getTargetName();
    }

    public ApplicationState getState() {
        return getRecord().getState();
    }

    public boolean isFinal() {
        return getRecord().isFinal();
    }

    /**
     * @return immutable latest record snapshot
     */
    public SubmissionRecord getRecord() {
        synchronized (lock) {
            return record;
        }
    }

    /**
     * Poll the attached application or Kubernetes driver pod and persist the latest state.
     *
     * @return latest record snapshot
     */
    public SubmissionRecord refresh() {
        synchronized (lock) {
            if (closed || record.isFinal()) {
                return record;
            }
            if (application != null) {
                applyAttachedState(Instant.now());
            } else if (reattachable) {
                refreshKubernetesReattach(Instant.now());
            }
            return record;
        }
    }

    /**
     * Return recent application logs.
     *
     * <p>Attached applications use their own log source. Restored Kubernetes submissions, and
     * finished records whose driver pod still exists, read the driver pod through the Kubernetes
     * API. Other submissions return an empty snapshot.</p>
     *
     * @param maxLines maximum number of trailing lines
     * @return immutable log snapshot
     */
    public ApplicationLog getRecentLogs(int maxLines) {
        int limit = ApplicationLog.requirePositive(maxLines);
        synchronized (lock) {
            RunningApplication currentApplication = application;
            if (currentApplication != null) {
                return currentApplication.getRecentLogs(limit);
            }
            if (!TargetNames.KUBERNETES.equals(record.getTargetName())) {
                return ApplicationLog.empty();
            }
            KubernetesPodMonitor currentMonitor = monitor;
            boolean ephemeral = false;
            try {
                if (currentMonitor == null) {
                    currentMonitor = createMonitor();
                    ephemeral = true;
                }
                if (currentMonitor == null) {
                    return ApplicationLog.empty();
                }
                Optional<ApplicationLog> logs =
                        currentMonitor.getDriverLogs(record.getSubmissionId(), limit);
                return logs == null ? ApplicationLog.empty() : logs.orElse(ApplicationLog.empty());
            } catch (RuntimeException e) {
                LOGGER.log(
                        Level.FINE,
                        "failed to read logs for submission " + record.getSubmissionId(),
                        e);
                return ApplicationLog.empty();
            } finally {
                if (ephemeral) {
                    closeQuietly(currentMonitor);
                }
            }
        }
    }

    /**
     * Request cancellation.
     *
     * <p>An attached application is cancelled through its launcher. A restored Kubernetes
     * submission is cancelled by deleting its driver pod; the record is persisted as
     * {@link ApplicationState#KILLED} after deletion succeeds. Final submissions are ignored.</p>
     *
     * @throws SubmissionException if a restored Kubernetes driver pod can not be deleted
     */
    public void cancel() {
        synchronized (lock) {
            if (closed || record.isFinal()) {
                return;
            }
            if (application != null) {
                application.cancel();
                return;
            }
            if (!reattachable) {
                throw new SubmissionException(
                        "submission " + record.getSubmissionId()
                                + " has no attached launcher and can not be cancelled");
            }

            KubernetesPodMonitor currentMonitor = monitor;
            boolean ephemeral = false;
            if (currentMonitor == null) {
                currentMonitor = createMonitor();
                ephemeral = true;
            }
            if (currentMonitor == null) {
                throw new SubmissionException(
                        "can not cancel submission " + record.getSubmissionId()
                                + ": Kubernetes API is unavailable");
            }
            boolean deleted;
            try {
                deleted = currentMonitor.deleteDriverPod(record.getSubmissionId());
            } catch (RuntimeException e) {
                throw new SubmissionException(
                        "failed to delete the Kubernetes driver pod for submission "
                                + record.getSubmissionId() + ": " + errorMessage(e), e);
            } finally {
                if (ephemeral) {
                    closeQuietly(currentMonitor);
                }
            }
            if (!deleted) {
                throw new SubmissionException(
                        "no Kubernetes driver pod found for submission "
                                + record.getSubmissionId());
            }

            SubmissionRecord candidate = record.withStatus(
                    ApplicationState.KILLED, null, Instant.now());
            persist(candidate);
            notifyFinalState(candidate);
            closeMonitor();
            monitor = null;
            podSeen = false;
            podMissingSince = null;
        }
    }

    void close() {
        synchronized (lock) {
            closed = true;
            closeMonitor();
            monitor = null;
        }
    }

    private void applyAttachedState(Instant now) {
        ApplicationState state;
        String applicationId;
        String failure;
        try {
            state = application.getState();
            if (state == null) {
                return;
            }
            if (state == ApplicationState.PENDING) {
                state = ApplicationState.UNKNOWN;
            }
            applicationId = application.getApplicationId().orElse(null);
            Optional<Throwable> reported = application.getError();
            failure = reported != null && reported.isPresent()
                    ? errorMessage(reported.get())
                    : null;
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to read the state of submission " + record.getSubmissionId(), e);
        }
        boolean failed = state == ApplicationState.FAILED || state == ApplicationState.LOST;
        if (failed && failure == null) {
            failure = missingFailureMessage(state, applicationId);
        }
        applyState(state, applicationId, failed ? failure : null, now);
    }

    private String missingFailureMessage(ApplicationState state, String applicationId) {
        StringBuilder message = new StringBuilder("submission ")
                .append(record.getSubmissionId())
                .append(" on target '")
                .append(record.getTargetName())
                .append("' ended in state ")
                .append(state);
        String application = boundedContext(applicationId);
        if (application != null) {
            message.append(" (application ").append(application).append(')');
        }
        message.append("; no launcher error was reported; "
                + "see the persisted launcher logs for details");
        return message.toString();
    }

    private static String boundedContext(String value) {
        if (value == null) {
            return null;
        }
        String bounded = value.replaceAll("\\p{Cntrl}", " ").trim();
        if (bounded.isEmpty()) {
            return null;
        }
        return bounded.length() <= 120 ? bounded : bounded.substring(0, 117) + "...";
    }

    private void refreshKubernetesReattach(Instant now) {
        KubernetesPodMonitor currentMonitor = monitor;
        if (currentMonitor == null) {
            currentMonitor = ensureMonitor();
        }
        if (currentMonitor == null) {
            return;
        }

        Optional<DriverPodStatus> found;
        try {
            found = currentMonitor.findDriverPod(record.getSubmissionId());
        } catch (RuntimeException e) {
            if (apiWarningLogged) {
                LOGGER.log(
                        Level.FINE,
                        "Kubernetes status lookup still fails for submission "
                                + record.getSubmissionId(),
                        e);
            } else {
                apiWarningLogged = true;
                LOGGER.log(
                        Level.WARNING,
                        "Kubernetes status lookup failed for submission "
                                + record.getSubmissionId()
                                + "; keeping the last durable state",
                        e);
            }
            return;
        }
        apiWarningLogged = false;

        if (found == null || !found.isPresent()) {
            handleMissingDriverPod(now);
            return;
        }

        DriverPodStatus pod = found.get();
        podSeen = true;
        podMissingSince = null;
        String applicationId = pod.getApplicationId().orElse(null);
        if (pod.isSucceeded()) {
            applyState(ApplicationState.FINISHED, applicationId, null, now);
        } else if (pod.isFailed()) {
            String podName = pod.getName() == null ? "" : " " + pod.getName();
            applyState(
                    ApplicationState.FAILED,
                    applicationId,
                    "Kubernetes driver pod" + podName + " finished with phase Failed",
                    now);
        } else if (PHASE_RUNNING.equals(pod.getPhase())) {
            applyState(ApplicationState.RUNNING, applicationId, null, now);
        } else {
            applyState(ApplicationState.SUBMITTED, applicationId, null, now);
        }
    }

    private void handleMissingDriverPod(Instant now) {
        if (!podSeen) {
            if (!now.isBefore(reattachDeadline)) {
                markLost(
                        "no Kubernetes driver pod was found for submission "
                                + record.getSubmissionId() + " within "
                                + reattachGraceMillis + " ms of reattachment",
                        now);
            }
            return;
        }
        if (podMissingSince == null) {
            podMissingSince = now;
            return;
        }
        if (!now.isBefore(podMissingSince.plusMillis(reattachGraceMillis))) {
            markLost(
                    "the Kubernetes driver pod for submission "
                            + record.getSubmissionId() + " disappeared before a final phase",
                    now);
        }
    }

    private void markLost(String message, Instant now) {
        SubmissionRecord candidate = record.withFailure(ApplicationState.LOST, message, now);
        persist(candidate);
        notifyFinalState(candidate);
        closeMonitor();
        monitor = null;
    }

    private void applyState(
            ApplicationState newState,
            String applicationId,
            String error,
            Instant now) {
        String currentApplicationId = record.getApplicationId().orElse(null);
        String nextApplicationId = trimToNull(applicationId) == null
                ? currentApplicationId
                : applicationId.trim();
        boolean failure = newState == ApplicationState.FAILED
                || newState == ApplicationState.LOST;
        String nextError = failure ? SubmissionRecord.sanitizeError(error) : null;
        String currentError = record.getError().orElse(null);

        if (record.getState() == newState
                && Objects.equals(currentApplicationId, nextApplicationId)
                && Objects.equals(currentError, nextError)) {
            return;
        }

        SubmissionRecord candidate = failure
                ? record.withFailure(newState, error, applicationId, now)
                : record.withStatus(newState, applicationId, now);
        persist(candidate);
        if (candidate.isFinal()) {
            notifyFinalState(candidate);
            closeMonitor();
            monitor = null;
        }
    }

    private void notifyFinalState(SubmissionRecord candidate) {
        if (!candidate.isFinal()) {
            return;
        }
        try {
            finalStateListener.accept(candidate);
        } catch (RuntimeException e) {
            LOGGER.log(
                    Level.WARNING,
                    "failed to schedule cleanup for submission "
                            + candidate.getSubmissionId() + "; will retry",
                    e);
        }
    }

    private void persist(SubmissionRecord candidate) {
        if (candidate.equals(record)) {
            return;
        }
        store.save(candidate);
        record = candidate;
    }

    private KubernetesPodMonitor ensureMonitor() {
        if (monitor != null) {
            return monitor;
        }
        KubernetesPodMonitor created = createMonitor();
        if (created != null) {
            monitor = created;
            monitorWarningLogged = false;
        }
        return created;
    }

    private KubernetesPodMonitor createMonitor() {
        try {
            KubernetesPodMonitor created = monitorFactory.create(
                    record.getMaster().orElse(null),
                    record.getNamespace().orElse(null),
                    record.getKubeconfig().orElse(null));
            if (created == null) {
                throw new SubmissionException("Kubernetes monitor factory returned null");
            }
            return created;
        } catch (RuntimeException e) {
            if (monitorWarningLogged) {
                LOGGER.log(
                        Level.FINE,
                        "still unable to create Kubernetes monitor for submission "
                                + record.getSubmissionId(),
                        e);
            } else {
                monitorWarningLogged = true;
                LOGGER.log(
                        Level.WARNING,
                        "unable to create Kubernetes monitor for submission "
                                + record.getSubmissionId()
                                + "; will retry while reattaching",
                        e);
            }
            return null;
        }
    }

    private void closeMonitor() {
        KubernetesPodMonitor current = monitor;
        if (current != null) {
            closeQuietly(current);
        }
    }

    private static void closeQuietly(KubernetesPodMonitor podMonitor) {
        if (podMonitor == null) {
            return;
        }
        try {
            podMonitor.close();
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "failed to close Kubernetes monitor", e);
        }
    }

    private static String errorMessage(Throwable error) {
        if (error == null) {
            return null;
        }
        String message = error.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return error.getClass().getSimpleName();
        }
        return SubmissionRecord.sanitizeError(
                error.getClass().getSimpleName() + ": " + message);
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    @Override
    public String toString() {
        return "ManagedSubmission{" + getRecord() + '}';
    }
}
