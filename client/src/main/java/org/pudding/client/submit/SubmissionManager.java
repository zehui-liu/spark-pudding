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

import org.pudding.client.submit.k8s.KubernetesJobSecretManager;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Submission lifecycle manager with durable records and Kubernetes reattachment.
 *
 * <p>The manager persists a {@link SubmissionRecord} before launching each application and
 * updates it from the attached application. On construction it reloads all records. Kubernetes
 * submissions that have not reached a final state are reattached through the driver pod label
 * (using the master, namespace and kubeconfig path stored in the record). Local and YARN
 * submissions can not be reattached, because their launcher lives in the previous client
 * process, so they are marked {@link ApplicationState#LOST}.</p>
 *
 * <p>The manager is intended for a long-lived service such as a REST backend. A daemon watcher
 * refreshes every submission periodically. All methods are safe to call from multiple
 * threads.</p>
 *
 * <p>Durable records never include job configuration, Spark configuration values, environment
 * values or kubeconfig contents.</p>
 */
public final class SubmissionManager implements AutoCloseable {

    private static final Logger LOGGER =
            Logger.getLogger("org.pudding.client.submit.manager");

    private static final long DEFAULT_POLL_MILLIS = 2000L;

    private static final long DEFAULT_REATTACH_GRACE_MILLIS = 30000L;

    private static final long CLOSE_WAIT_MILLIS = 5000L;

    private static final long SECRET_CLEANUP_RETRY_MILLIS = 30000L;

    private static final String NAMESPACE_CONF = "spark.kubernetes.namespace";

    private static final String KUBECONFIG_ENV = "KUBECONFIG";

    private static final String DEFAULT_NAMESPACE = "default";

    private static final String NON_KUBERNETES_LOST_MESSAGE =
            "the submission launcher is no longer attached after the client restarted; "
                    + "the final state is unknown";

    private volatile Map<String, ExecutionTarget> targets;

    private final SubmissionStore store;

    private final KubernetesMonitorFactory monitorFactory;

    private final KubernetesJobSecretManager jobSecretManager;

    private final long pollMillis;

    private final long reattachGraceMillis;

    private final ConcurrentMap<String, ManagedSubmission> submissions =
            new ConcurrentHashMap<>();

    private final ExecutorService secretCleanupExecutor;

    private final Set<String> cleanedJobSecrets = ConcurrentHashMap.newKeySet();

    private final Set<String> scheduledJobSecrets = ConcurrentHashMap.newKeySet();

    private final Set<String> jobSecretCleanupWarnings = ConcurrentHashMap.newKeySet();

    private final ConcurrentMap<String, Long> jobSecretCleanupRetryAt =
            new ConcurrentHashMap<>();

    /**
     * Allows concurrent submissions but makes {@link #close()} wait for submissions that are
     * still being prepared or launched, so a launch can not slip past shutdown.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private final AtomicBoolean running = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final Thread watcher;

    private SubmissionManager(Builder builder) {
        this.targets = Collections.unmodifiableMap(
                new LinkedHashMap<>(builder.targets));
        this.store = Objects.requireNonNull(builder.store, "submission store");
        this.monitorFactory = Objects.requireNonNull(
                builder.monitorFactory, "Kubernetes monitor factory");
        this.jobSecretManager = Objects.requireNonNull(
                builder.jobSecretManager, "Kubernetes job Secret manager");
        this.pollMillis = builder.pollMillis;
        this.reattachGraceMillis = builder.reattachGraceMillis;
        this.secretCleanupExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pudding-secret-cleanup");
            thread.setDaemon(true);
            return thread;
        });
        restore();
        for (SubmissionRecord record : list()) {
            scheduleJobSecretCleanup(record);
        }
        if (builder.autoStartWatcher) {
            running.set(true);
            watcher = new Thread(this::watch, "pudding-submission-watcher");
            watcher.setDaemon(true);
            watcher.start();
        } else {
            watcher = null;
        }
    }

    /**
     * @return manager builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Submit through the only configured target.
     *
     * @param request submission request
     * @return managed submission
     */
    public ManagedSubmission submit(SubmissionRequest request) {
        lifecycleLock.readLock().lock();
        try {
            Map<String, ExecutionTarget> current = targets;
            if (current.size() != 1) {
                throw new SubmissionException(
                        "exactly one execution target must be configured to omit the target name; "
                                + "configured targets: " + current.keySet());
            }
            return submitWhileOpen(current.keySet().iterator().next(), request);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Prepare, persist and launch a submission.
     *
     * <p>The pending record is saved before the launcher starts. If the launcher fails, the
     * record is persisted as {@link ApplicationState#FAILED} and a
     * {@link SubmissionFailedException} carrying the submission id is thrown. The failed record
     * remains available from {@link #find(String)} and {@link #list()}.</p>
     *
     * @param targetName execution target name
     * @param request    submission request
     * @return managed submission
     */
    public ManagedSubmission submit(String targetName, SubmissionRequest request) {
        lifecycleLock.readLock().lock();
        try {
            return submitWhileOpen(targetName, request);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Atomically replace the available execution targets.
     *
     * <p>Active submissions and durable records are untouched. New submissions use the new set.
     * The update waits for in-flight submissions to finish launching and is rejected after
     * {@link #close()}.</p>
     *
     * @param updatedTargets complete replacement set; may be empty
     */
    public void updateTargets(Collection<? extends ExecutionTarget> updatedTargets) {
        Objects.requireNonNull(updatedTargets, "execution targets");
        Map<String, ExecutionTarget> replacement = validatedTargets(updatedTargets);
        lifecycleLock.writeLock().lock();
        try {
            if (closed.get()) {
                throw new SubmissionException("submission manager is closed");
            }
            this.targets = replacement;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private ManagedSubmission submitWhileOpen(String targetName, SubmissionRequest request) {
        if (closed.get()) {
            throw new SubmissionException("submission manager is closed");
        }
        Objects.requireNonNull(request, "submission request");
        ExecutionTarget target = target(targetName);
        SubmissionPlan plan = target.prepare(request);
        if (plan == null) {
            throw new SubmissionException(
                    "execution target '" + target.getName() + "' returned no submission plan");
        }
        String planTarget = TargetNames.normalize(plan.getTargetName());
        if (!planTarget.equals(TargetNames.normalize(target.getTargetType()))) {
            cleanup(plan);
            throw new SubmissionException(
                    "submission plan targets '" + planTarget + "' but execution target is '"
                            + target.getName() + "'");
        }

        String submissionId;
        try {
            submissionId = SubmissionIds.requireValid(plan.getSubmissionId());
        } catch (RuntimeException e) {
            cleanup(plan);
            throw e;
        }
        Instant now = Instant.now();
        SubmissionRecord pending;
        try {
            pending = pendingRecord(plan, planTarget, submissionId, now);
        } catch (RuntimeException e) {
            cleanup(plan);
            throw e;
        }

        ManagedSubmission managed = ManagedSubmission.launched(
                pending, store, monitorFactory, reattachGraceMillis,
                this::scheduleJobSecretCleanup);
        if (submissions.putIfAbsent(submissionId, managed) != null) {
            cleanup(plan);
            throw new SubmissionException(
                    "submission id is already managed: " + submissionId);
        }
        try {
            store.save(pending);
        } catch (RuntimeException e) {
            submissions.remove(submissionId, managed);
            cleanup(plan);
            throw e;
        }

        try {
            provisionManagedJobSecret(plan, request);
        } catch (RuntimeException e) {
            recordLaunchFailure(managed, e);
            String message = e instanceof SubmissionException
                    ? errorMessage(e)
                    : "failed to prepare the managed Kubernetes job Secret: "
                            + errorMessage(e);
            throw new SubmissionFailedException(
                    submissionId, managed.getRecord(), message, e);
        }

        RunningApplication application;
        try {
            application = target.submit(plan);
            if (application == null) {
                throw new SubmissionException(
                        "execution target '" + target.getName()
                                + "' returned no running application");
            }
        } catch (RuntimeException e) {
            recordLaunchFailure(managed, e);
            throw new SubmissionFailedException(
                    submissionId,
                    managed.getRecord(),
                    "failed to start submission " + submissionId + " for target '"
                            + target.getName() + "': " + errorMessage(e),
                    e);
        }

        try {
            managed.attach(application);
        } catch (RuntimeException e) {
            LOGGER.log(
                    Level.WARNING,
                    "submission " + submissionId
                            + " was started but its initial state could not be persisted; "
                            + "the watcher will retry",
                    e);
        }
        return managed;
    }

    /**
     * Refresh every managed submission and return the latest records.
     *
     * <p>Per-submission failures are logged and do not prevent other submissions from being
     * refreshed.</p>
     *
     * @return immutable record snapshot sorted by creation time
     */
    public List<SubmissionRecord> reconcile() {
        for (ManagedSubmission managed : new ArrayList<>(submissions.values())) {
            try {
                managed.refresh();
            } catch (RuntimeException e) {
                LOGGER.log(
                        Level.WARNING,
                        "failed to refresh submission " + managed.getSubmissionId()
                                + "; will retry: " + e.getClass().getSimpleName(),
                        e);
            }
        }
        List<SubmissionRecord> records = list();
        for (SubmissionRecord record : records) {
            scheduleJobSecretCleanup(record);
        }
        return records;
    }

    /**
     * @param submissionId unique submission id
     * @return managed submission, or empty when it is not known
     */
    public Optional<ManagedSubmission> find(String submissionId) {
        String id = SubmissionIds.requireValid(submissionId);
        return Optional.ofNullable(submissions.get(id));
    }

    /**
     * @return immutable record snapshot sorted by creation time and submission id
     */
    public List<SubmissionRecord> list() {
        List<SubmissionRecord> records = new ArrayList<>();
        for (ManagedSubmission managed : submissions.values()) {
            records.add(managed.getRecord());
        }
        records.sort(Comparator
                .comparing(SubmissionRecord::getCreatedAt)
                .thenComparing(SubmissionRecord::getSubmissionId));
        return Collections.unmodifiableList(records);
    }

    /**
     * Release final records that ended before a retention cutoff.
     *
     * <p>This only releases the manager's in-memory handles and Kubernetes monitors. Record
     * deletion is the caller's responsibility. Active submissions are never removed.</p>
     *
     * @param cutoff exclusive finish-time cutoff
     * @return number of records released
     */
    public int pruneFinalBefore(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        int removed = 0;
        for (ManagedSubmission managed : new ArrayList<>(submissions.values())) {
            SubmissionRecord record = managed.getRecord();
            if (!record.isFinal()) {
                continue;
            }
            Instant endedAt = record.getFinishedAt().orElse(record.getUpdatedAt());
            if (endedAt != null
                    && endedAt.isBefore(cutoff)
                    && submissions.remove(record.getSubmissionId(), managed)) {
                managed.close();
                removed++;
            }
        }
        return removed;
    }

    /**
     * Stop the watcher and release Kubernetes API clients without cancelling applications.
     *
     * <p>Waiting submissions finish their preparation and launch before the manager closes; new
     * submissions are rejected afterwards.</p>
     */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            running.set(false);
            Thread thread = watcher;
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(CLOSE_WAIT_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            for (ManagedSubmission managed : submissions.values()) {
                managed.close();
            }
            secretCleanupExecutor.shutdown();
            try {
                secretCleanupExecutor.awaitTermination(CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                secretCleanupExecutor.shutdownNow();
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private void restore() {
        Instant now = Instant.now();
        for (SubmissionRecord stored : store.list()) {
            SubmissionRecord record = stored;
            if (!record.isFinal()) {
                if (!TargetNames.KUBERNETES.equals(record.getTargetName())) {
                    record = record.withFailure(
                            ApplicationState.LOST, NON_KUBERNETES_LOST_MESSAGE, now);
                    store.save(record);
                }
            }
            ManagedSubmission managed = ManagedSubmission.restored(
                    record, store, monitorFactory, reattachGraceMillis,
                    this::scheduleJobSecretCleanup, now);
            submissions.put(record.getSubmissionId(), managed);
        }
    }

    private void watch() {
        while (running.get()) {
            reconcile();
            if (!running.get()) {
                return;
            }
            try {
                Thread.sleep(pollMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private ExecutionTarget target(String targetName) {
        Map<String, ExecutionTarget> current = targets;
        if (targetName == null || targetName.trim().isEmpty()) {
            if (current.size() != 1) {
                throw new SubmissionException(
                        "execution target is required; configured targets: "
                                + current.keySet());
            }
            return current.values().iterator().next();
        }

        String requested = targetName.trim();
        ExecutionTarget exact = current.get(requested);
        if (exact != null) {
            return exact;
        }

        String canonical;
        try {
            canonical = TargetNames.normalize(requested);
        } catch (SubmissionException e) {
            throw unknownTarget(requested, current);
        }

        List<ExecutionTarget> matches = new ArrayList<>();
        for (ExecutionTarget candidate : current.values()) {
            if (canonical.equals(candidate.getTargetType())) {
                matches.add(candidate);
            }
        }
        if (matches.size() == 1) {
            return matches.get(0);
        }
        if (matches.size() > 1) {
            List<String> names = new ArrayList<>();
            for (ExecutionTarget match : matches) {
                names.add(match.getName());
            }
            throw new SubmissionException(
                    "execution target '" + requested + "' is ambiguous; "
                            + "configured targets of type '" + canonical + "': " + names);
        }
        throw unknownTarget(requested, current);
    }

    private static SubmissionException unknownTarget(
            String requested, Map<String, ExecutionTarget> configured) {
        return new SubmissionException(
                "execution target '" + requested + "' is not configured; configured targets: "
                        + configured.keySet());
    }

    private static SubmissionRecord pendingRecord(
            SubmissionPlan plan,
            String targetName,
            String submissionId,
            Instant now) {
        SubmissionRecord.Builder builder = SubmissionRecord.builder(
                        submissionId, targetName, ApplicationState.PENDING, now)
                .appName(plan.getAppName())
                .master(plan.getMaster())
                .deployMode(plan.getDeployMode())
                .updatedAt(now);
        if (TargetNames.KUBERNETES.equals(targetName)) {
            String namespace = trimToNull(plan.getSparkConf().get(NAMESPACE_CONF));
            builder.namespace(namespace == null ? DEFAULT_NAMESPACE : namespace)
                    .kubeconfig(plan.getEnvironment().get(KUBECONFIG_ENV))
                    .kubernetesJobSecret(plan.getJobSecret().orElse(null))
                    .kubernetesJobSecretKey(plan.getJobSecretKey().orElse(null))
                    .kubernetesJobSecretOwned(plan.isJobSecretOwned());
        }
        return builder.build();
    }

    private void provisionManagedJobSecret(SubmissionPlan plan, SubmissionRequest request) {
        if (!plan.isJobSecretOwned()) {
            return;
        }
        String name = plan.getJobSecret().orElseThrow(() -> new SubmissionException(
                "managed Kubernetes job Secret has no name"));
        String key = plan.getJobSecretKey().orElseThrow(() -> new SubmissionException(
                "managed Kubernetes job Secret has no key"));
        String namespace = trimToNull(plan.getSparkConf().get(NAMESPACE_CONF));
        if (namespace == null) {
            namespace = DEFAULT_NAMESPACE;
        }
        jobSecretManager.create(
                plan.getMaster(),
                namespace,
                trimToNull(plan.getEnvironment().get(KUBECONFIG_ENV)),
                name,
                key,
                request.getJob().getContent());
        LOGGER.info(
                "created managed Kubernetes job Secret '" + name + "' in namespace '"
                        + namespace + "' for submission " + plan.getSubmissionId());
    }

    private void scheduleJobSecretCleanup(SubmissionRecord record) {
        if (record == null
                || !record.isFinal()
                || !record.isKubernetesJobSecretOwned()
                || !record.getKubernetesJobSecret().isPresent()) {
            return;
        }
        String submissionId = record.getSubmissionId();
        if (cleanedJobSecrets.contains(submissionId)) {
            return;
        }
        Long retryAt = jobSecretCleanupRetryAt.get(submissionId);
        if (retryAt != null && System.currentTimeMillis() < retryAt) {
            return;
        }
        if (!scheduledJobSecrets.add(submissionId)) {
            return;
        }
        try {
            secretCleanupExecutor.execute(() -> cleanupJobSecret(record));
        } catch (RejectedExecutionException e) {
            scheduledJobSecrets.remove(submissionId);
        }
    }

    private void cleanupJobSecret(SubmissionRecord record) {
        String submissionId = record.getSubmissionId();
        String name = record.getKubernetesJobSecret().orElse(null);
        try {
            if (name == null) {
                return;
            }
            jobSecretManager.delete(
                    record.getMaster().orElse(null),
                    record.getNamespace().orElse(null),
                    record.getKubeconfig().orElse(null),
                    name);
            cleanedJobSecrets.add(submissionId);
            jobSecretCleanupWarnings.remove(submissionId);
            jobSecretCleanupRetryAt.remove(submissionId);
            LOGGER.info(
                    "deleted managed Kubernetes job Secret '" + name + "' for submission "
                            + submissionId);
        } catch (RuntimeException e) {
            jobSecretCleanupRetryAt.put(
                    submissionId, System.currentTimeMillis() + SECRET_CLEANUP_RETRY_MILLIS);
            if (jobSecretCleanupWarnings.add(submissionId)) {
                LOGGER.log(
                        Level.WARNING,
                        "failed to delete managed Kubernetes job Secret '" + name
                                + "' for submission " + submissionId + "; will retry",
                        e);
            } else {
                LOGGER.log(
                        Level.FINE,
                        "still unable to delete managed Kubernetes job Secret '"
                                + name + "' for submission " + submissionId,
                        e);
            }
        } finally {
            scheduledJobSecrets.remove(submissionId);
        }
    }

    private static void cleanup(SubmissionPlan plan) {
        JobConfigFile.delete(plan.getTemporaryPaths());
    }

    private static void recordLaunchFailure(ManagedSubmission managed, RuntimeException failure) {
        try {
            managed.failToStart(failure);
        } catch (RuntimeException persistenceFailure) {
            failure.addSuppressed(persistenceFailure);
            LOGGER.log(
                    Level.WARNING,
                    "submission " + managed.getSubmissionId()
                            + " failed to launch and its failed state could not be persisted",
                    persistenceFailure);
        }
    }

    private static Map<String, ExecutionTarget> validatedTargets(
            Collection<? extends ExecutionTarget> configured) {
        Map<String, ExecutionTarget> validated = new LinkedHashMap<>();
        for (ExecutionTarget target : configured) {
            Objects.requireNonNull(target, "execution target");
            String name = TargetNames.requireName(target.getName());
            if (validated.containsKey(name)) {
                throw new SubmissionException(
                        "execution target is configured more than once: " + name);
            }
            // Validate the canonical type early; getTargetType() rejects unsupported aliases.
            target.getTargetType();
            validated.put(name, target);
        }
        return Collections.unmodifiableMap(validated);
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static String errorMessage(Throwable error) {
        if (error == null) {
            return "unknown error";
        }
        String message = error.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return error.getClass().getSimpleName();
        }
        return message;
    }

    /**
     * Builder for {@link SubmissionManager}.
     */
    public static final class Builder {

        private final Map<String, ExecutionTarget> targets = new LinkedHashMap<>();

        private SubmissionStore store;

        private KubernetesMonitorFactory monitorFactory =
                KubernetesMonitorFactory.defaultFactory();

        private KubernetesJobSecretManager jobSecretManager =
                KubernetesJobSecretManager.defaultManager();

        private long pollMillis = DEFAULT_POLL_MILLIS;

        private long reattachGraceMillis = DEFAULT_REATTACH_GRACE_MILLIS;

        private boolean autoStartWatcher = true;

        private Builder() {
        }

        /**
         * Add an execution target. Targets are keyed by their unique name; multiple targets of
         * the same built-in type are allowed when they use distinct names.
         *
         * @param target execution target
         * @return this builder
         */
        public Builder target(ExecutionTarget target) {
            Objects.requireNonNull(target, "execution target");
            String name = TargetNames.requireName(target.getName());
            if (targets.containsKey(name)) {
                throw new SubmissionException(
                        "execution target is configured more than once: " + name);
            }
            // Validate the canonical type early; getTargetType() rejects unsupported aliases.
            target.getTargetType();
            targets.put(name, target);
            return this;
        }

        /**
         * Add execution targets.
         *
         * @param targets execution targets
         * @return this builder
         */
        public Builder targets(Collection<? extends ExecutionTarget> targets) {
            if (targets != null) {
                for (ExecutionTarget target : targets) {
                    target(target);
                }
            }
            return this;
        }

        /**
         * Set the durable store.
         *
         * @param store submission store
         * @return this builder
         */
        public Builder store(SubmissionStore store) {
            this.store = Objects.requireNonNull(store, "submission store");
            return this;
        }

        /**
         * Set the factory used to reattach Kubernetes submissions.
         *
         * @param monitorFactory monitor factory
         * @return this builder
         */
        public Builder monitorFactory(KubernetesMonitorFactory monitorFactory) {
            this.monitorFactory = Objects.requireNonNull(
                    monitorFactory, "Kubernetes monitor factory");
            return this;
        }

        /**
         * Set the manager used to create and delete per-submission Kubernetes job Secrets.
         *
         * @param jobSecretManager Kubernetes job Secret manager
         * @return this builder
         */
        public Builder jobSecretManager(KubernetesJobSecretManager jobSecretManager) {
            this.jobSecretManager = Objects.requireNonNull(
                    jobSecretManager, "Kubernetes job Secret manager");
            return this;
        }

        /**
         * Set the interval at which all submissions are refreshed.
         *
         * @param interval positive interval
         * @return this builder
         */
        public Builder pollInterval(Duration interval) {
            this.pollMillis = positiveMillis(interval, "poll interval");
            return this;
        }

        /**
         * Set how long a restored Kubernetes submission waits for its driver pod before it is
         * marked lost.
         *
         * @param grace positive reattachment grace period
         * @return this builder
         */
        public Builder reattachGrace(Duration grace) {
            this.reattachGraceMillis = positiveMillis(grace, "reattachment grace");
            return this;
        }

        Builder autoStartWatcher(boolean autoStartWatcher) {
            this.autoStartWatcher = autoStartWatcher;
            return this;
        }

        /**
         * Create the manager and reload durable submissions.
         *
         * @return submission manager
         */
        public SubmissionManager build() {
            if (store == null) {
                throw new SubmissionException("submission store is required");
            }
            return new SubmissionManager(this);
        }

        private static long positiveMillis(Duration duration, String field) {
            Objects.requireNonNull(duration, field);
            long millis = duration.toMillis();
            if (millis <= 0) {
                throw new SubmissionException(field + " must be at least one millisecond");
            }
            return millis;
        }
    }
}
