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

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * {@link RunningApplication} backed by a Spark launcher application handle.
 *
 * <p>The launcher captures the spark-submit process output through a dedicated JUL logger. A
 * handler on that logger keeps a bounded in-memory log snapshot, so callers can read recent
 * driver output while the application is attached.</p>
 */
final class SparkRunningApplication implements RunningApplication, SparkAppHandle.Listener {

    private static final String CHILD_LOGGER_PREFIX =
            "org.pudding.client.submit.application.";

    private static final String CHILD_LOGGER_CONF = "spark.launcher.childProcLoggerName";

    private static final long LOG_DETACH_DELAY_MILLIS = 500L;

    private final String targetName;

    private final BoundedLogBuffer logs = new BoundedLogBuffer();

    private final CountDownLatch finished = new CountDownLatch(1);

    private final AtomicBoolean outputCaptureDetached = new AtomicBoolean();

    private volatile SparkAppHandle handle;

    private volatile ApplicationState state = ApplicationState.UNKNOWN;

    private volatile String applicationId;

    private volatile Throwable error;

    private volatile Logger outputLogger;

    private volatile Handler outputHandler;

    SparkRunningApplication(String targetName) {
        this.targetName = Objects.requireNonNull(targetName, "target name");
    }

    static RunningApplication start(
            SparkLauncher launcher, String targetName, String loggerSuffix) {
        SparkRunningApplication application = new SparkRunningApplication(targetName);
        application.attachOutputCapture(launcher, loggerSuffix);
        try {
            SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener[0]);
            application.attach(handle);
            return application;
        } catch (IOException e) {
            application.detachOutputCapture();
            throw new SubmissionException(
                    "failed to start Spark application for target '" + targetName
                            + "': " + e.getMessage(), e);
        }
    }

    private void attachOutputCapture(SparkLauncher launcher, String loggerSuffix) {
        String suffix = trimToNull(loggerSuffix);
        if (suffix == null) {
            suffix = UUID.randomUUID().toString();
        }
        String loggerName = CHILD_LOGGER_PREFIX + suffix;
        Logger logger = Logger.getLogger(loggerName);
        logger.setLevel(Level.INFO);
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record != null && record.getMessage() != null) {
                    logs.append(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        handler.setLevel(Level.ALL);
        logger.addHandler(handler);
        launcher.setConf(CHILD_LOGGER_CONF, loggerName);
        outputLogger = logger;
        outputHandler = handler;
    }

    void attach(SparkAppHandle appHandle) {
        if (appHandle == null) {
            throw new SubmissionException("Spark application handle can not be null");
        }
        this.handle = appHandle;
        appHandle.addListener(this);
        update(appHandle);
    }

    @Override
    public String getTargetName() {
        return targetName;
    }

    @Override
    public Optional<String> getApplicationId() {
        refresh();
        return Optional.ofNullable(applicationId);
    }

    @Override
    public ApplicationState getState() {
        refresh();
        return state;
    }

    @Override
    public Optional<Throwable> getError() {
        refresh();
        return Optional.ofNullable(error);
    }

    @Override
    public ApplicationLog getRecentLogs(int maxLines) {
        return logs.snapshot(maxLines);
    }

    @Override
    public SubmissionResult await() throws InterruptedException {
        finished.await();
        refresh();
        return new SubmissionResult(targetName, state, applicationId, error);
    }

    @Override
    public void cancel() {
        SparkAppHandle current = handle;
        if (current != null) {
            current.kill();
        }
    }

    @Override
    public void stateChanged(SparkAppHandle appHandle) {
        update(appHandle);
    }

    @Override
    public void infoChanged(SparkAppHandle appHandle) {
        update(appHandle);
    }

    private void refresh() {
        SparkAppHandle current = handle;
        if (current != null) {
            update(current);
        }
    }

    private void update(SparkAppHandle appHandle) {
        if (appHandle == null) {
            return;
        }
        ApplicationState newState = mapState(appHandle.getState());
        String appId = appHandle.getAppId();
        if (appId != null && !appId.trim().isEmpty()) {
            applicationId = appId;
        }
        Optional<Throwable> reportedError = appHandle.getError();
        if (reportedError != null && reportedError.isPresent()) {
            error = reportedError.get();
        }
        state = newState;
        if (newState.isFinal()) {
            finished.countDown();
            scheduleOutputCaptureDetach();
        }
    }

    private void scheduleOutputCaptureDetach() {
        if (!outputCaptureDetached.compareAndSet(false, true)) {
            return;
        }
        Thread detach = new Thread(() -> {
            try {
                Thread.sleep(LOG_DETACH_DELAY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                detachOutputCapture();
            }
        }, "pudding-log-detach");
        detach.setDaemon(true);
        detach.start();
    }

    private void detachOutputCapture() {
        Logger logger = outputLogger;
        Handler handler = outputHandler;
        if (logger != null && handler != null) {
            logger.removeHandler(handler);
        }
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static ApplicationState mapState(SparkAppHandle.State sparkState) {
        if (sparkState == null) {
            return ApplicationState.UNKNOWN;
        }
        switch (sparkState) {
            case CONNECTED:
                return ApplicationState.CONNECTED;
            case SUBMITTED:
                return ApplicationState.SUBMITTED;
            case RUNNING:
                return ApplicationState.RUNNING;
            case FINISHED:
                return ApplicationState.FINISHED;
            case FAILED:
                return ApplicationState.FAILED;
            case KILLED:
                return ApplicationState.KILLED;
            case LOST:
                return ApplicationState.LOST;
            default:
                return ApplicationState.UNKNOWN;
        }
    }
}
