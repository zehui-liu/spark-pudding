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

import java.util.Optional;

/**
 * A handle to an application that has been submitted to an execution target.
 *
 * <p>All methods are safe to call from multiple threads. {@link #await()} may be called more than
 * once; it returns immediately after the application has reached a final state.</p>
 */
public interface RunningApplication {

    /**
     * @return target that owns the application
     */
    String getTargetName();

    /**
     * @return application id, available once the cluster has assigned one
     */
    Optional<String> getApplicationId();

    /**
     * @return current application state
     */
    ApplicationState getState();

    /**
     * @return submission or runtime error reported by the launcher, if any
     */
    Optional<Throwable> getError();

    /**
     * Return recent application log lines. Implementations return an empty snapshot when logs
     * are not available, for example for a YARN cluster driver after the client has exited.
     *
     * @param maxLines maximum number of trailing lines
     * @return immutable log snapshot
     */
    default ApplicationLog getRecentLogs(int maxLines) {
        ApplicationLog.requirePositive(maxLines);
        return ApplicationLog.empty();
    }

    /**
     * Wait until the application reaches a final state.
     *
     * @return final result
     * @throws InterruptedException if the waiting thread is interrupted
     */
    SubmissionResult await() throws InterruptedException;

    /**
     * Request cancellation of the application. This method is best effort and does not wait for
     * the final state.
     */
    void cancel();
}
