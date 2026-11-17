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

import java.util.Objects;

/**
 * Entry point for submitting Spark jobs through an {@link ExecutionTarget}.
 *
 * <p>This class is stateless and safe to share between request-handling threads. A REST service
 * should call {@link #submit(ExecutionTarget, SubmissionRequest)} and persist the returned
 * application id instead of blocking a request thread in {@link RunningApplication#await()}.</p>
 */
public final class PuddingSubmitter {

    /**
     * Prepare a submission without starting an application.
     *
     * @param target  execution target
     * @param request submission request
     * @return resolved submission plan
     */
    public SubmissionPlan prepare(ExecutionTarget target, SubmissionRequest request) {
        Objects.requireNonNull(target, "execution target");
        Objects.requireNonNull(request, "submission request");
        return target.prepare(request);
    }

    /**
     * Prepare and start an application asynchronously.
     *
     * @param target  execution target
     * @param request submission request
     * @return running application handle
     */
    public RunningApplication submit(ExecutionTarget target, SubmissionRequest request) {
        return target.submit(prepare(target, request));
    }

    /**
     * Prepare, start and wait for an application to finish.
     *
     * @param target  execution target
     * @param request submission request
     * @return final submission result
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public SubmissionResult submitAndWait(ExecutionTarget target, SubmissionRequest request)
            throws InterruptedException {
        return submit(target, request).await();
    }
}
