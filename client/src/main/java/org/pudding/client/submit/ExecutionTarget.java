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

/**
 * A Spark execution target such as local, YARN or Kubernetes.
 *
 * <p>Targets perform two steps:</p>
 * <ol>
 *     <li>{@link #prepare(SubmissionRequest)} resolves a target-neutral request into an immutable
 *     {@link SubmissionPlan};</li>
 *     <li>{@link #submit(SubmissionPlan)} starts the Spark application asynchronously and returns
 *     a handle.</li>
 * </ol>
 *
 * <p>Implementations must be safe to use from multiple threads. {@link #prepare(SubmissionRequest)}
 * must not start an application.</p>
 */
public interface ExecutionTarget {

    /**
     * @return unique target name, or an alias such as an Admin-managed target id
     */
    String getName();

    /**
     * Return the built-in target type used by submission plans and durable records.
     *
     * <p>The default derives the type from {@link #getName()}. Named targets that expose an
     * arbitrary alias must override this method or be wrapped in
     * {@link NamedExecutionTarget}.</p>
     *
     * @return canonical target type
     */
    default String getTargetType() {
        return TargetNames.normalize(getName());
    }

    /**
     * Wrap a target under a stable name without changing its canonical submission type.
     *
     * @param name target alias; must match {@code [a-z0-9][a-z0-9-]*}
     * @param target target implementation
     * @return named target
     */
    static ExecutionTarget named(String name, ExecutionTarget target) {
        return new NamedExecutionTarget(name, target);
    }

    /**
     * Resolve a request into the effective Spark submission plan.
     *
     * @param request submission request
     * @return submission plan
     * @throws SubmissionException if the request is invalid for this target
     */
    SubmissionPlan prepare(SubmissionRequest request);

    /**
     * Start an application described by a plan prepared by this target.
     *
     * @param plan submission plan
     * @return running application handle
     * @throws SubmissionException if the application cannot be started
     */
    RunningApplication submit(SubmissionPlan plan);
}
