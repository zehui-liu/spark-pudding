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

import java.util.Objects;

/**
 * Exposes a built-in execution target under a stable alias.
 *
 * <p>Submission plans and durable records keep the delegate's canonical target name. This lets a
 * manager register several YARN or Kubernetes clusters while preserving the existing record,
 * reattachment and Kubernetes Secret semantics.</p>
 */
public final class NamedExecutionTarget implements ExecutionTarget {

    private final String name;

    private final ExecutionTarget delegate;

    private final String targetType;

    /**
     * @param name alias matching {@code [a-z0-9][a-z0-9-]*}
     * @param delegate canonical execution target
     */
    public NamedExecutionTarget(String name, ExecutionTarget delegate) {
        this.name = TargetNames.requireName(name);
        this.delegate = Objects.requireNonNull(delegate, "execution target");
        this.targetType = Objects.requireNonNull(
                delegate.getTargetType(), "execution target type");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTargetType() {
        return targetType;
    }

    @Override
    public SubmissionPlan prepare(SubmissionRequest request) {
        return delegate.prepare(request);
    }

    @Override
    public RunningApplication submit(SubmissionPlan plan) {
        Objects.requireNonNull(plan, "submission plan");
        if (!TargetNames.normalize(plan.getTargetName()).equals(targetType)) {
            throw new SubmissionException(
                    "submission plan targets '" + plan.getTargetName()
                            + "' but named target '" + name + "' uses type '" + targetType + "'");
        }
        return delegate.submit(plan);
    }

    @Override
    public String toString() {
        return "NamedExecutionTarget[name=" + name + ", type=" + targetType + "]";
    }
}
