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

/**
 * Lifecycle state of a submitted Spark application.
 *
 * <p>Most values mirror {@code org.apache.spark.launcher.SparkAppHandle.State} so that callers of
 * the submission SDK do not have to depend on the Spark launcher API. {@link #PENDING} is added
 * for the durable period between preparing a submission and starting its launcher.</p>
 */
public enum ApplicationState {

    /**
     * The submission has been prepared and persisted but its launcher has not started yet.
     */
    PENDING,

    /**
     * The launcher has not received any state from the application yet.
     */
    UNKNOWN,

    /**
     * The application has connected to the cluster.
     */
    CONNECTED,

    /**
     * The application has been submitted to the cluster.
     */
    SUBMITTED,

    /**
     * The application is running.
     */
    RUNNING,

    /**
     * The application finished successfully.
     */
    FINISHED,

    /**
     * The application failed.
     */
    FAILED,

    /**
     * The application was killed.
     */
    KILLED,

    /**
     * The launcher lost track of the application.
     */
    LOST;

    /**
     * Whether no further state transition is expected.
     *
     * @return true for {@link #FINISHED}, {@link #FAILED}, {@link #KILLED} and {@link #LOST}
     */
    public boolean isFinal() {
        return this == FINISHED || this == FAILED || this == KILLED || this == LOST;
    }

    /**
     * Whether the application completed successfully.
     *
     * @return true only for {@link #FINISHED}
     */
    public boolean isSuccessful() {
        return this == FINISHED;
    }
}
