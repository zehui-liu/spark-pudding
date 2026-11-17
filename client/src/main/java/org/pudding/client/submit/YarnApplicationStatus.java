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
 * A snapshot of one YARN application returned by the ResourceManager REST API.
 */
final class YarnApplicationStatus {

    private final String state;

    private final String finalStatus;

    private final String diagnostics;

    YarnApplicationStatus(String state, String finalStatus, String diagnostics) {
        this.state = state;
        this.finalStatus = finalStatus;
        this.diagnostics = diagnostics;
    }

    String getState() {
        return state;
    }

    String getFinalStatus() {
        return finalStatus;
    }

    String getDiagnostics() {
        return diagnostics;
    }
}
