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

import java.util.regex.Pattern;

/**
 * Validation for submission ids that are used as durable file names.
 */
final class SubmissionIds {

    private static final Pattern VALID =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private SubmissionIds() {
    }

    static String requireValid(String submissionId) {
        if (submissionId == null || submissionId.trim().isEmpty()) {
            throw new SubmissionException("submission id can not be empty");
        }
        String id = submissionId.trim();
        if (!VALID.matcher(id).matches()) {
            throw new SubmissionException(
                    "invalid submission id; use 1 to 128 letters, digits, '.', '_' or '-' "
                            + "and start with a letter or digit");
        }
        return id;
    }
}
