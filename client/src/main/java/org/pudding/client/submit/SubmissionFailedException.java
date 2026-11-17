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
import java.util.Optional;

/**
 * A submission failed after its pending record was made durable.
 *
 * <p>The submission id identifies the persisted failed record, so a caller can show diagnostics
 * even when the launch request itself fails.</p>
 */
public class SubmissionFailedException extends SubmissionException {

    private static final long serialVersionUID = 1L;

    private final String submissionId;

    private final SubmissionRecord record;

    /**
     * Create a failed submission exception.
     *
     * @param submissionId persisted submission id
     * @param record       record snapshot at failure time, may be null
     * @param message      failure message
     * @param cause        underlying failure, may be null
     */
    public SubmissionFailedException(
            String submissionId,
            SubmissionRecord record,
            String message,
            Throwable cause) {
        super(message, cause);
        this.submissionId = Objects.requireNonNull(submissionId, "submission id");
        this.record = record;
    }

    /**
     * @return persisted submission id
     */
    public String getSubmissionId() {
        return submissionId;
    }

    /**
     * @return record snapshot at failure time, if available
     */
    public Optional<SubmissionRecord> getRecord() {
        return Optional.ofNullable(record);
    }
}
