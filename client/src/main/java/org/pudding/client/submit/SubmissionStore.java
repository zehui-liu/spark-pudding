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

import java.util.List;
import java.util.Optional;

/**
 * Durable storage for {@link SubmissionRecord}s.
 *
 * <p>Implementations must store records atomically and must not add sensitive submission data.
 * {@link FileSubmissionStore} is the built-in local implementation. A service running on several
 * hosts should supply a store backed by its shared database.</p>
 */
public interface SubmissionStore {

    /**
     * Create or replace the record with the same submission id.
     *
     * @param record submission record
     */
    void save(SubmissionRecord record);

    /**
     * Load one record.
     *
     * @param submissionId unique submission id
     * @return record, or empty when it does not exist
     */
    Optional<SubmissionRecord> find(String submissionId);

    /**
     * Load all records sorted by creation time and then submission id.
     *
     * @return immutable record list
     */
    List<SubmissionRecord> list();

    /**
     * Delete one record. Deleting a missing record is not an error.
     *
     * @param submissionId unique submission id
     */
    void delete(String submissionId);
}
