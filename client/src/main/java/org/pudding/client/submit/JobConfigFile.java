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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Writes a job configuration to a temporary file that only the submitting user can read.
 *
 * <p>The file is deleted when the application reaches a final state, or when the JVM exits.
 * Its content is never placed in the Spark command line.</p>
 */
final class JobConfigFile {

    private static final String FILE_NAME = "job.conf";

    private static final Set<Path> PENDING = ConcurrentHashMap.newKeySet();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> PENDING.forEach(SecureFiles::deleteRecursively),
                "pudding-job-config-shutdown"));
    }

    private JobConfigFile() {
    }

    /**
     * Create a temporary directory containing the job configuration at mode 0600.
     *
     * @param jobConfiguration job configuration
     * @return the job configuration file; its parent directory is the cleanup path
     */
    static Path create(JobConfiguration jobConfiguration) {
        Path directory = SecureFiles.createOwnerOnlyDirectory("pudding-job-");
        try {
            Path file = directory.resolve(FILE_NAME);
            SecureFiles.writeOwnerOnlyFile(
                    file,
                    jobConfiguration.getContent().getBytes(StandardCharsets.UTF_8));
            PENDING.add(directory);
            return file;
        } catch (RuntimeException e) {
            SecureFiles.deleteRecursively(directory);
            throw e;
        }
    }

    static void scheduleCleanup(RunningApplication application, List<Path> directories) {
        if (directories == null || directories.isEmpty()) {
            return;
        }
        PENDING.addAll(directories);
        Thread cleanup = new Thread(() -> {
            try {
                application.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                delete(directories);
            }
        }, "pudding-job-config-cleanup");
        cleanup.setDaemon(true);
        cleanup.start();
    }

    static void delete(Collection<Path> directories) {
        if (directories == null) {
            return;
        }
        for (Path directory : directories) {
            PENDING.remove(directory);
            SecureFiles.deleteRecursively(directory);
        }
    }
}
