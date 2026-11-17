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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * File-system helpers for data that must only be readable by the submitting user.
 */
final class SecureFiles {

    private static final Set<PosixFilePermission> FILE_PERMISSIONS =
            PosixFilePermissions.fromString("rw-------");

    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS =
            PosixFilePermissions.fromString("rwx------");

    private static final boolean POSIX =
            FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private SecureFiles() {
    }

    static Path createOwnerOnlyDirectory(String prefix) {
        try {
            Path directory = POSIX
                    ? Files.createTempDirectory(
                            prefix,
                            PosixFilePermissions.asFileAttribute(DIRECTORY_PERMISSIONS))
                    : Files.createTempDirectory(prefix);
            setDirectoryPermissions(directory);
            return directory;
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to create temporary directory for " + prefix, e);
        }
    }

    static Path writeOwnerOnlyFile(Path path, byte[] content) {
        try {
            if (POSIX) {
                Files.createFile(path, PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS));
            } else {
                Files.createFile(path);
            }
            Files.write(path, content);
            setFilePermissions(path);
            return path;
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to write temporary file: " + path.getFileName(), e);
        }
    }

    static void setFilePermissions(Path path) {
        setPermissions(path, FILE_PERMISSIONS);
    }

    static void setDirectoryPermissions(Path path) {
        setPermissions(path, DIRECTORY_PERMISSIONS);
    }

    private static void setPermissions(Path path, Set<PosixFilePermission> permissions) {
        if (!POSIX) {
            return;
        }
        try {
            Files.setPosixFilePermissions(path, permissions);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to restrict permissions on temporary path: "
                            + path.getFileName(), e);
        }
    }

    static void deleteRecursively(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(path)) {
            paths.sorted(Comparator.reverseOrder()).forEach(candidate -> {
                try {
                    Files.deleteIfExists(candidate);
                } catch (IOException ignored) {
                    // A later shutdown cleanup or the operating system removes the remainder.
                }
            });
        } catch (IOException ignored) {
            // Best-effort cleanup; callers must not fail a completed submission for this.
        }
    }
}
