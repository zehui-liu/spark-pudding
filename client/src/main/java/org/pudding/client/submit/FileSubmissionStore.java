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
import java.io.Reader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link SubmissionStore} that keeps one properties file per record.
 *
 * <p>The directory is restricted to the owner (mode 0700 on POSIX systems). Records are written
 * to an owner-only temporary file, forced to disk and moved into place atomically when the file
 * system supports it. Symbolic links inside the directory are rejected so a record can not be
 * redirected outside the store.</p>
 *
 * <p>Record files contain no job configuration, environment variable values, Spark configuration
 * values or kubeconfig contents.</p>
 */
public final class FileSubmissionStore implements SubmissionStore {

    private static final String FILE_SUFFIX = ".properties";

    private static final String TEMP_SUFFIX = ".tmp";

    private static final String FORMAT_VERSION = "1";

    private static final String FORMAT_KEY = "format.version";

    private static final String SUBMISSION_ID_KEY = "submission.id";

    private static final String TARGET_KEY = "target";

    private static final String APP_NAME_KEY = "application.name";

    private static final String MASTER_KEY = "master";

    private static final String DEPLOY_MODE_KEY = "deploy.mode";

    private static final String NAMESPACE_KEY = "kubernetes.namespace";

    private static final String KUBECONFIG_KEY = "kubernetes.kubeconfig";

    private static final String JOB_SECRET_NAME_KEY = "kubernetes.job.secret";

    private static final String JOB_SECRET_DATA_KEY = "kubernetes.job.secret.key";

    private static final String JOB_SECRET_OWNED_KEY = "kubernetes.job.secret.owned";

    private static final String APPLICATION_ID_KEY = "application.id";

    private static final String STATE_KEY = "state";

    private static final String ERROR_KEY = "error";

    private static final String CREATED_AT_KEY = "created.at";

    private static final String UPDATED_AT_KEY = "updated.at";

    private static final String FINISHED_AT_KEY = "finished.at";

    private static final Set<PosixFilePermission> FILE_PERMISSIONS =
            PosixFilePermissions.fromString("rw-------");

    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS =
            PosixFilePermissions.fromString("rwx------");

    private static final boolean POSIX =
            FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private final Path directory;

    /**
     * Open or create a store directory.
     *
     * @param directory directory for submission record files
     */
    public FileSubmissionStore(Path directory) {
        Objects.requireNonNull(directory, "store directory");
        this.directory = directory.toAbsolutePath().normalize();
        prepareDirectory();
    }

    /**
     * @return absolute store directory
     */
    public Path getDirectory() {
        return directory;
    }

    @Override
    public void save(SubmissionRecord record) {
        Objects.requireNonNull(record, "submission record");
        String id = record.getSubmissionId();
        Path target = recordPath(id);
        Path temporary = null;
        try {
            String contents = encode(record);
            byte[] bytes = contents.getBytes(StandardCharsets.UTF_8);
            temporary = POSIX
                    ? Files.createTempFile(
                            directory,
                            "." + id + "-",
                            TEMP_SUFFIX,
                            PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS))
                    : Files.createTempFile(directory, "." + id + "-", TEMP_SUFFIX);
            try (FileChannel channel = FileChannel.open(
                    temporary,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            restrictFile(temporary);
            moveIntoPlace(temporary, target);
            temporary = null;
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to save submission record '" + id + "' in " + directory + ": "
                            + e.getClass().getSimpleName(), e);
        } finally {
            deleteTemporary(temporary);
        }
    }

    @Override
    public Optional<SubmissionRecord> find(String submissionId) {
        String id = SubmissionIds.requireValid(submissionId);
        Path path = recordPath(id);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return Optional.empty();
        }
        requireRegularRecordFile(path);
        return Optional.of(read(path, id));
    }

    @Override
    public List<SubmissionRecord> list() {
        List<Path> paths;
        try (Stream<Path> stream = Files.list(directory)) {
            paths = stream
                    .filter(path -> path.getFileName().toString().endsWith(FILE_SUFFIX))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to list submission records in " + directory + ": "
                            + e.getClass().getSimpleName(), e);
        }
        List<SubmissionRecord> records = new ArrayList<>();
        for (Path path : paths) {
            String fileName = path.getFileName().toString();
            String id = fileName.substring(0, fileName.length() - FILE_SUFFIX.length());
            try {
                SubmissionIds.requireValid(id);
            } catch (SubmissionException e) {
                throw new SubmissionException(
                        "invalid submission record file name: " + fileName);
            }
            requireRegularRecordFile(path);
            records.add(read(path, id));
        }
        records.sort(Comparator
                .comparing(SubmissionRecord::getCreatedAt)
                .thenComparing(SubmissionRecord::getSubmissionId));
        return Collections.unmodifiableList(records);
    }

    @Override
    public void delete(String submissionId) {
        String id = SubmissionIds.requireValid(submissionId);
        try {
            Files.deleteIfExists(recordPath(id));
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to delete submission record '" + id + "' in " + directory + ": "
                            + e.getClass().getSimpleName(), e);
        }
    }

    private void prepareDirectory() {
        if (Files.isSymbolicLink(directory)) {
            throw new SubmissionException(
                    "submission store directory can not be a symbolic link: " + directory);
        }
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to create submission store directory " + directory + ": "
                            + e.getClass().getSimpleName(), e);
        }
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new SubmissionException(
                    "submission store path is not a directory: " + directory);
        }
        restrictDirectory();
    }

    private void restrictDirectory() {
        if (!POSIX) {
            return;
        }
        try {
            Files.setPosixFilePermissions(directory, DIRECTORY_PERMISSIONS);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to restrict permissions on submission store directory: " + directory,
                    e);
        }
    }

    private static void restrictFile(Path path) throws IOException {
        if (POSIX) {
            Files.setPosixFilePermissions(path, FILE_PERMISSIONS);
        }
    }

    private static void moveIntoPlace(Path temporary, Path target) throws IOException {
        try {
            Files.move(
                    temporary,
                    target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void requireRegularRecordFile(Path path) {
        if (Files.isSymbolicLink(path)) {
            throw new SubmissionException(
                    "symbolic links are not allowed in the submission store: "
                            + path.getFileName());
        }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new SubmissionException(
                    "submission record is not a regular file: " + path.getFileName());
        }
    }

    private SubmissionRecord read(Path path, String expectedId) {
        String fileName = path.getFileName().toString();
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to read submission record file " + fileName + ": "
                            + e.getClass().getSimpleName(), e);
        } catch (IllegalArgumentException e) {
            // Properties.load can include file content in this exception; do not attach it.
            throw new SubmissionException(
                    "invalid properties in submission record file: " + fileName);
        }

        String formatVersion = require(properties, FORMAT_KEY, fileName);
        if (!FORMAT_VERSION.equals(formatVersion)) {
            throw new SubmissionException(
                    "unsupported format version in submission record file: " + fileName);
        }
        String id = require(properties, SUBMISSION_ID_KEY, fileName);
        if (!id.equals(expectedId)) {
            throw new SubmissionException(
                    "submission id does not match its file name: " + fileName);
        }
        ApplicationState state = parseState(require(properties, STATE_KEY, fileName), fileName);
        Instant createdAt = parseInstant(
                require(properties, CREATED_AT_KEY, fileName), fileName);
        String updatedValue = optional(properties, UPDATED_AT_KEY);
        Instant updatedAt = updatedValue == null
                ? createdAt
                : parseInstant(updatedValue, fileName);
        String finishedValue = optional(properties, FINISHED_AT_KEY);
        if (state.isFinal() && finishedValue == null) {
            throw new SubmissionException(
                    "final submission record has no finish time: " + fileName);
        }
        if (!state.isFinal() && finishedValue != null) {
            throw new SubmissionException(
                    "non-final submission record has a finish time: " + fileName);
        }
        Instant finishedAt = finishedValue == null
                ? null
                : parseInstant(finishedValue, fileName);

        try {
            return SubmissionRecord.builder(
                            id,
                            require(properties, TARGET_KEY, fileName),
                            state,
                            createdAt)
                    .appName(require(properties, APP_NAME_KEY, fileName))
                    .master(optional(properties, MASTER_KEY))
                    .deployMode(optional(properties, DEPLOY_MODE_KEY))
                    .namespace(optional(properties, NAMESPACE_KEY))
                    .kubeconfig(optional(properties, KUBECONFIG_KEY))
                    .kubernetesJobSecret(optional(properties, JOB_SECRET_NAME_KEY))
                    .kubernetesJobSecretKey(optional(properties, JOB_SECRET_DATA_KEY))
                    .kubernetesJobSecretOwned(
                            optionalBoolean(properties, JOB_SECRET_OWNED_KEY, fileName))
                    .applicationId(optional(properties, APPLICATION_ID_KEY))
                    .error(optional(properties, ERROR_KEY))
                    .updatedAt(updatedAt)
                    .finishedAt(finishedAt)
                    .build();
        } catch (SubmissionException e) {
            throw new SubmissionException(
                    "invalid submission record file " + fileName + ": " + e.getMessage());
        }
    }

    private static String encode(SubmissionRecord record) {
        Properties properties = new Properties();
        properties.setProperty(FORMAT_KEY, FORMAT_VERSION);
        properties.setProperty(SUBMISSION_ID_KEY, record.getSubmissionId());
        properties.setProperty(TARGET_KEY, record.getTargetName());
        properties.setProperty(APP_NAME_KEY, record.getAppName());
        properties.setProperty(STATE_KEY, record.getState().name());
        properties.setProperty(CREATED_AT_KEY, record.getCreatedAt().toString());
        properties.setProperty(UPDATED_AT_KEY, record.getUpdatedAt().toString());
        setOptional(properties, MASTER_KEY, record.getMaster().orElse(null));
        setOptional(properties, DEPLOY_MODE_KEY, record.getDeployMode().orElse(null));
        setOptional(properties, NAMESPACE_KEY, record.getNamespace().orElse(null));
        setOptional(properties, KUBECONFIG_KEY, record.getKubeconfig().orElse(null));
        setOptional(
                properties, JOB_SECRET_NAME_KEY, record.getKubernetesJobSecret().orElse(null));
        setOptional(
                properties, JOB_SECRET_DATA_KEY,
                record.getKubernetesJobSecretKey().orElse(null));
        if (record.isKubernetesJobSecretOwned()) {
            properties.setProperty(JOB_SECRET_OWNED_KEY, Boolean.TRUE.toString());
        }
        setOptional(properties, APPLICATION_ID_KEY, record.getApplicationId().orElse(null));
        setOptional(properties, ERROR_KEY, record.getError().orElse(null));
        setOptional(properties, FINISHED_AT_KEY, record.getFinishedAt()
                .map(Instant::toString)
                .orElse(null));

        StringWriter writer = new StringWriter();
        try {
            properties.store(writer, "spark-pudding submission record");
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to encode submission record '" + record.getSubmissionId() + "'", e);
        }
        return writer.toString();
    }

    private static void setOptional(Properties properties, String key, String value) {
        if (value != null) {
            properties.setProperty(key, value);
        }
    }

    private static String require(Properties properties, String key, String fileName) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new SubmissionException(
                    "missing " + key + " in submission record file: " + fileName);
        }
        return value.trim();
    }

    private static String optional(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private static boolean optionalBoolean(
            Properties properties, String key, String fileName) {
        String value = optional(properties, key);
        if (value == null) {
            return false;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new SubmissionException(
                "invalid " + key + " in submission record file: " + fileName);
    }

    private static ApplicationState parseState(String value, String fileName) {
        try {
            return ApplicationState.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new SubmissionException(
                    "invalid state in submission record file: " + fileName);
        }
    }

    private static Instant parseInstant(String value, String fileName) {
        try {
            return Instant.parse(value);
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "invalid timestamp in submission record file: " + fileName);
        }
    }

    private Path recordPath(String id) {
        return directory.resolve(id + FILE_SUFFIX);
    }

    private static void deleteTemporary(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best effort: the primary failure is reported to the caller and the temporary
            // file is left for normal cleanup if deletion is not possible.
        }
    }
}
