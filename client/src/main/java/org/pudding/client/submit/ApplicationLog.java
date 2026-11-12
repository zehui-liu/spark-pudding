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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable snapshot of recent application log lines.
 *
 * <p>Logs may contain user data. They are returned only to callers that explicitly request them
 * and never included in {@link #toString()}.</p>
 */
public final class ApplicationLog {

    private final List<String> lines;

    private final boolean truncated;

    private ApplicationLog(Collection<String> lines, boolean truncated) {
        this.lines = Collections.unmodifiableList(new ArrayList<>(lines));
        this.truncated = truncated;
    }

    /**
     * @return empty log snapshot
     */
    public static ApplicationLog empty() {
        return new ApplicationLog(Collections.emptyList(), false);
    }

    /**
     * Create a snapshot from complete lines.
     *
     * @param lines     log lines
     * @param truncated whether older lines were omitted
     * @return snapshot
     */
    public static ApplicationLog of(Collection<String> lines, boolean truncated) {
        Objects.requireNonNull(lines, "log lines");
        List<String> copy = new ArrayList<>();
        for (String line : lines) {
            if (line != null) {
                copy.add(line);
            }
        }
        return new ApplicationLog(copy, truncated);
    }

    /**
     * Split multi-line log text and keep at most {@code maxLines} trailing lines.
     *
     * @param text     log text
     * @param maxLines maximum number of lines to keep
     * @return snapshot
     */
    public static ApplicationLog tail(String text, int maxLines) {
        requirePositive(maxLines);
        if (text == null || text.isEmpty()) {
            return empty();
        }
        String[] split = text.split("\\r?\\n", -1);
        int end = split.length;
        if (end > 0 && split[end - 1].isEmpty()) {
            end--;
        }
        int start = Math.max(0, end - maxLines);
        List<String> selected = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            selected.add(split[i]);
        }
        return new ApplicationLog(selected, start > 0);
    }

    /**
     * @param maxLines requested line limit
     * @return the limit
     */
    public static int requirePositive(int maxLines) {
        if (maxLines <= 0) {
            throw new SubmissionException("maximum log lines must be positive");
        }
        return maxLines;
    }

    /**
     * @return oldest-to-newest log lines
     */
    public List<String> getLines() {
        return lines;
    }

    /**
     * @return true when older lines were omitted because of the requested limit or buffer capacity
     */
    public boolean isTruncated() {
        return truncated;
    }

    /**
     * @return true when no line is available
     */
    public boolean isEmpty() {
        return lines.isEmpty();
    }

    /**
     * @return lines separated by newlines
     */
    public String getText() {
        return String.join("\n", lines);
    }

    @Override
    public String toString() {
        return "ApplicationLog{lines=" + lines.size()
                + ", truncated=" + truncated
                + '}';
    }
}
