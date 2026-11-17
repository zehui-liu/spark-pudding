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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Fixed-size, thread-safe buffer that keeps the most recent application log lines.
 */
final class BoundedLogBuffer {

    static final int DEFAULT_CAPACITY = 2000;

    private final int capacity;

    private final Deque<String> lines = new ArrayDeque<>();

    private long totalLines;

    BoundedLogBuffer() {
        this(DEFAULT_CAPACITY);
    }

    BoundedLogBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("log buffer capacity must be positive");
        }
        this.capacity = capacity;
    }

    synchronized void append(String line) {
        if (line == null) {
            return;
        }
        lines.addLast(line);
        totalLines++;
        while (lines.size() > capacity) {
            lines.removeFirst();
        }
    }

    synchronized ApplicationLog snapshot(int maxLines) {
        ApplicationLog.requirePositive(maxLines);
        int count = Math.min(maxLines, lines.size());
        int skip = lines.size() - count;
        List<String> selected = new ArrayList<>(count);
        int index = 0;
        for (String line : lines) {
            if (index++ >= skip) {
                selected.add(line);
            }
        }
        return ApplicationLog.of(selected, totalLines > count);
    }
}
