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

import org.pudding.core.util.ConfigRedactor$;

/**
 * Java facade for the configuration redactor.
 *
 * <p>Submitting services should redact every launcher log line and error message before storing
 * or returning it. Redaction handles sensitive keys, assignments and URL user-info; it can not
 * guarantee that arbitrary application output contains no credential, so persisted logs must
 * still be treated as sensitive.</p>
 *
 * <p>The SDK exposes this facade for service integrations but does not apply it to captured
 * application logs automatically: the service decides where logs are persisted or exposed.</p>
 */
public final class LogRedaction {

    private LogRedaction() {
    }

    /**
     * Redact credentials from one log line.
     *
     * @param line log line, may be null
     * @return redacted line, or null when the input is null
     */
    public static String redactLine(String line) {
        return ConfigRedactor$.MODULE$.redactLine(line);
    }

    /**
     * Redact credentials from each line of a text value.
     *
     * @param text text value, may be null
     * @return redacted text, or null when the input is null
     */
    public static String redactText(String text) {
        if (text == null) {
            return null;
        }
        String[] lines = text.split("\\R", -1);
        StringBuilder result = new StringBuilder(text.length());
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                result.append('\n');
            }
            result.append(redactLine(lines[i]));
        }
        return result.toString();
    }
}
