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

package org.pudding.client;

import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Manifest;

/**
 * Reports the version from the jar manifest so the CLI cannot drift from the
 * Maven project version.
 */
public final class PuddingVersionProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() {
        String version = PuddingVersionProvider.class.getPackage().getImplementationVersion();
        if (version == null || version.trim().isEmpty()) {
            version = manifestAttribute("Implementation-Version");
        }
        if (version == null || version.trim().isEmpty()) {
            version = "development";
        }
        String sparkVersion = manifestAttribute("Implementation-Spark-Version");
        String sparkSuffix = sparkVersion == null || sparkVersion.trim().isEmpty()
                ? ""
                : " (Spark " + sparkVersion + ")";
        return new String[]{"spark-pudding " + version + sparkSuffix};
    }

    private static String manifestAttribute(String name) {
        try (InputStream input =
                     PuddingVersionProvider.class.getResourceAsStream("/META-INF/MANIFEST.MF")) {
            if (input == null) {
                return null;
            }
            return new Manifest(input).getMainAttributes().getValue(name);
        } catch (IOException e) {
            return null;
        }
    }
}
