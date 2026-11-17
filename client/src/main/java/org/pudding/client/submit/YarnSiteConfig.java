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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves the YARN ResourceManager web application endpoint used for cluster lifecycle
 * tracking.
 *
 * <p>The client deliberately does not depend on Hadoop YARN classes. It reads the standard
 * {@code yarn-site.xml} from {@code YARN_CONF_DIR} or {@code HADOOP_CONF_DIR} and uses the
 * ResourceManager REST API. An explicit {@code PUDDING_YARN_RM_URL} environment entry takes
 * precedence; this is also the escape hatch for clusters whose web application address is not
 * present in the submitted Hadoop configuration.</p>
 */
final class YarnSiteConfig {

    private static final String EXPLICIT_URL = "PUDDING_YARN_RM_URL";

    private static final String YARN_CONF_DIR = "YARN_CONF_DIR";

    private static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";

    private static final String HTTP_ADDRESS = "yarn.resourcemanager.webapp.address";

    private static final String HTTPS_ADDRESS = "yarn.resourcemanager.webapp.https.address";

    private YarnSiteConfig() {
    }

    /**
     * Resolve the ResourceManager web URL.
     *
     * @param environment submission environment, normally from a {@link SubmissionPlan}
     * @return URL without a trailing slash, or empty when no web application address exists
     */
    static Optional<String> webappAddress(Map<String, String> environment) {
        Map<String, String> env = environment == null ? Map.of() : environment;
        String explicit = trimToNull(env.get(EXPLICIT_URL));
        if (explicit == null) {
            explicit = trimToNull(System.getenv(EXPLICIT_URL));
        }
        if (explicit != null) {
            return Optional.of(withScheme(explicit, false));
        }

        String confDir = firstNonBlank(
                env.get(YARN_CONF_DIR),
                env.get(HADOOP_CONF_DIR),
                System.getenv(YARN_CONF_DIR),
                System.getenv(HADOOP_CONF_DIR));
        if (confDir == null) {
            return Optional.empty();
        }
        File yarnSite = new File(confDir, "yarn-site.xml");
        if (!yarnSite.isFile()) {
            return Optional.empty();
        }
        Map<String, String> properties = parseProperties(yarnSite);
        String http = findAddress(properties, HTTP_ADDRESS);
        if (http != null) {
            return Optional.of(withScheme(http, false));
        }
        String https = findAddress(properties, HTTPS_ADDRESS);
        if (https != null) {
            return Optional.of(withScheme(https, true));
        }
        return Optional.empty();
    }

    private static String findAddress(
            Map<String, String> properties,
            String preferred) {
        String direct = trimToNull(properties.get(preferred));
        if (direct != null) {
            return direct;
        }
        // HA configurations use yarn.resourcemanager.webapp.address.rm1, .rm2, ...
        List<String> candidates = new ArrayList<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(preferred + ".")) {
                String value = trimToNull(entry.getValue());
                if (value != null) {
                    candidates.add(value);
                }
            }
        }
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    private static Map<String, String> parseProperties(File file) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            Document document = factory.newDocumentBuilder().parse(file);
            NodeList properties = document.getElementsByTagName("property");
            Map<String, String> result = new java.util.LinkedHashMap<>();
            for (int i = 0; i < properties.getLength(); i++) {
                Node node = properties.item(i);
                if (!(node instanceof Element)) {
                    continue;
                }
                Element property = (Element) node;
                String name = childText(property, "name");
                String value = childText(property, "value");
                if (name != null && value != null) {
                    result.put(name, value);
                }
            }
            return result;
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static String childText(Element parent, String tag) {
        NodeList children = parent.getElementsByTagName(tag);
        if (children.getLength() == 0) {
            return null;
        }
        return trimToNull(children.item(0).getTextContent());
    }

    private static String withScheme(String value, boolean https) {
        String trimmed = value.trim();
        String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            return stripTrailingSlash(trimmed);
        }
        return (https ? "https://" : "http://") + stripTrailingSlash(trimmed);
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            String trimmed = trimToNull(value);
            if (trimmed != null) {
                return trimmed;
            }
        }
        return null;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
