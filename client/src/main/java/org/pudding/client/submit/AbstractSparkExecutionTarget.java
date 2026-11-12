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

import org.apache.spark.launcher.SparkLauncher;
import org.pudding.core.configure.JobConf;
import org.pudding.core.configure.JobPipelineConf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared implementation for targets that submit through the Spark launcher.
 *
 * <p>Subclasses only choose the Spark master, deploy mode, target-specific Spark configuration
 * and resource location. Discovery of jars and construction of the shared submission plan happen
 * here.</p>
 */
public abstract class AbstractSparkExecutionTarget implements ExecutionTarget {

    /**
     * Main class of the Pudding driver.
     */
    public static final String MAIN_CLASS = "org.pudding.core.PuddingSpoon";

    /**
     * Spark configuration keys that describe deployment and are owned by the target.
     */
    private static final List<String> RESERVED_SPARK_CONF = Arrays.asList(
            "spark.master",
            "spark.submit.deployMode",
            "spark.app.name");

    private static final String APP_RESOURCE_PREFIX = "pudding-core";

    private static final String LIBS_DIRECTORY = "libs";

    /**
     * Component types whose implementation lives in a differently named jar. The manifest keeps
     * the logical type ({@code file-sink}) while the implementation is packaged in the
     * {@code file-common-sink} module.
     */
    private static final Map<String, String> JAR_PREFIX_ALIASES = jarPrefixAliases();

    private static Map<String, String> jarPrefixAliases() {
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("file-sink", "file-common-sink");
        aliases.put("mysql-cdc-source", "database-cdc-source");
        aliases.put("postgres-cdc-source", "database-cdc-source");
        aliases.put("mongodb-cdc-source", "database-cdc-source");
        return Collections.unmodifiableMap(aliases);
    }

    /**
     * Support jars that are not tied to a source, sink or catalog type. State backend providers
     * are selected by configuration rather than by a component type, and the S3 runtime is used
     * by any connector or state path that references an object store.
     */
    private static final List<String> BASE_JAR_PREFIXES = Arrays.asList(
            "redis-state-provider",
            "etcd-state-provider",
            "hadoop-aws-runtime");

    /**
     * Object-store runtimes selected from URI schemes present in the job configuration. S3A is
     * always included as a base support jar for existing state paths; the other packs are added
     * only when the job mentions their scheme.
     */
    private static final Pattern OBJECT_STORE_SCHEME =
            Pattern.compile("(?i)\\b(s3a|s3|oss|abfs|abfss|wasb|wasbs|gs|gcs|cosn|cos)://");

    private static final Map<String, String> OBJECT_STORE_RUNTIMES = objectStoreRuntimes();

    private static Map<String, String> objectStoreRuntimes() {
        Map<String, String> runtimes = new LinkedHashMap<>();
        runtimes.put("s3a", "hadoop-aws-runtime");
        runtimes.put("s3", "hadoop-aws-runtime");
        runtimes.put("oss", "hadoop-oss-runtime");
        runtimes.put("abfs", "hadoop-azure-runtime");
        runtimes.put("abfss", "hadoop-azure-runtime");
        runtimes.put("wasb", "hadoop-azure-runtime");
        runtimes.put("wasbs", "hadoop-azure-runtime");
        runtimes.put("gs", "gcs-runtime");
        runtimes.put("gcs", "gcs-runtime");
        runtimes.put("cosn", "hadoop-cos-runtime");
        runtimes.put("cos", "hadoop-cos-runtime");
        return Collections.unmodifiableMap(runtimes);
    }

    private final String sparkHome;

    private final String sparkPuddingHome;

    private final String resourceBase;

    private final Map<String, String> extraSparkConf;

    protected AbstractSparkExecutionTarget(
            String sparkHome,
            String sparkPuddingHome,
            String resourceBase,
            Map<String, String> extraSparkConf) {
        this.sparkHome = trimToNull(sparkHome);
        this.sparkPuddingHome = trimToNull(sparkPuddingHome);
        this.resourceBase = trimToNull(resourceBase);
        this.extraSparkConf = checkedSparkConf(extraSparkConf);
    }

    /**
     * Resolve the Spark master for this target.
     *
     * @param jobConf job configuration
     * @return Spark master
     */
    protected abstract String resolveMaster(JobConf jobConf);

    /**
     * Resolve the Spark deploy mode for this target.
     *
     * @param jobConf job configuration
     * @return deploy mode
     */
    protected abstract String resolveDeployMode(JobConf jobConf);

    /**
     * Validate target-specific settings after all configuration has been merged.
     *
     * @param jobConf      job configuration
     * @param sparkConf    effective Spark configuration
     * @param master       effective Spark master
     * @param deployMode   effective deploy mode
     * @param resourceBase effective resource base
     */
    protected abstract void validateTarget(
            JobConf jobConf,
            Map<String, String> sparkConf,
            String master,
            String deployMode,
            String resourceBase);

    /**
     * Validate target-specific settings after all configuration has been merged.
     *
     * <p>The default delegates to the request-independent overload. Targets whose validation
     * depends on request options override this method.</p>
     *
     * @param jobConf      job configuration
     * @param sparkConf    effective Spark configuration
     * @param master       effective Spark master
     * @param deployMode   effective deploy mode
     * @param resourceBase effective resource base
     * @param request      submission request
     */
    protected void validateTarget(
            JobConf jobConf,
            Map<String, String> sparkConf,
            String master,
            String deployMode,
            String resourceBase,
            SubmissionRequest request) {
        validateTarget(jobConf, sparkConf, master, deployMode, resourceBase);
    }

    /**
     * Resolve where jars and the application resource are stored. The default uses the configured
     * resource base or the local distribution root.
     *
     * @param localDistribution local spark-pudding distribution root
     * @return resource base
     */
    protected String resolveResourceBase(String localDistribution) {
        return firstNonBlank(resourceBase, localDistribution);
    }

    /**
     * Add target-specific Spark configuration.
     *
     * @param sparkConf effective Spark configuration
     * @param jobConf   job configuration
     */
    protected void applyTargetSparkConf(Map<String, String> sparkConf, JobConf jobConf) {
    }

    /**
     * Add target-specific Spark configuration with access to request options.
     *
     * <p>The default delegates to the request-independent overload.</p>
     *
     * @param sparkConf effective Spark configuration
     * @param jobConf   job configuration
     * @param request   submission request
     */
    protected void applyTargetSparkConf(
            Map<String, String> sparkConf, JobConf jobConf, SubmissionRequest request) {
        applyTargetSparkConf(sparkConf, jobConf);
    }

    /**
     * Add target-specific environment variables for the submission process.
     *
     * @param environment process environment
     * @param jobConf     job configuration
     */
    protected void applyTargetEnvironment(Map<String, String> environment, JobConf jobConf) {
    }

    /**
     * Add the arguments that tell the driver where to read the job configuration.
     *
     * <p>The default writes the configuration to a temporary file readable only by the
     * submitting user and passes {@code --jobFile}. Targets that copy the file to remote storage
     * or deliver it through a Secret override this method.</p>
     *
     * @param plan              submission plan being built
     * @param jobConfiguration  job configuration
     * @param deployMode        effective deploy mode
     */
    protected void addJobArguments(
            SubmissionPlan.Builder plan,
            JobConfiguration jobConfiguration,
            String deployMode) {
        Path configFile = stageJobConfigFile(plan, jobConfiguration);
        plan.addAppArgs(
                "--confType", jobConfiguration.getType(),
                "--jobFile", configFile.toAbsolutePath().normalize().toString());
    }

    /**
     * Write the job configuration to a user-only temporary file and register it for cleanup.
     *
     * @param plan             submission plan being built
     * @param jobConfiguration job configuration
     * @return job configuration file
     */
    protected final Path stageJobConfigFile(
            SubmissionPlan.Builder plan, JobConfiguration jobConfiguration) {
        Path file = JobConfigFile.create(jobConfiguration);
        plan.addTemporaryPath(file.getParent());
        return file;
    }

    /**
     * Add target-specific fields to the plan after the shared configuration has been resolved.
     *
     * @param plan         submission plan being built
     * @param jobConf      job configuration
     * @param submissionId unique id assigned to this submission
     */
    protected void customizePlan(
            SubmissionPlan.Builder plan, JobConf jobConf, String submissionId) {
    }

    /**
     * Add target-specific fields to the plan with access to request options.
     *
     * <p>The default delegates to the request-independent overload.</p>
     *
     * @param plan         submission plan being built
     * @param jobConf      job configuration
     * @param submissionId unique id assigned to this submission
     * @param request      submission request
     */
    protected void customizePlan(
            SubmissionPlan.Builder plan,
            JobConf jobConf,
            String submissionId,
            SubmissionRequest request) {
        customizePlan(plan, jobConf, submissionId);
    }

    /**
     * @return whether a resource base was explicitly configured
     */
    protected final boolean hasResourceBase() {
        return resourceBase != null;
    }

    /**
     * @return explicitly configured resource base, or null
     */
    protected final String configuredResourceBase() {
        return resourceBase;
    }

    @Override
    public final SubmissionPlan prepare(SubmissionRequest request) {
        if (request == null) {
            throw new SubmissionException("submission request can not be null");
        }
        try {
            return doPrepare(request);
        } catch (SubmissionException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to prepare submission for target '" + getName() + "': " + message(e),
                    e);
        }
    }

    private SubmissionPlan doPrepare(SubmissionRequest request) {
        JobConfiguration jobConfiguration = request.getJob();
        JobPipelineConf pipeline = ScalaInterop.parse(jobConfiguration);
        JobConf jobConf = pipeline.job();

        String appName = requireText(jobConf.appName(), "job.appName");
        String effectiveSparkHome = requireText(
                firstNonBlank(sparkHome, jobConf.sparkHome()), "job.sparkHome");
        String localDistribution = requireText(
                firstNonBlank(
                        sparkPuddingHome,
                        ScalaInterop.optionValue(jobConf.sparkPuddingHome())),
                "sparkPuddingHome (set PUDDING_HOME or job.sparkPuddingHome)");
        String effectiveResourceBase = requireText(
                resolveResourceBase(localDistribution), "resource base");

        LibraryLayout layout = readLayout(localDistribution);
        List<String> selectedJars = selectJars(
                layout.jars,
                layout.coreJar,
                ScalaInterop.connectorTypes(pipeline),
                objectStoreRuntimePrefixes(jobConfiguration.getContent()));

        String master = requireText(resolveMaster(jobConf), "Spark master");
        String deployMode = requireText(resolveDeployMode(jobConf), "Spark deploy mode");

        Map<String, String> sparkConf = new LinkedHashMap<>(ScalaInterop.sparkConf(jobConf));
        removeReservedSparkConf(sparkConf);
        applyTargetSparkConf(sparkConf, jobConf, request);
        sparkConf.putAll(extraSparkConf);
        removeReservedSparkConf(sparkConf);

        Map<String, String> environment = new LinkedHashMap<>(request.getEnvironment());
        applyTargetEnvironment(environment, jobConf);

        validateTarget(
                jobConf, sparkConf, master, deployMode, effectiveResourceBase, request);

        List<String> jars = new ArrayList<>();
        for (String jar : selectedJars) {
            jars.add(joinResource(effectiveResourceBase, jar));
        }

        SubmissionPlan.Builder plan = SubmissionPlan.builder(getName())
                .appName(appName)
                .sparkHome(effectiveSparkHome)
                .master(master)
                .deployMode(deployMode)
                .appResource(joinResource(effectiveResourceBase, layout.coreJar))
                .mainClass(MAIN_CLASS)
                .jars(jars)
                .sparkConf(sparkConf)
                .environment(environment);
        try {
            addJobArguments(plan, jobConfiguration, deployMode);
            String submissionId = request.getSubmissionId()
                    .orElseGet(() -> UUID.randomUUID().toString());
            plan.submissionId(submissionId);
            customizePlan(plan, jobConf, submissionId, request);
            return plan.verbose(request.isVerbose()).build();
        } catch (RuntimeException e) {
            JobConfigFile.delete(plan.temporaryPaths());
            throw e;
        }
    }

    @Override
    public final RunningApplication submit(SubmissionPlan plan) {
        if (plan == null) {
            throw new SubmissionException("submission plan can not be null");
        }
        if (!getName().equals(plan.getTargetName())) {
            throw new SubmissionException(
                    "submission plan targets '" + plan.getTargetName()
                            + "' but was submitted to '" + getName() + "'");
        }

        SparkLauncher launcher = new SparkLauncher(plan.getEnvironment())
                .setSparkHome(plan.getSparkHome())
                .setAppName(plan.getAppName())
                .setMaster(plan.getMaster())
                .setDeployMode(plan.getDeployMode())
                .setAppResource(plan.getAppResource())
                .setMainClass(plan.getMainClass())
                .addAppArgs(plan.getAppArgs().toArray(new String[0]));

        for (String jar : plan.getJars()) {
            launcher.addJar(jar);
        }
        for (Map.Entry<String, String> entry : plan.getSparkConf().entrySet()) {
            launcher.setConf(entry.getKey(), entry.getValue());
        }
        launcher.setVerbose(plan.isVerbose());

        RunningApplication application;
        try {
            application = start(launcher, plan);
        } catch (RuntimeException e) {
            JobConfigFile.delete(plan.getTemporaryPaths());
            throw e;
        }
        JobConfigFile.scheduleCleanup(application, plan.getTemporaryPaths());
        return application;
    }

    /**
     * Start the prepared application with access to the resolved plan.
     *
     * <p>The default starts a launcher-backed application with the plan's submission id. Targets
     * that need different lifecycle tracking override this method.</p>
     *
     * @param launcher configured Spark launcher
     * @param plan     resolved submission plan
     * @return running application handle
     */
    protected RunningApplication start(SparkLauncher launcher, SubmissionPlan plan) {
        return SparkRunningApplication.start(launcher, getName(), plan.getSubmissionId());
    }

    /**
     * Launch the spark-submit process with stderr redirected to stdout.
     *
     * @param launcher configured Spark launcher
     * @return running spark-submit process
     */
    protected final Process launchProcess(SparkLauncher launcher) {
        if (launcher == null) {
            throw new SubmissionException("spark launcher can not be null");
        }
        launcher.redirectError();
        try {
            return launcher.launch();
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to start Spark application for target '" + getName() + "': "
                            + e.getMessage(), e);
        }
    }

    /**
     * Start the prepared application and derive its state from the spark-submit exit code.
     *
     * <p>Use this for cluster managers that do not report Spark launcher protocol states.</p>
     *
     * @param launcher configured Spark launcher
     * @return running application handle
     */
    protected final RunningApplication startProcess(SparkLauncher launcher) {
        return ProcessRunningApplication.monitor(launchProcess(launcher), getName());
    }

    /**
     * Start a YARN cluster submission and track the application through the ResourceManager
     * REST API.
     *
     * @param launcher    configured Spark launcher
     * @param environment submission environment containing YARN/Hadoop configuration paths
     * @return running application
     */
    protected final RunningApplication startYarnCluster(
            SparkLauncher launcher,
            Map<String, String> environment) {
        String resourceManager = YarnSiteConfig.webappAddress(environment).orElseThrow(() ->
                new SubmissionException(
                        "YARN cluster lifecycle tracking requires "
                                + "yarn.resourcemanager.webapp.address or "
                                + "yarn.resourcemanager.webapp.https.address in yarn-site.xml, "
                                + "or PUDDING_YARN_RM_URL in the submission environment"));
        return YarnClusterRunningApplication.start(
                launchProcess(launcher), getName(), resourceManager);
    }

    private static LibraryLayout readLayout(String localDistribution) {
        Path libsPath = Paths.get(localDistribution, LIBS_DIRECTORY);
        if (!Files.isDirectory(libsPath)) {
            throw new SubmissionException(
                    "spark-pudding libs directory does not exist: " + libsPath.toAbsolutePath());
        }

        List<String> jars;
        try (Stream<Path> paths = Files.list(libsPath)) {
            jars = paths
                    .filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".jar"))
                    .filter(name -> !name.contains("-sources") && !name.contains("-javadoc"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to read spark-pudding libs directory: " + libsPath.toAbsolutePath(), e);
        }
        if (jars.isEmpty()) {
            throw new SubmissionException(
                    "no jars found in spark-pudding libs directory: " + libsPath.toAbsolutePath());
        }

        String coreJar = jars.stream()
                .filter(name -> name.startsWith(APP_RESOURCE_PREFIX))
                .findFirst()
                .orElseThrow(() -> new SubmissionException(
                        "pudding-core jar not found in " + libsPath.toAbsolutePath()));
        return new LibraryLayout(coreJar, jars);
    }

    private static Set<String> objectStoreRuntimePrefixes(String content) {
        Set<String> prefixes = new LinkedHashSet<>();
        if (content == null || content.isEmpty()) {
            return prefixes;
        }
        Matcher matcher = OBJECT_STORE_SCHEME.matcher(content);
        while (matcher.find()) {
            String runtime = OBJECT_STORE_RUNTIMES.get(matcher.group(1).toLowerCase(Locale.ROOT));
            if (runtime != null) {
                prefixes.add(runtime);
            }
        }
        return prefixes;
    }

    private static List<String> selectJars(
            List<String> jars,
            String coreJar,
            List<String> connectorTypes,
            Set<String> runtimePrefixes) {
        Set<String> selected = new LinkedHashSet<>();
        for (String jar : jars) {
            if (jar.equals(coreJar)) {
                continue;
            }
            boolean baseJar = BASE_JAR_PREFIXES.stream().anyMatch(jar::startsWith)
                    || runtimePrefixes.stream().anyMatch(jar::startsWith);
            if (baseJar) {
                selected.add(jar);
                continue;
            }
            for (String type : connectorTypes) {
                String prefix = JAR_PREFIX_ALIASES.getOrDefault(type, type);
                if (jar.startsWith(prefix)) {
                    selected.add(jar);
                    break;
                }
            }
        }
        return new ArrayList<>(selected);
    }

    private static String joinResource(String resourceBase, String fileName) {
        String base = stripTrailingSlashes(resourceBase.trim());
        if (hasScheme(base)) {
            return base + "/" + LIBS_DIRECTORY + "/" + fileName;
        }
        return Paths.get(base, LIBS_DIRECTORY, fileName).toAbsolutePath().normalize().toString();
    }

    private static void removeReservedSparkConf(Map<String, String> sparkConf) {
        for (String key : RESERVED_SPARK_CONF) {
            sparkConf.remove(key);
        }
    }

    private static Map<String, String> checkedSparkConf(Map<String, String> values) {
        Map<String, String> result = new LinkedHashMap<>();
        if (values == null) {
            return result;
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = requireText(entry.getKey(), "spark configuration key");
            if (entry.getValue() == null) {
                throw new SubmissionException(
                        "spark configuration value for '" + key + "' can not be null");
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    /**
     * @param value candidate
     * @param field field name used in the error message
     * @return trimmed non-empty value
     */
    protected static String requireText(String value, String field) {
        if (isBlank(value)) {
            throw new SubmissionException(field + " can not be empty");
        }
        return value.trim();
    }

    /**
     * @param values candidates in priority order
     * @return first non-blank trimmed value, or null
     */
    protected static String firstNonBlank(String... values) {
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }

    /**
     * @param value candidate
     * @return true if null, empty or whitespace only
     */
    protected static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * @param value candidate
     * @return trimmed value or null
     */
    protected static String trimToNull(String value) {
        return isBlank(value) ? null : value.trim();
    }

    /**
     * @param value resource base
     * @return true when the value starts with a URI scheme
     */
    protected static boolean hasScheme(String value) {
        return value != null && value.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*$");
    }

    private static String stripTrailingSlashes(String value) {
        if (value.length() > 1 && value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String message(RuntimeException e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }

    private static final class LibraryLayout {

        private final String coreJar;

        private final List<String> jars;

        private LibraryLayout(String coreJar, List<String> jars) {
            this.coreJar = coreJar;
            this.jars = Collections.unmodifiableList(jars);
        }
    }
}
