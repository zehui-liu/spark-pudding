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

import org.pudding.client.submit.ExecutionTarget;
import org.pudding.client.submit.JobConfiguration;
import org.pudding.client.submit.PuddingSubmitter;
import org.pudding.client.submit.RunningApplication;
import org.pudding.client.submit.SubmissionException;
import org.pudding.client.submit.SubmissionRequest;
import org.pudding.client.submit.SubmissionResult;
import org.pudding.client.submit.TargetNames;
import org.pudding.client.target.KubernetesTarget;
import org.pudding.client.target.LocalTarget;
import org.pudding.client.target.YarnTarget;
import org.pudding.core.JobConfigureParser;
import org.pudding.core.configure.JobPipelineConf;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Command line entry point for submitting a spark-pudding job.
 *
 * <p>The embedded submission SDK is exposed through
 * {@code org.pudding.client.submit.PuddingSubmitter} and the execution targets under
 * {@code org.pudding.client.target}.</p>
 */
@CommandLine.Command(
        name = "sparkPuddingClient",
        versionProvider = PuddingVersionProvider.class,
        mixinStandardHelpOptions = true)
public class PuddingClient implements Callable<Integer> {

    /**
     * conf file args
     */
    @CommandLine.Option(
            names = {"-c", "--confFile"},
            description = "env.properties file path",
            required = true)
    private String confFile;

    /**
     * job file args
     */
    @CommandLine.Option(
            names = {"-j", "--jobFile"},
            description = "job configure file path",
            required = true)
    private String jobFile;

    /**
     * target selection
     */
    @CommandLine.Option(
            names = {"-t", "--target"},
            description = "execution target: local, yarn or kubernetes; "
                    + "inferred from master by default")
    private String target;

    @CommandLine.Option(
            names = {"--master"},
            description = "override the Spark master from the job configuration")
    private String master;

    @CommandLine.Option(
            names = {"--deployMode", "--deploy-mode"},
            description = "override the deploy mode where the target supports it")
    private String deployMode;

    @CommandLine.Option(
            names = {"--resourceBase", "--resource-base"},
            description = "base location of the libs directory, such as an HDFS URI")
    private String resourceBase;

    @CommandLine.Option(
            names = {"--sparkHome", "--spark-home"},
            description = "override job.sparkHome")
    private String sparkHome;

    @CommandLine.Option(
            names = {"--sparkPuddingHome", "--spark-pudding-home"},
            description = "override job.sparkPuddingHome")
    private String sparkPuddingHome;

    @CommandLine.Option(
            names = {"--k8s-image"},
            description = "Kubernetes Spark container image")
    private String k8sImage;

    @CommandLine.Option(
            names = {"--k8s-namespace"},
            description = "Kubernetes namespace")
    private String k8sNamespace;

    @CommandLine.Option(
            names = {"--k8s-service-account"},
            description = "Kubernetes driver service account")
    private String k8sServiceAccount;

    @CommandLine.Option(
            names = {"--k8s-image-pull-secrets"},
            description = "comma separated Kubernetes image pull secret names",
            split = ",")
    private List<String> k8sImagePullSecrets = new ArrayList<>();

    @CommandLine.Option(
            names = {"--k8s-job-secret"},
            description = "Kubernetes Secret containing the job configuration; "
                    + "required for --target kubernetes")
    private String k8sJobSecret;

    @CommandLine.Option(
            names = {"--k8s-job-secret-key"},
            description = "key in the job configuration Secret (default: "
                    + KubernetesTarget.DEFAULT_JOB_SECRET_KEY + ")")
    private String k8sJobSecretKey;

    @CommandLine.Option(
            names = {"--kubeconfig"},
            description = "kubeconfig path exported as KUBECONFIG for submission")
    private String kubeconfig;

    @CommandLine.Option(
            names = {"--conf"},
            description = "additional Spark configuration as key=value; repeatable")
    private Map<String, String> sparkConf = new LinkedHashMap<>();

    @CommandLine.Option(
            names = {"--verbose"},
            description = "print the generated spark-submit command; job configuration is "
                    + "delivered by file or Secret, never inline")
    private boolean verbose;

    @Override
    public Integer call() {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%6$s%n");
        try {
            JobConfiguration job = JobConfiguration.fromFile(Paths.get(jobFile));
            String targetName = resolveTargetName(job);
            rejectKubernetesOptions(targetName);

            ExecutionTarget executionTarget = createTarget(targetName);
            SubmissionRequest request = SubmissionRequest.builder(job)
                    .environment(loadEnvironment(Paths.get(confFile)))
                    .verbose(verbose)
                    .build();

            RunningApplication application = new PuddingSubmitter()
                    .submit(executionTarget, request);
            SubmissionResult result = application.await();
            printResult(result);
            return result.isSuccessful() ? 0 : 1;
        } catch (SubmissionException e) {
            System.err.println("submission failed: " + e.getMessage());
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("submission interrupted");
            return 130;
        }
    }

    private String resolveTargetName(JobConfiguration job) {
        if (!isBlank(target)) {
            return TargetNames.normalize(target);
        }
        String effectiveMaster = !isBlank(master) ? master : readJobMaster(job);
        return TargetNames.inferFromMaster(effectiveMaster);
    }

    private static String readJobMaster(JobConfiguration job) {
        try {
            JobPipelineConf pipeline = JobConfigureParser.apply(job.getType())
                    .parseFromString(job.getContent());
            if (pipeline == null || pipeline.job() == null) {
                throw new SubmissionException(
                        "job configuration does not contain a job section");
            }
            return pipeline.job().master();
        } catch (SubmissionException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SubmissionException(
                    "failed to parse job configuration: " + message(e), e);
        }
    }

    private ExecutionTarget createTarget(String targetName) {
        Map<String, String> extraSparkConf = sparkConf == null
                ? Collections.emptyMap()
                : new LinkedHashMap<>(sparkConf);
        switch (targetName) {
            case TargetNames.LOCAL:
                return createLocalTarget(extraSparkConf);
            case TargetNames.YARN:
                return createYarnTarget(extraSparkConf);
            case TargetNames.KUBERNETES:
                return createKubernetesTarget(extraSparkConf);
            default:
                throw new SubmissionException("unsupported execution target: " + targetName);
        }
    }

    private ExecutionTarget createLocalTarget(Map<String, String> extraSparkConf) {
        validateDeployMode("local", "client");
        LocalTarget.Builder builder = LocalTarget.builder()
                .sparkHome(sparkHome)
                .sparkPuddingHome(sparkPuddingHome)
                .resourceBase(resourceBase)
                .sparkConf(extraSparkConf);
        if (!isBlank(master)) {
            builder.master(master.trim());
        }
        return builder.build();
    }

    private ExecutionTarget createYarnTarget(Map<String, String> extraSparkConf) {
        if (!isBlank(master) && !TargetNames.YARN.equalsIgnoreCase(master.trim())) {
            throw new SubmissionException(
                    "yarn target requires master 'yarn', got: " + master);
        }
        return YarnTarget.builder()
                .deployMode(deployMode)
                .sparkHome(sparkHome)
                .sparkPuddingHome(sparkPuddingHome)
                .resourceBase(resourceBase)
                .sparkConf(extraSparkConf)
                .build();
    }

    private ExecutionTarget createKubernetesTarget(Map<String, String> extraSparkConf) {
        validateDeployMode("kubernetes", "cluster");
        KubernetesTarget.Builder builder = KubernetesTarget.builder()
                .sparkHome(sparkHome)
                .sparkPuddingHome(sparkPuddingHome)
                .resourceBase(resourceBase)
                .image(k8sImage)
                .namespace(k8sNamespace)
                .serviceAccount(k8sServiceAccount)
                .imagePullSecrets(k8sImagePullSecrets)
                .jobSecret(k8sJobSecret)
                .jobSecretKey(k8sJobSecretKey)
                .kubeconfig(kubeconfig)
                .sparkConf(extraSparkConf);
        if (!isBlank(master)) {
            builder.master(master.trim());
        }
        return builder.build();
    }

    private void validateDeployMode(String targetName, String requiredDeployMode) {
        if (!isBlank(deployMode)
                && !requiredDeployMode.equalsIgnoreCase(deployMode.trim())) {
            throw new SubmissionException(
                    targetName + " target only supports deploy mode '" + requiredDeployMode
                            + "', got: " + deployMode);
        }
    }

    private void rejectKubernetesOptions(String targetName) {
        if (TargetNames.KUBERNETES.equals(targetName)) {
            return;
        }
        boolean hasKubernetesOptions = !isBlank(k8sImage)
                || !isBlank(k8sNamespace)
                || !isBlank(k8sServiceAccount)
                || !isBlank(k8sJobSecret)
                || !isBlank(k8sJobSecretKey)
                || !isBlank(kubeconfig)
                || (k8sImagePullSecrets != null && !k8sImagePullSecrets.isEmpty());
        if (hasKubernetesOptions) {
            throw new SubmissionException(
                    "--k8s-image, --k8s-namespace, --k8s-service-account, --k8s-job-secret, "
                            + "--k8s-job-secret-key, --k8s-image-pull-secrets and --kubeconfig "
                            + "require --target kubernetes");
        }
    }

    private static Map<String, String> loadEnvironment(Path path) {
        Path absolutePath = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(absolutePath)) {
            throw new SubmissionException("env.properties file does not exist: " + absolutePath);
        }
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(absolutePath)) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new SubmissionException(
                    "failed to read env.properties file: " + absolutePath, e);
        }
        Map<String, String> environment = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            environment.put(name, properties.getProperty(name));
        }
        return environment;
    }

    private static void printResult(SubmissionResult result) {
        String applicationId = result.getApplicationId()
                .map(id -> " (" + id + ")")
                .orElse("");
        if (result.isSuccessful()) {
            System.out.printf(
                    "application%s finished successfully via %s target%n",
                    applicationId, result.getTargetName());
            return;
        }
        String error = result.getError()
                .map(cause -> ": " + message(cause))
                .orElse("");
        System.err.printf(
                "application%s finished with state %s via %s target%s%n",
                applicationId, result.getState(), result.getTargetName(), error);
    }

    private static String message(Throwable e) {
        String message = e.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return e.getClass().getSimpleName();
        }
        return message;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new PuddingClient()).execute(args));
    }
}
