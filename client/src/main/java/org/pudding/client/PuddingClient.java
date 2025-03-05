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

package org.pudding.client;

import org.apache.commons.io.FileUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.pudding.core.JobConfigureParser;
import org.pudding.core.PuddingSpoon;
import org.pudding.core.configure.JobConf;
import org.pudding.core.configure.JobPipelineConf;
import org.pudding.core.configure.SinkConf;
import org.pudding.core.configure.SourceConf;
import org.pudding.core.configure.TransformConf;
import picocli.CommandLine;
import scala.Option;
import scala.collection.immutable.Seq;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static scala.collection.JavaConversions.*;

@CommandLine.Command(name = "sparkPuddingClient", version = "0.0.1", mixinStandardHelpOptions = true)
public class PuddingClient implements Callable<Integer> {

    public static final String PUDDING_HOME = "PUDDING_HOME";

    /**
     * main class
     */
    public static final String MAIN_CLASS = "org.pudding.core.PuddingSpoon";

    public static final String CONF_TYPE = "--confType";

    public static final String JOB_TEXT = "--jobText";

    /**
     * app resource
     */
    public static final String RESOURCE_JAR_NAME = "pudding-core";

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

    @Override
    public Integer call() throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%6$s%n");
        CountDownLatch latch = new CountDownLatch(1);

        String configType = PuddingSpoon.getFileType(jobFile);
        String configText = FileUtils.readFileToString(new File(jobFile), "utf-8");

        JobPipelineConf jobPipelineConf = JobConfigureParser
                .apply(configType)
                .parseFromString(configText);
        JobConf jobConf = jobPipelineConf.job();

        // get spark-pudding path
        String sparkPuddingHome = System.getenv(PUDDING_HOME);
        if (jobConf.sparkPuddingHome().nonEmpty()) {
            sparkPuddingHome = jobConf.sparkPuddingHome().get();
        }

        // get jars from spark-pudding /libs
        List<String> libJars = getJarsFromLibs(sparkPuddingHome + "/libs");

        // get env config form env.properties
        Map<String, String> env = new HashMap<>();
        Properties props = new Properties();
        try (FileInputStream inputStream = new FileInputStream(confFile)) {
            props.load(inputStream);
            for (String key : props.stringPropertyNames()) {
                env.put(key, props.getProperty(key));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // common args
        SparkLauncher launcher = new SparkLauncher(env)
                .setSparkHome(jobConf.sparkHome())
                .setAppName(jobConf.appName())
                .setMaster(jobConf.master())
                .setDeployMode(jobConf.deployMode())
                .setAppResource(sparkPuddingHome + "/libs/" + getAppResource(libJars))
                .setMainClass(MAIN_CLASS)
                .addAppArgs(CONF_TYPE, configType, JOB_TEXT, configText);

        // spark jar
        String finalSparkPuddingHome = sparkPuddingHome;
        Objects.requireNonNull(getJars(jobPipelineConf, libJars)
                        .stream()
                        .map(j -> String.format("%s/libs/%s", finalSparkPuddingHome, j)))
                .forEach(launcher::addJar);

        // spark conf
        mapAsJavaMap(jobConf.config().get()).forEach(launcher::setConf);

        // start job
        launcher.setVerbose(true).startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
                if (handle.getState().isFinal()) {
                    latch.countDown();
                }
            }

            @Override
            public void infoChanged(SparkAppHandle handle) {
            }
        });

        latch.await();

        return 0;
    }

    private static List<String> getJars(JobPipelineConf jobPipelineConf, List<String> jars) {
        List<String> types = seqAsJavaList(jobPipelineConf.sources())
                .stream()
                .map(SourceConf::type)
                .collect(Collectors.toList());

        types.addAll(seqAsJavaList(jobPipelineConf.sinks())
                .stream()
                .map(SinkConf::type)
                .collect(Collectors.toList()));

        Option<Seq<TransformConf>> transforms = jobPipelineConf.transforms();
        if (transforms.nonEmpty()) {
            types.addAll(seqAsJavaList(transforms.get()).stream()
                    .map(TransformConf::type)
                    .collect(Collectors.toList()));
        }

        return jars.stream().filter(jar -> {
            for (String type : types) {
                if (jar.startsWith(type)) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    private static String getAppResource(List<String> jars) {
        return jars.stream()
                .filter(jar -> jar.startsWith(RESOURCE_JAR_NAME))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("resource jar name not found"));
    }

    private static List<String> getJarsFromLibs(String libsPath) {
        Path path = Paths.get(libsPath);
        try (Stream<Path> paths = Files.list(path)) {
            return paths.map(p -> p.getFileName().toString()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("reading the folder error: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new PuddingClient()).execute(args));
    }
}
