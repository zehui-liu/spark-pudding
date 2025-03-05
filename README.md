# spark pudding

A micro framework for data integration and processing based on Apache Spark.

- Support defining data processing flow through JSON, YAML, and HOCON configuration files.
- Supports reading and writing from multiple data sources.
- Support data sources based on interface expansion and complex data processing logic.
- Support Apache Spark stream batch integration.

## Building from source

Prerequisites for building Spark Pudding:

- Unix-like system (like Linux, Mac OS X)
- JDK 8 、11
- Git
- Maven (3.8.7)

apache spark 3.2  package:

```shell
mvn clean package -DskipTests
```

other apache spark version package :

```shell
mvn clean package -DskipTests -Dspark.version=3.4.0
```

## Code check

To ensure code quality, this project is strictly written according to Apache's open source standards, using Apache Spark's Scala checkstyle. The following command is used to perform code style checks:

```shell
mvn scalastyle:check
```

## Quick start:

Prerequisites:

- Spark 3.3 client
- Hadoop 3.1

1、Execute the following command to decompress：

```shell
tar -zxvf spark-pudding-0.0.1-SNAPSHOT.tar.gz
```

2、Configure env.properties, like this:

```shell
HADOOP_CONF_DIR=xxx
YARN_CONF_DIR=xxx
```

3、Write task template files, you can use json、yaml or hocon like this:

```json
{
  "job": {
      "sparkHome": "${your path}/spark-3.3.0-bin-hadoop3",
      "sparkPuddingHome": "${your path}/spark-pudding-0.0.1-SNAPSHOT",
      "appName": "spark read csv example job",
      "master": "yarn",
      "deployMode": "client",
      "describe": "spark test job",
      "enableHiveSupport": "true",
      "config": {
          "spark.executor.memory": "512m",
          "spark.executor.cores": "1",
          "spark.driver.memory": "512m",
          "spark.driver.cores": "1",
          "spark.executor.instances": "1"
      }
  },
  "source": [
      {
          "type": "file-common-source",
          "describe": "file common source,local file or hdfs file",
          "id": "source-1",
          "config": {
              "path": "hdfs:///${your hdfs path}examples/data/demo.csv",
              "fileType": "csv",
              "streamRead": false,
              "textDelimiter": ",",
              "forceConvertSchema": true,
              "options": {
                  "header": "true",
                  "delimiter": ","
              },
              "schema": {
                  "col1": "string",
                  "col2": "string",
                  "col3": "string",
                  "col4": "string"
              }
          }
      }
  ],
  "sink": [
      {
          "type": "console-sink",
          "describe": "show data on console",
          "id": "sink1",
          "config": {
              "isStreaming": false,
              "numRows": 10,
              "truncate": 20,
              "vertical": false,
              "printSchema": true
          },
          "dependency": "source-1"
      }
  ]
}
```

4、Execute command in bin folder to start Spark job：

```shell
./job-launcher.sh  "${your path}/spark-pudding-0.0.1-SNAPSHOT/conf/env.properties"  "${your path}/spark-pudding-0.0.1-SNAPSHOT/examples/read_csv_example.json"  "${your path}/spark-pudding-0.0.1-SNAPSHOT/log/${your name}.log"
```

now spark job is started in background. you can see log use command:

```shell
tail -100f ${your path}/spark-pudding-0.0.1-SNAPSHOT/log/t.log
```

also, you can use spark-submit shell to start a job:

you should put ${your path}/spark-pudding-0.0.1-SNAPSHOT/libs jar to spark client jars folder .

and you can use spark client start a spark job like this command:

```shell
./spark-submit \
  --class org.pudding.core.PuddingSpoon  \
  --jars ${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/console-sink-0.0.1-SNAPSHOT.jar,${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/file-common-sink-0.0.1-SNAPSHOT.jar,${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/file-common-source-0.0.1-SNAPSHOT.jar,${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/hive-source-0.0.1-SNAPSHOT.jar,${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/jdbc-common-source-0.0.1-SNAPSHOT.jar,${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/kafka-source-0.0.1-SNAPSHOT.jar \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 512m \
  --executor-cores 1 \
  ${your path}/spark-pudding-0.0.1-SNAPSHOT/libs/pudding-core-0.0.1-SNAPSHOT.jar \
  --fileType 'json'  \
  --confText '{
  "job": {
      "sparkHome": "${your path}/spark-3.3.0-bin-hadoop3",
      "sparkPuddingHome": "${your path}/spark-pudding-0.0.1-SNAPSHOT",
      "appName": "spark read csv example job",
      "master": "yarn",
      "deployMode": "client",
      "describe": "spark test job",
      "enableHiveSupport": "true",
      "config": {
          "spark.executor.memory": "512m",
          "spark.executor.cores": "1",
          "spark.driver.memory": "512m",
          "spark.driver.cores": "1",
          "spark.executor.instances": "1"
      }
  },
  "source": [
      {
          "type": "file-common-source",
          "describe": "file common source,local file or hdfs file",
          "id": "source-1",
          "config": {
              "path": "hdfs:///${your path}/demo.csv",
              "fileType": "csv",
              "streamRead": false,
              "textDelimiter": ",",
              "forceConvertSchema": true,
              "options": {
                  "header": "true",
                  "delimiter": ","
              },
              "schema": {
                  "col1": "string",
                  "col2": "string",
                  "col3": "string",
                  "col4": "string"
              }
          }
      }
  ],
  "sink": [
      {
          "type": "console-sink",
          "describe": "show data on console",
          "id": "sink1",
          "config": {
              "isStreaming": false,
              "numRows": 10,
              "truncate": 20,
              "vertical": false,
              "printSchema": true
          },
          "dependency": "source-1"
      }
  ]
}'
```

## Core concept:

| name      | describe                                                     |
| --------- | ------------------------------------------------------------ |
| source    | Abstracting the ability of Apache Spark to read data as a source, the interface functionality is defined in the DataSource class. |
| transform | Abstracting Apache Spark's ability to process data into a transform, supporting Apache Spark's complex data processing capabilities, and defining interface functionality in the DataTransform class. |
| sink      | Abstracting the ability of Apache Spark to write data as a sink, the interface functionality is defined in the DataSink class. |