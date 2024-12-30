# spark pudding

一款基于apache spark 的流批一体数据集成、数据处理的微框架。支持以配置文件的方式构建复杂的 spark 任务，支持 yaml、json、hocon 做配置文件，核心通过将 spark 任务抽象成 source、transform、sink构建单个任务的DAG。提供拓展接口，方便编写自己的source、transform、sink，目的是提高 spark 的代码开发效率，一次开发多次使用。

## 适用场景 ：

- 以 spark 为基础的数据集成，支持批量处理和流处理。
- 以 spark 为基础的复杂的数据链路的构建，一次开发多次使用，用于复杂业务或者临时数据分析提取等数据链路的快速构建。

## 编译：

spark pudding 以 Maven 作为构建工具，使用如下的命令行构建：

```shell
mvn clean package -DskipTests
```

## 代码检查：

为保证代码质量，本工程严格按照 apache 的开源标准编写，使用 apache spark 的 scala checkstyle，使用如下命令进行代码风格检查：

```shell
 mvn scalastyle:check
```

## 部署：

打包后成品包在 build/target 目录下，spark-pudding-${project.version}.tar.gz, 解压后将 libs 下 jar 放置在 apache spark 客户端 中 jars目录下，按照如下的命令行启动任务：

```shell
./spark-submit \
  --class org.pudding.core.PuddingSpoon \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 2 \
  --executor-memory 2g \
  --executor-cores 2 \
  {your-path}/spark-pudding-${project.version}/libs/core-${project.version}.jar \
  "{your-path}/read_csv_example.yaml" 
```

以上部署会将不需要的 jar 上传到运行环境中，如果开发的 jar 很大而不被使用就显得没有那么优雅。为规避这个问题，客户端功能正在开发中，后续版本提供。

## 核心概念：

| 名称      | 描述                                                         |
| --------- | ------------------------------------------------------------ |
| source    | 将 apache spark 读取数据的能力抽象成 source，接口功能在DataSource 类中进行定义。 |
| transform | 将 apache spark 处理数据的能力抽象成transform，支持apache spark 的复杂数据处理能力，接口功能在DataTransform 类中进行定义。 |
| sink      | 将apache spark 写入数据的能力抽象成 sink，接口功能在DataSink 类中进行定义。 |

## RoadMap：

- [ ] 当前存在的 module 中的todo 全部实现。
- [ ] 编写更多的测试用例，覆盖更多的代码。
- [ ] 给出更多的 examples 使用案例。
- [ ] 提供客户端，支持 apache spark 不同的使用启动方式。

## 案例实现：

待补充

