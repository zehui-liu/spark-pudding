# spark pudding

[English](README.md) | 简体中文

一个基于 Apache Spark 的轻量级数据集成与处理框架。

- 使用 JSON、YAML 或 HOCON 配置定义数据管道。
- 通过可插拔的 source、transform 和 sink 模块读写多种数据源。
- 同一份作业可以运行在本地 Spark、Apache YARN 或 Kubernetes 上。
- 通过可复用的客户端 SDK 或命令行客户端提交作业。

## 说明

本项目代码由个人独立开发，与开发者所在公司无关。

## Release Notes

`1.0` 版本线的能力时间线和升级注意事项见
[RELEASE_NOTES.md](RELEASE_NOTES.md)（中文）和
[RELEASE_NOTES.en.md](RELEASE_NOTES.en.md)（English）。

## 版本策略

产品版本规则、Spark 兼容线、分支模型和发布产物见
[build/doc/versioning.zh-CN.md](build/doc/versioning.zh-CN.md)。

## 从源码构建

前置条件：

- Unix 类系统（Linux 或 macOS）
- JDK 11 或 17（JDK 11 是主要 CI 基线，JDK 17 也会验证；JDK 8 和 JDK 18+ 会被构建拒绝）
- Maven 3.8.7
- 运行环境使用 Apache Spark 3.4.0、Hadoop 3.x 和 Scala 2.12

构建并测试全部内容：

```shell
mvn clean verify
```

测试使用 ScalaTest 运行。`verify` 还会组装发行包，并检查每个 connector、provider 和 runtime
模块是否都在 `libs` 中产出了 jar；缺少组件 jar 时构建会失败。

构建产物：

```text
build/target/spark-pudding-1.0.0/
build/target/spark-pudding-1.0.0.tar.gz
```

根 `pom.xml` 中的 `spark.version` 是项目构建和测试所针对的版本。Kubernetes 镜像和提交主机的
Spark 安装必须使用相同版本。Iceberg、Hudi 和 Paimon 依赖分别使用
`iceberg.spark.binary.version`、`hudi.spark.binary.version` 和
`paimon.spark.binary.version`；`elasticsearch.version` 和 `mongodb.spark.version` 固定
Elasticsearch 和 MongoDB connector 的版本，Kafka connector 跟随 `spark.version`。修改 Spark
基线时需要一起更新这些版本。`pudding k8s build-image` 在两者都可用时会比较 Spark 发行版本和客户端
jar manifest，不一致时拒绝继续；无法比较时给出警告并继续。经过测试的 JDK、Spark、Hadoop、
lakehouse 和 connector 版本、CI 矩阵以及外部服务测试配置见
[build/doc/compatibility.md](build/doc/compatibility.md)。

## 提交 SDK 与执行目标

client 模块提供可复用的提交 SDK，支持 `local`、`yarn` 和 `kubernetes` 目标。CLI 根据作业的
master 推断目标，也接受 `--target`。使用 REST 后端时，`SubmissionManager` 会持久化提交记录、
提供最近日志、对账状态，并在客户端重启后重新连接 Kubernetes driver Pod。API、CLI 选项、资源
布局和部署规则见 [client/README.md](client/README.md)。镜像构建、kind 环境和 Kubernetes 提交
命令见 [build/k8s/README.md](build/k8s/README.md)。

## 作业模式

必填的 `job.mode` 选择用户目标；执行引擎由该模式推导，不能单独配置：

| 模式 | 使用场景 | 拓扑 | 执行方式 |
| --- | --- | --- | --- |
| `batch` | 通用有界处理 | 一个或多个 source、transform，以及一个 sink | batch |
| `sync` | 单 source/单 sink 同步 | 一个 source 和一个 sink；允许 transform | batch |
| `streaming` | 持续处理 | 一个 source 和一个 sink | structured streaming |

所有模式都只允许一个 sink。一次运行只有一个提交边界，引擎不提供跨多个 sink 的分布式事务。

`sync` 需要指定策略：

```yaml
job:
  mode: sync
  strategy: full        # 或 incremental
```

- `sync` + `full` 刷新目标，并自动注入 overwrite 模式；要求 sink 支持原子替换，否则必须显式设置
  `config.allowNonAtomicReplace: true`。
- `sync` + `incremental` 使用游标日志，需要增量状态；transform 必须保留 key、operation 和
  ordering 元数据。
- `batch` 用于任意有界处理，不使用游标状态。
- `streaming` 使用 Structured Streaming 和该单个 sink 的 checkpoint。

`job.strategy` 只对 sync 有效。`job.trigger` 只对 streaming 有效。`job.execution` 以及旧的
`full`、`incremental` 和 `etl` 模式会被拒绝；旧作业文件不会迁移，必须按当前模型重写。

调度是外部能力。spark-pudding 没有内部调度器：请使用 Airflow、cron、Kubernetes CronJob 或
其他编排系统运行 batch 和 sync 作业，并用同样的机制保持 streaming 作业存活。完整矩阵、校验
规则和 trigger 语法见 [build/doc/job-modes.md](build/doc/job-modes.md)。

## 快速开始

1. 解压发行包并设置环境：

```shell
mkdir -p spark-pudding-1.0.0
tar -xzf spark-pudding-1.0.0.tar.gz -C spark-pudding-1.0.0
export PUDDING_HOME="$PWD/spark-pudding-1.0.0"
export JAVA_HOME=/path/to/jdk-11
# 也支持 JDK 17：export JAVA_HOME=/path/to/jdk-17
export PATH="$JAVA_HOME/bin:$PATH"
```

2. 配置 `conf/env.properties`。它是一个 properties 文件，其中的条目会导出到 Spark 提交进程。
   使用 YARN 时加入集群配置目录：

```properties
HADOOP_CONF_DIR=/path/to/hadoop/etc/hadoop
YARN_CONF_DIR=/path/to/hadoop/etc/hadoop
```

本地作业在 `JAVA_HOME`、`SPARK_HOME` 已导出的情况下可以使用空文件；否则在这里设置。
客户端和 driver 需要 JDK 11 或 17。

3. 按实际环境修改 `examples/read_csv_example.yaml`。至少设置 `job.sparkHome`、
   `job.sparkPuddingHome`、`job.master` 和 source 路径。本地作业示例：

```yaml
job:
  mode: batch
  sparkHome: '/path/to/spark-3.4.0-bin-hadoop3'
  sparkPuddingHome: '/path/to/spark-pudding-1.0.0'
  master: local[*]
  deployMode: client
source:
  - type: file-common-source
    config:
      path: '/path/to/spark-pudding-1.0.0/examples/data/demo.csv'
      fileType: csv
```

4. 提交作业。带日志文件时，`pudding submit` 在后台启动作业后返回；不带日志文件时，客户端保持
   附着并返回作业退出状态：

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml" \
  "$PUDDING_HOME/log/my-job.log"
```

5. 跟踪作业：

```shell
tail -f "$PUDDING_HOME/log/my-job.log"
```

部分示例使用 `${SPARK_HOME}` 和 `${PUDDING_HOME}` 占位符。引擎不会展开作业配置中的环境变量；
提交前请替换这些值（例如使用 `envsubst`），或直接编辑示例。`read_csv_example.yaml` 已经使用
显式路径。完整示例索引见 [build/examples/README.md](build/examples/README.md)。

`pudding submit` 根据 `job.master` 推导执行目标。需要镜像、命名空间或 service account 的
Kubernetes 作业使用 `bin/pudding k8s submit`；需要覆盖目标选项时直接使用客户端 CLI。

## 统一 CLI

发行包通过 `bin/pudding` 提供统一入口。参数会原样传递。实现脚本位于 `bin/lib/`，属于内部
实现。

| 命令 | 用途 |
| --- | --- |
| `bin/pudding submit <env.properties> <job-file> [log-file]` | 通过内置 client 提交作业 |
| `bin/pudding checkpoint <show\|status\|delete\|cleanup> ...` | 查看或删除 streaming checkpoint |
| `bin/pudding state <show\|status\|list\|reset\|resolve\|unlock\|migrate> ...` | 查看或修复增量状态 |
| `bin/pudding capabilities <verify\|show\|diff> ...` | 校验或比较能力 manifest |
| `bin/pudding maintenance <iceberg\|paimon\|hudi> <action> ...` | 执行一次湖仓表维护 |
| `bin/pudding maintenance-server ...` | 启动 HTTP 表维护控制面 |
| `bin/pudding k8s submit ...` | 提交 Kubernetes 作业 |
| `bin/pudding k8s build-image ...` | 构建 Spark Kubernetes 镜像 |
| `bin/pudding k8s kind-setup ...` | 创建本地 kind 集群 |
| `bin/pudding verify <distribution-dir>` | 在源码树中校验发行包 |

示例：

```shell
"$PUDDING_HOME/bin/pudding" checkpoint status s3a://bucket/checkpoints/orders --json
"$PUDDING_HOME/bin/pudding" state show hdfs:///pudding/state/orders
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg status \
  --catalog lake --table db.orders --conf-file /etc/pudding/iceberg.properties
```

`bin/pudding help` 会列出所有命令。统一入口是唯一受支持的公开 CLI；`bin/lib/` 下的实现脚本
属于内部实现，不应直接调用。每个命令的完整选项和可运行示例见
[build/doc/cli.zh-CN.md](build/doc/cli.zh-CN.md)。

Spark 配置会同时传递到提交进程和 driver session；实际优先级和 session 解析流程见
[build/doc/spark-conf.md](build/doc/spark-conf.md)。

## Connection 与 Catalog

一个管道可以定义 connection 和 catalog，并在 source 和 sink 中引用：

- **connection** 描述如何访问一个系统：endpoint、凭据和原生客户端属性。它适用于表系统，也
  适用于 Kafka、Elasticsearch、MongoDB 以及未来的 connector。
- **catalog** 描述表元数据：Hive、Iceberg、Hudi、Paimon 和 JDBC catalog 会在 Spark session
  启动前安装。Gravitino 只作为元数据提供者：读取其 REST API 解析底层 catalog，然后使用原生
  Spark catalog。不需要 Gravitino Spark connector 和 Java client。

内置 provider 实现位于 `core` 之外的 `provider/connection-providers` 和
`provider/catalog-providers`。它们通过 `ServiceLoader` 发现，因此新增 provider 不需要修改
core 或作业配置格式。`job.config`、`options`、`connection.properties` 和
`catalog.properties` 中的 HOCON 嵌套 map 会展平为点分 key，因此
`security { protocol = "SASL_SSL" }` 等价于 `security.protocol = "SASL_SSL"`。client 会将
两个 provider jar 自动加入提交。直接使用 `spark-submit` 时，如果作业使用 connection 或
catalog，需要自行加入 provider jar；见 [provider/README.md](provider/README.md)。

Hudi、Paimon、Iceberg、Elasticsearch、MongoDB、Kafka 和对象存储（S3A/OSS/Azure/GCS/COS）runtime 打包在
`runtime/` 下，由对应的 source/sink 实现或状态路径共享。client 按 connector 选择一个 runtime
jar，并对同时使用 source 和 sink 的作业去重。见 [runtime/README.md](runtime/README.md)。
直接使用 `spark-submit` 时同样需要加入对应的 runtime jar。

共享 connector 支持代码位于 core 之外的 `connector/`。JDBC 和 MySQL source/sink connector
使用 `connector/jdbc-common` 解析 JDBC 选项；选择这些 connector 时 client 会自动加入该 jar。
直接使用 `spark-submit` 时还需要加入 `libs/jdbc-common-<version>.jar`。见
[connector/README.md](connector/README.md)。

```yaml
connection:
  - name: kafka-prod
    type: kafka
    endpoint: broker-1:9092,broker-2:9092
    auth:
      mechanism: SCRAM-SHA-512
      username: pudding
      password: ${secret:env/KAFKA_PASSWORD}
    properties:
      security.protocol: SASL_SSL

  - name: hms-prod
    type: hive-metastore
    endpoint: thrift://hms-1:9083,thrift://hms-2:9083
    properties:
      metastore.sasl.enabled: 'true'

  - name: pg-meta
    type: jdbc
    endpoint: jdbc:postgresql://meta-db:5432/lake
    auth:
      username: pudding
      password: ${secret:file:/etc/pudding/secrets/lake-db#password}
    properties:
      driver: org.postgresql.Driver

  - name: mongo-prod
    type: mongodb
    endpoint: mongodb://mongo-1:27017,mongo-2:27017
    auth:
      username: pudding
      password: ${secret:env/MONGO_PASSWORD}
    properties:
      authSource: admin
      replicaSet: rs0

  - name: gravitino-prod
    type: gravitino
    endpoint: http://gravitino:8090
    auth:
      token: ${secret:env/GRAVITINO_TOKEN}

catalog:
  - name: hive_prod
    type: hive
    connection: hms-prod
    properties:
      warehouse: hdfs://nn/warehouse

  - name: lake
    type: gravitino
    connection: gravitino-prod
    properties:
      metalake: prod
      catalog: lakehouse

  - name: pg_catalog
    type: jdbc
    connection: pg-meta

source:
  - id: events
    type: kafka-source
    connection: kafka-prod
    config:
      subscribe: order-events
      options:
        startingOffsets: latest

  - id: orders
    type: hive-source
    catalog: hive_prod
    config:
      table: sales.orders

  - id: mongo_orders
    type: mongodb-source
    connection: mongo-prod
    config:
      database: app
      collection: orders
```

同一个作业中的 connection 和 catalog 名称必须唯一。一个 source 或 sink 最多引用其中一个。
connection 和 catalog 引用会在 Spark 启动前校验；connector 不支持该引用时会直接拒绝，不会
静默忽略。

解析顺序：

1. 作业解析器保留 connection 和 catalog 名称引用；
2. `PipelineValidator` 校验名称、provider 类型、引用和组件能力；
3. driver 解析 secret 引用、解析 catalog 定义（包括 Gravitino REST 调用），并把 catalog 设置
   安装到 Spark 配置中；
4. 每个 source 和 sink 收到自己的资源配置，以及解析后的 connection 和 catalog。

对于 endpoint 和凭据，connection 设置优先于 source/sink 的 `options`。资源选择器
（`table`、`subscribe`、`esIndex`、`database`、`collection`）仍保留在组件配置中。

### Secret 引用

`job.config`、connection、catalog 和组件配置中的凭据可以使用：

- `${secret:env/NAME}`：环境变量；
- `${secret:file:/path/to/file#key}`：properties 文件中的某个 key；
- `${secret:file:/path/to/file}`：文件的 trim 后内容。

引用只在 driver 解析，client 永不解析。不要在作业文件中放明文凭据；请使用引用，并让文件
保持 `0600` 权限。

### Gravitino

Gravitino 只作为元数据 catalog 使用。`gravitino` catalog provider 调用
`GET /api/metalakes/{metalake}/catalogs/{catalog}`，并把返回的 Hive、Iceberg、JDBC、Hudi 或
Paimon catalog 映射为原生 Spark catalog。安装前会将 catalog 属性合并到 Gravitino 属性之上。
Kafka 消息类 catalog，以及 Gravitino 不按表 catalog 建模的系统（Elasticsearch、MongoDB），
仍然通过 connection 配置。

支持的 Gravitino provider 为 Hive、Iceberg、JDBC、Hudi 和 Paimon。REST API 要求 Gravitino 的
vendor media type；client 发送 `Accept: application/vnd.gravitino.v1+json`，支持 bearer token
或 basic authentication。Gravitino catalog 名称从 `catalog` 属性读取（也接受
`gravitinoCatalog`）。source 或 sink 可以引用 Gravitino catalog；其声明的 vendor 类型会在
driver 解析，并在使用前校验原生 catalog 类型。上线前请在 staging 作业中验证对应 Gravitino
版本的 catalog 属性。

### Hudi、Paimon 和 Iceberg

Hudi、Paimon 和 Iceberg 既可作为 catalog，也可作为 source/sink connector。内置模块针对
Spark 3.4 使用 Hudi 0.14.0、Paimon 1.3.2 和 Iceberg 1.5.0 构建。source/sink 语义、
catalog/path 行为、全量替换原子性和测试覆盖见
[build/doc/lakehouse-matrix.md](build/doc/lakehouse-matrix.md)。

#### Hudi

Hudi 的 `HoodieCatalog` 是 Spark **session** catalog，因此一个作业只能配置一个 Hudi
catalog，不能与其他 session catalog（例如 generic Paimon catalog）组合。该 catalog provider
还会安装 `HoodieSparkSessionExtension` 以及 Kryo serializer/registrar。
`hive-metastore` connection 可以提供 metastore URI。

```yaml
connection:
  - name: hms-prod
    type: hive-metastore
    endpoint: thrift://hms-1:9083,thrift://hms-2:9083

catalog:
  - name: hudi_lake
    type: hudi
    connection: hms-prod
    properties:
      warehouse: hdfs://namenode:8020/warehouse

source:
  - type: hudi-source
    id: source-1
    catalog: hudi_lake
    config:
      table: sales.orders
      filterExpr: "dt >= '2026-01-01'"

sink:
  - type: hudi-sink
    id: sink-1
    catalog: hudi_lake
    config:
      table: sales.orders
      operation: upsert
      recordKeyFields: [id]
      partitionBy: [dt]
      preCombineField: ts
    dependency: source-1
```

如果要使用 Hudi 的 path-based data source，请用 `tablePath` 代替 `catalog`。Path 模式支持
snapshot、incremental、read-optimized 和 CDC 查询，支持 time travel、schema evolution 和任意
Hudi 选项。Catalog 模式使用 Spark table API；高级读取选项仍需要 `tablePath`；catalog 增量读
和 streaming 写会解析表的 base path 并复用 path reader/writer。sink 支持 insert、
upsert、bulk insert、delete、insert overwrite、table/partition overwrite、bootstrap、
COW/MOR 表、Hive sync 和 streaming 写入。见
[source/hudi-source/README.md](source/hudi-source/README.md) 和
[sink/hudi-sink/README.md](sink/hudi-sink/README.md)。

Hudi catalog 属性包括 `warehouse`、catalog 级别的 `enableHiveSupport`（默认 `true`）、`setSerializer`（默认
`true`）、`serializer`、`setKryoRegistrator`（默认 `true`）、`kryoRegistrator` 以及
`metastore.uris`/`hive.metastore.uris`。其他 `spark.*` 和 `hive.*` 条目会转发到 session。

Hudi connector jar 打包了 Hudi 的 HFile/metadata-table 支持，并使用 Spark 的 Hadoop 和 Hive
runtime，而不是 Hudi 较旧的 Hadoop 2.x 副本。这样 Spark classpath 保持权威。

#### Paimon

Paimon 可以作为**命名** catalog（`SparkCatalog`）安装，并与 Iceberg、JDBC 或 Hudi session
catalog 共存；也可以通过 `sessionCatalog: true` 或 `mode: generic` 作为**通用 session**
catalog（`SparkGenericCatalog`）安装。Paimon catalog 需要 `warehouse`；元数据可以放在文件系统、
Hive metastore 或 JDBC metastore 中。

```yaml
catalog:
  - name: paimon_lake
    type: paimon
    properties:
      warehouse: hdfs://namenode:8020/paimon/warehouse
      metastore: filesystem

source:
  - type: paimon-source
    id: source-1
    catalog: paimon_lake
    config:
      table: sales.orders
      scanMode: latest-full
      filterExpr: "dt >= '2026-01-01'"

sink:
  - type: paimon-sink
    id: sink-1
    catalog: paimon_lake
    config:
      table: sales.orders
      primaryKey: [id, dt]
      partition: [dt]
      bucket: 4
    dependency: source-1
```

source 支持 batch 和 streaming 读取、snapshot/tag/branch/time-travel 扫描以及 Paimon scan
选项。sink 创建带主键、分区和 bucket 的表，并支持 batch 和 streaming 写入。由于 Paimon
1.3.2 没有原生 catalog `toTable` streaming sink，catalog 表的 append 会解析 location；
update/CDC 使用直接主键 writer。除非 catalog 是 generic session catalog，否则 Paimon 表名会
加上 catalog 的 Spark 名称。见
[source/paimon-source/README.md](source/paimon-source/README.md) 和
[sink/paimon-sink/README.md](sink/paimon-sink/README.md)。

Hudi 和 Paimon 示例都使用 `sales` 数据库。数据库必须在第一次写入前存在；请在表准备步骤中使用
Spark SQL（或 catalog 工具）创建它。示例配置位于
`examples/read_hudi_catalog_example.yaml`、`examples/write_hudi_catalog_example.yaml`、
`examples/read_paimon_example.yaml`、`examples/write_paimon_example.yaml` 和
`examples/read_iceberg_example.yaml`。提交前请替换 `${SPARK_HOME}` 和 `${PUDDING_HOME}`
占位符。

Paimon catalog 需要 `warehouse`。其他属性包括 `metastore`（`filesystem`、`hive` 或
`jdbc`）、`uri`、`jdbc.user`、`jdbc.password`、`token`、`sparkCatalogName`/`catalogName`、
`sessionCatalog`/`mode` 以及 `table-default.*` 默认值。`hive-metastore`、`jdbc` 或 `mysql`
connection 会自动提供 metastore URI 和凭据。

#### Iceberg

Iceberg 默认作为**命名** Spark catalog 安装；当 Spark 名称为 `spark_catalog` 时作为 Hive
session catalog 安装。`iceberg-source` 按 table 或 path 读取，支持 batch 的
snapshot/timestamp/branch/tag 选择器、append/upsert/CDC 增量读以及 Structured Streaming
snapshot 读；Iceberg 的 `.changes` 需要 catalog 表。`iceberg-sink` 通过同一个 catalog 写入，
并在 streaming 和 sync incremental 作业中支持 append/upsert/CDC。source 配置见
[source/iceberg-source/README.md](source/iceberg-source/README.md)。`iceberg-runtime` jar 由
client 选择；直接使用 `spark-submit` 时需要手动加入。

### 原生数据库 CDC

`mysql-cdc-source`、`postgres-cdc-source` 和 `mongodb-cdc-source` 分别通过 Debezium（MySQL
binlog、PostgreSQL 逻辑复制）或 MongoDB change stream 直接读取数据库变更，无需 Kafka、
Kafka Connect 或 Debezium Server。三种 source 仅支持 streaming，并且必须显式声明源列
`schema`。行数据包含声明的列以及 `__op`（`I`/`U`/`D`）、`_cdc_timestamp`、
`_cdc_sequence` 和 `_cdc_table`。checkpoint 中只保存 binlog 位点、LSN 或 change stream
resume token。

```yaml
source:
  - type: mysql-cdc-source
    id: orders
    connection: mysql
    config:
      database: inventory
      table: orders
      schema: "id bigint, name string"
      snapshotMode: initial
```

MySQL 需要 row 格式 binlog 和完整行镜像；PostgreSQL 需要 `wal_level=logical`；MongoDB 需要
副本集或分片集群。MySQL schema history 默认通过 Hadoop FileSystem 存放在 checkpoint 下
（本地、HDFS 或对象存储）。由于失败重启可能重放最后一个未提交批次，且中断的初始 snapshot
会在重启后重新执行，应使用 upsert 或 CDC sink。详见
[build/doc/database-cdc.md](build/doc/database-cdc.md) 和
[source/database-cdc-source/README.md](source/database-cdc-source/README.md)。

### Connector 支持

| Connector | Connection | Catalog |
| --- | --- | --- |
| `kafka-source`, `kafka-sink` | kafka | — |
| `elasticsearch-source`, `elasticsearch-sink` | elasticsearch | — |
| `jdbc-source`, `jdbc-sink` | jdbc, mysql | — |
| `mysql-source`, `mysql-sink` | jdbc, mysql | — |
| `mongodb-source`, `mongodb-sink` | mongodb | — |
| `mysql-cdc-source` | mysql, jdbc | — |
| `postgres-cdc-source` | jdbc | — |
| `mongodb-cdc-source` | mongodb | — |
| `hive-source`, `hive-sink` | — | hive |
| `iceberg-source`, `iceberg-sink` | — | iceberg |
| `hudi-source`, `hudi-sink` | — | hudi |
| `paimon-source`, `paimon-sink` | — | paimon |
| `file-common-source`, `file-sink` | — | — |
| `console-sink` | — | — |

Catalog provider：

| Catalog | Connection | Spark catalog |
| --- | --- | --- |
| `hive` | hive-metastore（可选） | Hive session catalog |
| `iceberg` | hive-metastore、jdbc、mysql（可选） | 命名 catalog，或在名称为 `spark_catalog` 时作为 Hive session catalog |
| `jdbc` | jdbc、mysql（可选） | 命名 catalog |
| `hudi` | hive-metastore（可选） | Hudi session catalog |
| `paimon` | hive-metastore、jdbc、mysql（可选） | 命名 catalog，或 generic session catalog |
| `gravitino` | gravitino | 解析为上述某个原生 catalog |

远程 Hive metastore 设置由 `hive` catalog 安装。Spark 每个 session 只支持一个 session
catalog；包含多个 Hive、Hudi、generic Paimon 或名称为 `spark_catalog` 的 Iceberg catalog 的
作业会被拒绝。Iceberg、JDBC 和命名 Paimon catalog 可以组合，并作为命名 Spark catalog 安装。

### MongoDB

`mongodb-source` 使用官方 MongoDB Spark connector。connection 提供连接串（凭据从 `auth`
注入），source 选择资源：

```yaml
source:
  - type: mongodb-source
    id: orders
    connection: mongo-prod
    config:
      database: app
      collection: orders
      options:
        readPreference.name: secondaryPreferred
```

为了兼容已有作业，在没有引用 connection 时接受内联 `config.uri`（或
`config.connectionUri`）。使用 `spark.mongodb.` 命名空间的 connection 属性会原样传给
connector；其他属性会作为 MongoDB URI 选项追加。

`mongodb-sink` 使用同一个 connection，并写入配置的 `database` 和 `collection`。支持
insert/replace/update、id 字段、upsert、batch 和 streaming 写入。见
[sink/mongodb-sink/README.md](sink/mongodb-sink/README.md)。

Kafka 和 Elasticsearch sink 见 [sink/kafka-sink/README.md](sink/kafka-sink/README.md) 和
[sink/elasticsearch-sink/README.md](sink/elasticsearch-sink/README.md)。

## 增量同步

Hudi、Paimon、Iceberg 和 JDBC source 可以执行有界增量读取。source 选择一种语义，每个 sink
自动应用该语义：

| 语义 | Source 输出 | Sink 行为 |
| --- | --- | --- |
| `append` | 仅新行 | append |
| `upsert` | 每个 key 的最新状态 | 按 `keyFields` 合并 |
| `cdc` | 行以及 `I`/`U`/`D` 操作列 | insert、update 和 delete |

无法应用该语义的 sink 会在 Spark 启动前的配置校验阶段被拒绝。`hudi-sink`、`paimon-sink`、
`iceberg-sink`、`jdbc-sink` 和 `mysql-sink` 支持 append、upsert 和 CDC。`hudi-source`
支持 upsert 和 CDC，`paimon-source` 支持 append 和 CDC，`iceberg-source` 支持 append、
upsert 和 CDC；`jdbc-source`/`mysql-source` 通过顶层 `cursorField` 支持 append 和 upsert。
Iceberg upsert/CDC 的 changelog 读取需要 catalog 表；JDBC 表快照无法感知 delete。

```yaml
job:
  mode: sync
  strategy: incremental
source:
  - id: hudi_orders
    type: hudi-source
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      incremental:
        semantics: cdc
        keyFields: [id]
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
sink:
  - id: paimon_orders
    type: paimon-sink
    catalog: paimon_lake
    config:
      table: sales.orders
      primaryKey: [id]
      mode: append
    dependency: hudi_orders
```

第一次运行需要 `start` 或 `initial: latest|full`；增量状态存储进度和待提交事务元数据，但不负责
引导读取。只有所有 sink 成功后，游标才会提交到配置的增量状态中。
单写者锁保护状态，在 sink 写入前会记录待提交事务。待提交记录包含 connector 配置、transform
链以及解析后的非敏感 connection/catalog 设置的计划指纹，因此恢复时如果 source、sink、
transform、key 或相关配置发生变化会被拒绝。JDBC、Redis 和 etcd 锁可以是可续租的 lease
（`lockTtlMs`）；lease 会暴露 fencing token，锁被接管的 writer 会失败，而不是覆盖更新的状态。
文件系统后端（包括 HDFS/S3A 状态路径）没有原子 compare-and-swap，并且拒绝 `lockTtlMs`；
崩溃的 writer 使用 `pudding state unlock` 解锁。对象存储上的 filesystem 状态必须显式设置
`incremental.allowBestEffortLock=true` 才能使用；生产环境需要严格单写者时请使用 JDBC、
Redis 或 etcd。如果进程在 sink 和游标提交之间停止，
`incremental.recovery` 控制是否重放批次（`auto` 会重试幂等 upsert/CDC merge 和带有事务 id
的 append 提交），或者要求人工决定：

```shell
bin/pudding state show <state-target> [source-id]
bin/pudding state status <state-target> [source-id] --json
bin/pudding state resolve <state-target> <source-id> --commit|--abort
bin/pudding state reset <state-target> <source-id> --cursor <cursor>
bin/pudding state unlock <state-target> <source-id>
bin/pudding state migrate <jdbc-state-target>
```

默认后端是 `statePath` 选择的文件系统。作业也可以选择 JDBC、Redis 或 etcd；它们都暴露尝试
次数、最近提交/失败元数据、锁和 fencing 信息，并提供相同的恢复命令：

```yaml
incremental:
  state:
    backend: redis
    host: redis.internal
    port: 6379
    password: ${secret:env/REDIS_PASSWORD}
    prefix: pudding:incremental:
    lockTtlMs: 600000
```

```yaml
incremental:
  state:
    backend: jdbc
    url: jdbc:postgresql://state-db:5432/pudding
    user: pudding
    password: ${secret:env/STATE_DB_PASSWORD}
    driver: org.postgresql.Driver
    table: pudding_incremental_state
    lockTtlMs: 600000
```

Redis 支持 `mode: single|sentinel|cluster`；Sentinel 使用 `masterName` + `sentinels`（以及
可选的 `sentinelUser`/`sentinelPassword`），Cluster 使用 `clusterNodes`。etcd 在
`lockTtlMs` 为正数时附加原生 lease。在 lease 支持之前创建的 JDBC 状态表可以使用
`pudding state migrate` 升级；lease 以 epoch 毫秒存储在 `lock_expires_at_ms` 中。发行包还
包含 `libs/hadoop-aws-runtime-<version>.jar` 以及 OSS/Azure/GCS/COS runtime pack，用于对象存储状态路径和 source/sink；如果 Spark
镜像已经提供兼容的 Hadoop/AWS SDK 组合，可以删除该 jar。
其他后端可以实现 `IncrementalStateBackendProvider` SPI 并继承
`AbstractIncrementalStateBackend`。JDBC DDL/迁移、Redis 模式、锁 lease 和 fencing 规则、
properties 文件状态目标以及 `status --json` 字段见
[build/doc/incremental-sync.md](build/doc/incremental-sync.md)。

增量计数器可以通过 JMX 在 `org.pudding.core.incremental:type=IncrementalMetrics` 获取；
driver 还会为锁和事务状态变化输出结构化的 `incremental.*` 审计事件。

`job.mode: sync` 搭配 `job.strategy: incremental` 需要一个增量 source 和一个增量 sink。
只有保留 key、operation 和 ordering 元数据的 transform 才允许使用。增量同步是 batch-only，
且使用一个 sink，因此进度只有一个 writer。完整契约见
[build/doc/incremental-sync.md](build/doc/incremental-sync.md) 和
[build/doc/job-modes.md](build/doc/job-modes.md)。

## 流式摄入

持续 Kafka 摄入使用 `job.mode: streaming`。它和 batch 的 `sync incremental` 日志是不同机制：
游标进度存储在 sink 的 Spark checkpoint 中，而不是增量状态后端。

```yaml
job:
  mode: streaming
  trigger: "processing-time: 30 seconds"

source:
  - type: kafka-source
    id: kafka_orders
    config:
      subscribe: orders
      valueFormat: json
      valueSchema:
        id: long
        name: string
        dt: string
        __op: string
      cdcFormat: op-column

sink:
  - type: hudi-sink
    id: hudi_orders
    dependency: kafka_orders
    config:
      table: orders
      tablePath: hdfs://namenode:8020/hudi/orders
      operation: upsert
      recordKeyFields: [id]
      cdcOpColumn: __op
      cdcOrderFields: [_kafka_timestamp, _kafka_partition, _kafka_offset]
      checkpointLocation: hdfs://namenode:8020/checkpoints/hudi-orders
      outputMode: append
```

支持的组合：

| 目标 | Append | Update / Upsert | CDC |
| --- | --- | --- | --- |
| `hudi-sink` | 原生 streaming（path 或 catalog location） | `foreachBatch` Hudi upsert，需要 `recordKeyFields` | `foreachBatch` delete + upsert |
| `paimon-sink` | path 或 catalog streaming | `foreachBatch` Paimon 主键 merge | `foreachBatch` + `_row_kind_` |
| `iceberg-sink` | 原生 streaming append 或 `operation: upsert` | `foreachBatch` + `MERGE INTO`，需要 `recordKeyFields` | `foreachBatch` + `MERGE INTO` |
| `elasticsearch-sink` | 原生 streaming append | 原生 update，需要 `idField` | 通过 `operation` 配置 |
| `jdbc-sink`、`mysql-sink` | `foreachBatch` JDBC append | `foreachBatch` staging + `MERGE` 或按 key update/insert，需要 `recordKeyFields` | 不支持（不会删除行） |

规则和限制：

- 原生 MySQL/PostgreSQL/MongoDB CDC source 输出同样的 `I`/`U`/`D` 操作契约，并且仅支持
  streaming。`schema` 为必填项，CDC/upsert sink 应使用相同的 key 列。MongoDB delete 行只带
  `_id`，因此声明的 schema 必须包含 `_id`。
- Kafka source 支持 `json`、Confluent Schema Registry `avro`/`protobuf`，以及通用
  `op-column` 或 Debezium `before`/`after` CDC 信封；必须配置值 schema，JSON 的
  `parseMode` 默认 fail-fast。Avro/Protobuf 会自动选择可选
  `kafka-schema-registry-runtime`。Confluent wire format 中嵌入的 schema id 是权威来源：
  `schemaRegistry.subject` 用于需要按 subject 解析的场景，但当消息带有 schema id 时不要求
  subject 存在；未知 schema id 会失败，错误信息包含 topic 和 id。
- Streaming 作业只接受一个 sink，因此一个 checkpoint 拥有 Kafka offset。所有模式都只接受一个
  sink；多目标管道请拆成多个作业。
- Hudi 和 Paimon streaming 接受 catalog 表并解析其存储 location。Paimon 1.3.2 + Spark 3.4
  没有原生 catalog `toTable` streaming writer，因此 append 使用解析出的 path，update/CDC 使用
  直接的主键 writer。Iceberg streaming 需要 Iceberg catalog。三种格式都要求目标表已存在。
- Hudi、Iceberg、Paimon、Elasticsearch、JDBC 和 MySQL 支持 `outputMode: append` 或 `update`。
  `update` 需要 `recordKeyFields`（Elasticsearch 需要 `idField`），用于有状态聚合的变更输出；
  file、Kafka 和 MongoDB streaming sink 只支持 append；`complete` 仅用于 console 展示。湖仓
  和 JDBC sink 会拒绝 `continuous` trigger。
- 有状态 streaming checkpoint 会保存 source、transform 链和 Spark 版本的指纹。修改有状态
  配置后重启会在查询开始前失败；无状态 streaming 作业不受影响。见
  [build/doc/streaming-state.md](build/doc/streaming-state.md)。
- CDC 是 checkpoint 绑定的 at-least-once，配合按 key 幂等写入；Kafka producer 重复和跨分区
  乱序不会被消除。微批会按 `cdcOrderFields` 或 Kafka/Debezium 的 ordering 元数据按 key 去重。
- `_kafka_*` 和 `_cdc_*` 列是内部簿记列，CDC sink 会在写入前删除。
- `checkpointRetention` 默认 `keep`。`delete-on-success` 是 opt-in，只允许 `once` 或
  `available-now`；只有在有限查询已经消费完输入并有明确完成证据后才删除 checkpoint。外部
  `query.stop()` 在完成证据出现前发生、查询失败或超时都会保留 checkpoint。作业之外可以使用
  `bin/pudding checkpoint status --json|show|delete|cleanup` 检查或删除 checkpoint；
  `cleanup --keep-last N` 保留最新 N 个；`--conf-file` 可提供 S3A、OSS、GCS 等 Hadoop 文件系统
  的 `fs.*` 设置。见
  [build/doc/streaming-checkpoints.md](build/doc/streaming-checkpoints.md)。推荐把 checkpoint
  放在 HDFS、NFS 或持久卷，把对象存储留给湖仓数据；如果 checkpoint 必须放对象存储，请使用
  独立前缀和单 writer。
- Streaming 查询计数器通过 JMX 暴露在
  `org.pudding.core.streaming:type=StreamingMetrics`，并输出结构化的
  `streaming.query.started`、`streaming.query.progress`、`streaming.query.completed` 和
  `streaming.query.failed` 日志事件。有状态查询还会暴露最新事件时间 watermark 及其延迟
  （`streaming.query.last.watermark.ms`、`streaming.query.watermark.lag.ms`），以及聚合的
  state 行数、内存、删除行和 watermark 丢弃行（`streaming.state.*`）。
- Kafka streaming 增加 driver 侧 lag 和健康指标：`streaming.source.lag.records`、
  `.lag.max`、`.partitions`、`.lag.updated.ms` 和 `.healthy`。可用
  `streaming.query.last.progress.timestamp.ms` 计算进度年龄。latest-offset 查询由
  `lagMetricsIntervalMs` 节流，且不会导致查询失败。
- 有状态的 streaming 窗口聚合和去重要求事件时间列和 watermark；见
  [build/doc/streaming-state.md](build/doc/streaming-state.md)。

示例：`build/examples/stream_kafka_to_hudi.yaml`、`stream_kafka_to_paimon.yaml`、
`stream_kafka_to_iceberg.yaml`、`stream_window_aggregate.yaml`、`stream_window_update.yaml`、
`stream_dedup.yaml` 和 `stream_stream_join.yaml`。完整列表见
[build/examples/README.md](build/examples/README.md)。

## 表维护

`bin/pudding maintenance` 通过原生 Spark SQL procedure 执行一次性的 Hudi、Paimon 或 Iceberg
表维护。它独立于作业模式和 checkpoint/state 工具，执行节奏交给外部调度器。

```shell
SPARK_MASTER=local[*] bin/pudding maintenance iceberg status \
  --catalog lake --table db.orders \
  --conf-file examples/maintenance/iceberg-catalog.properties --json

SPARK_MASTER=local[*] bin/pudding maintenance iceberg rewrite-data-files \
  --catalog lake --table db.orders --target-file-size 128MB --min-input-files 5 \
  --conf-file examples/maintenance/iceberg-catalog.properties

SPARK_MASTER=local[*] bin/pudding maintenance hudi compact \
  --path hdfs:///warehouse/orders --option op=scheduleAndExecute
```

支持的 action：Iceberg 为 `status`、`rewrite-data-files`、`rewrite-manifests`、
`expire-snapshots`、`remove-orphan-files`；Paimon 为 `status`、`compact`、
`compact-manifests`、`expire-snapshots`、`remove-orphan-files`；Hudi 为 `status`、`clean`、
`compact`、`cluster`、`archive`。破坏性操作需要 `--confirm`；`--dry-run` 只预览或调用原生
dry-run。`--conf-file` 只接受 `spark.*`，建议权限保持 `0600`。安全规则、各格式行为和
`TableMaintenanceService` 编程接口见
[build/doc/table-maintenance.md](build/doc/table-maintenance.md)。

## JDBC source 和 sink

`jdbc-source` 和 `jdbc-sink` 使用 Spark 内置 JDBC data source 读写任意 JDBC 表。发行包内嵌
PostgreSQL 42.7.3 和 MySQL 8.0.30 driver；其他数据库需要将 driver 放到 Spark driver 和
executor classpath。

```yaml
source:
  - type: jdbc-source
    id: orders
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_ro
      password: '<read from a secret store; never commit>'
      driver: org.postgresql.Driver    # jdbc:postgresql URL 可省略
      options:
        fetchsize: '500'
sink:
  - type: jdbc-sink
    id: orders_out
    dependency: orders
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders_copy
      user: pudding_rw
      password: '<read from a secret store; never commit>'
      mode: overwrite                 # append | overwrite | ignore | error | errorifexists
      options:
        batchsize: '500'
```

Source 的 key 包括 `url`（必须以 `jdbc:` 开头）、`table`、`user`、`password`（可以为空）、
可选的 `driver` 和 `options`。PostgreSQL URL 会推断 `org.postgresql.Driver`；其他 URL 必须
设置 `driver`。`options` 接受字符串、布尔值或数字，并传给 Spark JDBC；顶层 connection key
优先。两个 connector 也接受 `connection: <name>` 引用；connection 值优先，内联 `user`、
`password` 和 `driver` 在 connection 未提供时作为 fallback。

JDBC source 支持 batch 和 sync incremental 游标读取；sink 支持 `batch`、`streaming` 和
sync incremental 写入。sink 默认 `overwrite`，可以将 `mode` 设置为 `append`、
`overwrite`、`ignore`、`error` 或 `errorifexists`。在 `sync` + `strategy: full` 下，
`jdbc-sink` 和 `mysql-sink` 会先写入 staging 表，再在单个事务中替换目标行；原子路径要求
目标表已经存在，MySQL 表需要使用 InnoDB 等事务引擎。

### JDBC 增量 source

在 source 顶层配置单调递增的游标列，并增加 `incremental` 块：

```yaml
job:
  mode: sync
  strategy: incremental
source:
  - type: jdbc-source
    id: orders
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_ro
      password: ${secret:env/ORDERS_DB_PASSWORD}
      cursorField: updated_at
      incremental:
        semantics: upsert        # append | upsert
        keyFields: [id]          # upsert 必填
        initial: full            # full | latest，仅首次运行
        statePath: hdfs://namenode:8020/pudding/state/orders
sink:
  - type: jdbc-sink
    id: orders_copy
    dependency: orders
    config:
      url: jdbc:postgresql://db.internal:5432/warehouse
      table: public.orders_copy
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
```

- 读取范围为 `cursorField > start` 且 `cursorField <= end`。`start` 来自增量状态、
  `incremental.start` 或 `initial=latest`；`end` 默认取当前最大值。
- 支持 numeric、date、time、timestamp 和 string 游标。游标列从目标表元数据中引用并引用化，
  游标值会转义后拼入 SQL，不会作为原始 SQL 片段执行。
- `append` 返回新增行；`upsert` 返回范围内每个 key 的最新完整行。表快照无法感知 delete，
  需要删除传播时使用 CDC source。
- `cursorField` 属于 source 配置而不是状态文件；与其它 source 配置一样，变更会由待提交
  事务的计划指纹拒绝。
- `initial=latest` 需要表中至少有一个非空游标值；空表请使用 `initial=full` 或显式
  `incremental.start`。
- source 只读取一张表。首次运行可以设置 `incremental.start`，之后以已提交游标为准。

### JDBC CDC sink

`jdbc-sink` 和 `mysql-sink` 在 `sync` + `incremental` 下支持 `cdc`，前提是 source 输出操作列
和排序元数据：

```yaml
sink:
  - type: jdbc-sink
    id: orders_target
    dependency: orders_cdc
    config:
      url: jdbc:postgresql://db.internal:5432/warehouse
      table: public.orders
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
      cdcOpColumn: __op
      cdcOrderFields: [_cdc_timestamp, _cdc_sequence]
```

sink 会保留每个 key 的最后一次操作，并在一个数据库事务中执行 delete 和 upsert；目标表需要
有与增量 keyFields 匹配的主键或唯一索引。`append` 不保证重放幂等，`upsert` 和 `cdc`
对相同行是重放安全的。该路径与 streaming 的 `cdcOpColumn` 不同，streaming JDBC 仍然拒绝
delete 语义。

Streaming 写入通过 `foreachBatch` 实现，并要求 `checkpointLocation`：

```yaml
sink:
  - type: jdbc-sink
    id: orders_stream
    dependency: orders_stream
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_rw
      password: '<read from a secret store; never commit>'
      checkpointLocation: hdfs://namenode:8020/checkpoints/jdbc-orders
      outputMode: update            # append | update
      recordKeyFields: [id]
```

- `outputMode: append` 使用 Spark JDBC append 写入每个微批；目标表不存在时会根据第一个非空
  微批创建。
- `outputMode: update` 把每个微批视为完整行，并按 `recordKeyFields` upsert。目标表必须已经
  存在，且具有与配置 key 完全一致的主键或唯一索引。streaming 行必须包含目标的全部列；额外的
  `_kafka_*`/`_cdc_*` 内部列会被删除。
- PostgreSQL 和 H2 使用 `MERGE`；MySQL 和 MariaDB 先执行按 key 的 `UPDATE ... JOIN`，再执行
  `INSERT ... WHERE NOT EXISTS`。每个微批先写入私有 staging 表，再在单个数据库事务中 merge，
  最后删除 staging 表。MySQL 路径只按 `recordKeyFields` 匹配；与
  `ON DUPLICATE KEY UPDATE` 不同，不会被其他唯一索引影响。null key 或微批内重复 key 会直接
  失败，而不是任选一行。
- JDBC streaming 不删除行，因此会拒绝 `cdcOpColumn`。streaming sink 的 `mode` 只能是
  `append`；需要 upsert 时使用 `outputMode: update`。
- 对内容未变化的完整行，merge 是幂等的，因此 checkpoint 在提交后重放不会产生重复数据。若有
  其他 writer 并发修改相同 key，则不构成 exactly-once；请保证同一组 target key 只有一个 writer。
- Streaming upsert 支持 PostgreSQL、MySQL、MariaDB 和 H2；其他 JDBC 数据库仍只支持 batch，
  除非扩展新的 `MERGE` 方言。

校验错误会指出出错的 key，但不会输出 URL、密码或 option 值，作业配置日志使用 core 的脱敏器。
包含凭据的作业文件请保持 `0600` 权限，并在提交后删除；优先从 secret store 注入凭据，不要把
凭据提交到代码库。

`jdbc-source`、`jdbc-sink`、`mysql-source` 和 `mysql-sink` 共享
`libs/jdbc-common-<version>.jar`。client 会自动加入；直接使用 `spark-submit` 的作业必须在
`--jars` 中包含它。

## Transform 算子

`sql-transform` 在一个或多个依赖上执行 SQL。`basic-transform` 模块提供常用的 DataFrame 算子：

| 类型 | 用途 | 依赖数 |
| --- | --- | --- |
| `filter-transform` | 使用 Spark SQL 条件过滤行 | 恰好 1 |
| `aggregate-transform` | 使用 SQL 表达式分组聚合；streaming 支持事件时间窗口 | 恰好 1 |
| `dedup-transform` | 按 key 保留一行；streaming 状态由 watermark 界定 | 恰好 1 |
| `union-transform` | 使用 `union` / `unionByName` 合并行 | 2 个或更多 |
| `column-transform` | 选择、重命名和转换列 | 恰好 1 |
| `limit-transform` | 限制行数 | 恰好 1 |
| `join-transform` | Join 两个 DataFrame；支持带 watermark 的 inner stream-stream join | 恰好 2 |
| `validate-transform` | 校验必需列和 SQL 规则；失败、丢弃或隔离非法行 | 恰好 1 |
| `schema-contract-transform` | 输入 schema 与声明的列/类型不一致时失败 | 恰好 1 |

`filter-transform`、`column-transform`、`union-transform` 以及 `mode: drop` 的
`validate-transform` 也可以在 `streaming` 作业中运行。`sql-transform` 支持无状态 streaming
SQL。`aggregate-transform` 通过显式的 `eventTime`、`watermark` 和事件时间 `window` 支持有状态
streaming 窗口聚合；`dedup-transform` 支持由 watermark 界定状态的 streaming 去重。
`join-transform` 在双侧声明事件时间和 watermark 时支持 inner stream-stream join；outer 和
semi/anti streaming join 仍不支持。`limit-transform` 和有状态 SQL 仍仅支持 batch。详见
[build/doc/streaming-state.md](build/doc/streaming-state.md)。

```yaml
transform:
  - type: filter-transform
    id: filter1
    dependencies: [source1]
    config:
      condition: "amount >= 100"

  - type: column-transform
    id: columns1
    dependencies: [filter1]
    config:
      fields:
        - order_id
        - name: city
          source: order_city
        - name: amount
          type: double

  - type: aggregate-transform
    id: aggregate1
    dependencies: [columns1]
    config:
      groupBy: [city]
      aggregations:
        order_count: "count(*)"
        total_amount: "sum(amount)"
```

每个算子都支持可选的 `numPartitions`。`union-transform` 和 `join-transform` 接受多个依赖；
`byPosition` 模式的 `union-transform` 和 `join-transform` 需要显式的 input/left/right 名称，
因为运行时依赖 map 不保证顺序。使用这些类型时 client 会自动加入 `basic-transform` jar。
完整选项和 join 语义见 [transform/basic-transform/README.md](transform/basic-transform/README.md)。

## Capability manifest

每个发行包根目录都有生成的 `capabilities.yaml`。它枚举该发行包中准确的 source、sink、
transform、connection provider 和 catalog provider，包括提供每个组件的 jar。Provider 组件
指向 `connection-providers-*.jar` 或 `catalog-providers-*.jar`。Hudi、Paimon、Iceberg、
Elasticsearch、MongoDB、Kafka 以及可选的 Kafka Schema Registry runtime 是支持性产物，不是
组件，因此不会单独列出；
client 会为作业使用的每个 connector 添加对应的 runtime jar。

```yaml
schemaVersion: "2.1"
productVersion: "1.0.0"
sparkVersion: "3.4.0"
scalaBinaryVersion: "2.12"
components:
  - kind: source
    type: mongodb-source
    className: org.pudding.source.MongodbSource
    module: mongodb-source
    jar: mongodb-source-1.0.0.jar
    capabilities:
      connectionTypes: [mongodb]
      catalogTypes: []
      executionModes: [batch]
      syncIncrementalSemantics: []
      lakeSyncIncremental: false
```

`executionModes` 表示内部 batch/streaming 支持；`streamingOutputModes` 列出 sink 的
streaming 输出模式（`append`、`update`、`complete`），`streamingUpsert` 标记基于 key 的
update 语义；`syncFullRefresh` 表示 sink 可以为 `sync` + `full` 替换目标内容；
`syncAtomicReplace` 表示该替换在对应后端上是原子的，否则需要显式设置
`allowNonAtomicReplace`；`syncIncrementalSemantics` 列出 source 或 sink 在
`sync` + `incremental` 下支持的契约（`append`、`upsert` 或 `cdc`）；
`idempotentSyncIncrementalSemantics` 列出可以安全重放未提交批次的 sink 语义；
`syncIncrementalFencing`、`lakeSyncIncremental` 和 `preservesSyncIncrementalMetadata` 描述
剩余 sync 能力。后端可以使用与引擎相同的规则预检管道。后端应读取即将运行的发行包中的
manifest，而不是自己 classpath 中的 manifest。加载是严格的：缺少根 manifest 会被拒绝。
manifest 在构建时根据实际 ServiceLoader 注册生成，因此能力变化不需要手工维护列表。Schema
`1.0` 会被拒绝且不迁移；`2.0` 仍可读取。请重新生成 manifest，或把第三方 fragment 改写为
`2.0` 或 `2.1`。

第三方 connector 可以在放到 `libs` 下的 jar 中加入
`META-INF/spark-pudding/component.yaml` fragment；后端会递归扫描 jar 并合并 fragment，拒绝
重复的 `kind:type`。core 中的 capability API（`CapabilityManifest.load`、
`CapabilityManifest.verifyDistribution`、`CapabilityManifest.diff`）打包在
`bin/pudding-client.jar` 中，并提供 `bin/pudding capabilities verify|show|diff` 命令。请按产品
版本保存 manifest 快照，用于检测新增、删除和变更的能力。schema、fragment 规则、严格加载行为
和后端用法见 [build/doc/capability-manifest.md](build/doc/capability-manifest.md)。

## 使用 pudding submit 提交作业

`pudding submit` 是推荐的作业运行方式。它根据 `job.master` 推导执行目标，加入作业需要的
引擎、连接器、provider 和 runtime jar，以私有临时文件或 Kubernetes Secret 的方式传递作业
配置，并返回真实的作业结果。环境文件必填，可以为空。

前台运行是调度器应使用的方式：进程只在作业结束时退出。

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml"
```

通过可选的第三个参数后台运行并跟踪日志：

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml" \
  "$PUDDING_HOME/log/my-job.log"

tail -f "$PUDDING_HOME/log/my-job.log"
```

执行目标跟随 `job.master`：local master 使用 `local` 目标（client 模式），`yarn` 使用 YARN
（默认 cluster 模式，也接受 client 模式），`k8s://` 使用 Kubernetes（只支持 cluster 模式）。
需要镜像、namespace 或 service account 时使用 `bin/pudding k8s submit`，它还会自动创建并
删除作业配置 Secret。带日志文件时，命令在后台启动作业后返回；不带日志文件时保持附着，只有
作业到达 `FINISHED` 才返回 `0`。

统一入口只暴露常用路径。需要 `--target`、`--master`、`--resourceBase`、`--conf` 或其它目标
覆盖选项时，直接运行 `bin/pudding-client.jar`：

```shell
"$PUDDING_HOME/bin/pudding-client.jar" \
  -c "$PUDDING_HOME/conf/env.properties" \
  -j "$PUDDING_HOME/examples/read_csv_example.yaml" \
  --target local --conf spark.driver.memory=1g
```

SDK 和全部 client 选项见 [client/README.md](client/README.md)；每个 `pudding` 子命令的完整
示例见 [build/doc/cli.zh-CN.md](build/doc/cli.zh-CN.md)。

## 使用 spark-submit 运行引擎

直接启动引擎是 `pudding submit` 之外的另一种方式。管道通过 `--confType` 以及 `--jobText`、
`--jobEnv` 或 `--jobFile` 中的恰好一个传入：

```shell
"$SPARK_HOME/bin/spark-submit" \
  --class org.pudding.core.PuddingSpoon \
  --master yarn \
  --deploy-mode cluster \
  --files "$PUDDING_HOME/examples/read_csv_example.json" \
  --jars "$PUDDING_HOME/libs/console-sink-1.0.0.jar,$PUDDING_HOME/libs/file-common-source-1.0.0.jar" \
  "$PUDDING_HOME/libs/pudding-core-1.0.0.jar" \
  --confType json \
  --jobFile read_csv_example.json
```

上面的示例只使用 file source 和 console sink。使用 connection 或 catalog 的作业还必须把
`libs/connection-providers-<version>.jar` 和 `libs/catalog-providers-<version>.jar` 加入
`--jars`；client 会自动加入。使用 Hudi、Paimon、Iceberg、Elasticsearch、MongoDB 或 Kafka
的作业还必须额外加入对应的 `libs/*-runtime-<version>.jar`；Kafka 作业使用
`valueFormat: avro` 或 `protobuf` 时还需要
`libs/kafka-schema-registry-runtime-<version>.jar`。JDBC 和 MySQL source/sink 作业必须加入
`libs/jdbc-common-<version>.jar`。

在 cluster 模式下，application jar、`--jars` 条目和数据路径必须对集群可见（例如 HDFS URI）。
优先使用 `--jobFile`（通过 `--files` 暂存），或使用 `--jobEnv` 搭配
`spark.yarn.appMasterEnv.<NAME>`；YARN cluster 模式的 driver 不会继承提交主机的环境，因此
裸 `--jobEnv` 只对 local 或 client 模式 driver 可靠。内联 `--jobText` 会出现在进程列表和
日志中。文件必须对 driver 进程可读；client SDK 会写入仅 owner 可读的临时文件，并在应用结束
时删除。Kubernetes 请将配置存入 Secret 并注入 driver：

```shell
--conf spark.kubernetes.driver.secretKeyRef.PUDDING_JOB_CONFIG=<secret>:<key>
```

Kubernetes target 会自动完成该操作，绝不会把作业配置放到 spark-submit 命令行。除非使用
`bin/pudding k8s submit`（它会自动创建 Secret），否则 `sparkPuddingClient` CLI 需要
`--k8s-job-secret`。使用 `SubmissionManager` 的长生命周期服务可以为每次提交请求 SDK 管理的
Secret（`managedJobSecret()`）；manager 创建 `pudding-job-<submissionId>`，只记录其
名称/key/所有权，并在最终状态或启动失败后删除；对账和启动时也会重试清理。

## 测试

`mvn verify` 运行单元测试和本地集成套件。Hudi/Iceberg/Paimon 的语义、catalog/path 和全量
原子替换回归套件见 [build/doc/lakehouse-matrix.md](build/doc/lakehouse-matrix.md)。外部服务测试
是 opt-in，环境变量缺失时会跳过：

| 测试 | 环境 | 用途 |
| --- | --- | --- |
| `RedisStateBackendTest` | `PUDDING_TEST_REDIS_HOST`/`_PORT`，或 `PUDDING_TESTCONTAINERS=true` | 状态日志、锁 lease、fencing、Sentinel/Cluster 配置和 SPI 加载。 |
| `EtcdStateBackendTest` | `PUDDING_TEST_ETCD_ENDPOINT`，或 `PUDDING_TESTCONTAINERS=true` | 通过 etcd v3 HTTP gateway 和原生 lease 验证同一契约。 |
| `RemoteFileSystemStateBackendTest` | `PUDDING_TEST_STATE_URI`，可选 `PUDDING_TEST_STATE_CONF` | HDFS 或 S3A 状态目录、显式解锁恢复、清理以及 checkpoint CLI show/status/delete。请使用一次性 base URI。 |
| `ContainerRemoteStateTest` | `PUDDING_TESTCONTAINERS=true` | Testcontainers 启动的 HDFS（NameNode/DataNode）和 MinIO S3A 状态目录、待提交事务恢复、checkpoint 查看/删除以及 checkpoint 重启/delete-on-success。 |
| `IncrementalStateCrashRecoveryTest` | 无 | 启动独立 JVM，在待提交事务写入后 kill 进程，并恢复日志和锁。 |
| `BuildJobCrashRecoveryTest` | 无 | 在子 JVM 中运行真实 BuildJob，在 sink 提交后 kill，并验证一次 sink 写入和游标恢复。 |
| `KafkaSourceStreamingIntegrationTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_KAFKA_IMAGE` | Kafka 容器、JSON/CDC 解码和 Structured Streaming offset 处理。 |
| `IcebergS3FileIOTest` | `PUDDING_TESTCONTAINERS=true`，或 `PUDDING_TEST_S3_ENDPOINT` + 可选 access/secret key | MinIO S3FileIO 数据层读写（AWS SDK + JdbcCatalog）。 |
| `HudiObjectStoreTest` | `PUDDING_TESTCONTAINERS=true`，或 `PUDDING_TEST_S3_ENDPOINT` + 可选 access/secret key | Hudi 表数据、commit、增量读、full refresh 和 S3A checkpoint。 |
| `PaimonObjectStoreTest` | `PUDDING_TESTCONTAINERS=true`，或 `PUDDING_TEST_S3_ENDPOINT` + 可选 access/secret key | Paimon catalog 表、snapshot、CDC、full refresh 和 S3A checkpoint。 |
| `JdbcStreamingTest` | 无 | H2 `MERGE`、JDBC upsert 校验和 staging 清理。 |
| `JdbcStreamingSinkTest` | 无 | H2 端到端 checkpointed `append` 和 `update` streaming（`jdbc-sink`）。 |
| `JdbcSinkPostgresContainerTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_POSTGRES_IMAGE` | PostgreSQL 容器、`jdbc-sink` 原子全量替换、回滚、streaming upsert 和 staging 清理。 |
| `MysqlSinkContainerTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_MYSQL_IMAGE` | MySQL/InnoDB 容器、`mysql-sink` 原子全量替换、回滚、streaming upsert 和 staging 清理。 |
| `MysqlCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_MYSQL_IMAGE` | MySQL binlog snapshot、insert/update/delete 和 checkpoint 重启。 |
| `PostgresCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_POSTGRES_IMAGE` | PostgreSQL `pgoutput` snapshot、insert/update/delete 和 checkpoint 重启。 |
| `MongodbCdcSourceTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_MONGO_IMAGE` | MongoDB 副本集 snapshot、change stream、updateLookup/delete 和 resume token 重启。 |
| `DatabaseCdcHudiPaimonTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_MYSQL_IMAGE` | MySQL CDC → Hudi/Paimon CDC sink；snapshot、DML、checkpoint 重启和最终目标行校验。 |
| `DatabaseCdcIcebergTest` | `PUDDING_TESTCONTAINERS=true`，可选 `PUDDING_TEST_MYSQL_IMAGE` | MySQL CDC → Iceberg CDC sink；snapshot、DML、checkpoint 重启和最终目标行校验。 |

设置 `PUDDING_TESTCONTAINERS_REQUIRED=true` 后，容器无法启动会直接失败而不是跳过；CI 工作流
会设置该变量。`PUDDING_TESTCONTAINERS_SKIP_S3=true` 允许在不拉取 MinIO 的情况下只跑本地
HDFS 迭代。`PUDDING_TEST_HDFS_IMAGE`、`PUDDING_TEST_MINIO_IMAGE`、
`PUDDING_TEST_KAFKA_IMAGE`、`PUDDING_TEST_POSTGRES_IMAGE`、`PUDDING_TEST_MYSQL_IMAGE` 和
`PUDDING_TEST_MONGO_IMAGE`
可以覆盖受限镜像仓库的容器镜像。真实 S3 运行方式和版本矩阵见
[build/doc/compatibility.md](build/doc/compatibility.md)。maintenance 模块还包含 Iceberg、
Paimon 和 Hudi 的本地 Spark 集成测试，随 `mvn verify` 运行，不需要外部服务。

示例：

```shell
PUDDING_TESTCONTAINERS=true \
  mvn -pl provider/redis-state-provider -am test

PUDDING_TEST_REDIS_SENTINEL_ENDPOINTS=redis-1:26379,redis-2:26379 \
PUDDING_TEST_REDIS_SENTINEL_MASTER=mymaster \
  mvn -pl provider/redis-state-provider -am test

PUDDING_TEST_STATE_URI=hdfs://namenode:8020/tmp/pudding-test \
PUDDING_TEST_STATE_CONF=/etc/hadoop/conf \
  mvn -pl core test
```

## 代码检查

项目遵循 Apache Spark 的 Scala 和 Java 风格规则：

```shell
mvn scalastyle:check
mvn -pl client checkstyle:check
```

`scalastyle` 是默认构建的一部分。`maven-checkstyle-plugin` 绑定在 Java `client` 模块的
`validate` 阶段，因此 `mvn verify`（甚至 `mvn -pl client validate`）会自动运行它。规则集为
[build/checkstyle.xml](build/checkstyle.xml)，范围很窄的例外应放入
[build/checkstyle/checkstyle-suppressions.xml](build/checkstyle/checkstyle-suppressions.xml)。
如果要找出还需要运行 Checkstyle 的新增 Java 模块，请搜索 `src/main/java` 下的 `*.java`。

## 安全

- 优先在 `connection` 和组件配置中使用 `${secret:env/NAME}` 或
  `${secret:file:/path#key}` 引用。引用在 driver 解析；client 永不解析。
- 作业文件和环境文件可能包含凭据。请确保只有提交用户可读（模式 `0600`），并在使用后删除临时
  副本。
- 引擎在记录配置日志时会脱敏类凭据值。client SDK 不会把作业配置放到命令行：local 和 YARN
  提交接收仅 owner 可读的 `--jobFile`，Kubernetes 接收 Secret 引用。因此详细命令日志只显示
  文件路径或环境变量名，不显示配置内容。
- Kubernetes 请把凭据放在 Secret 中，而不是 ConfigMap 或作业参数中。作业配置通过 Secret key
  引用注入；自动创建的 Secret 会在 `bin/pudding k8s submit` 退出时或所属提交到达最终状态时删除。
  见 [build/k8s/README.md](build/k8s/README.md)。

## 核心概念

| 名称 | 描述 |
| --- | --- |
| job mode | `batch`、`sync` 或 `streaming`；选择处理契约。 |
| sync strategy | `full` 或 `incremental`；仅 `job.mode: sync` 需要。 |
| execution mode | 内部 `batch` 或 `streaming`，由 job mode 推导。 |
| state backend | 增量作业的游标/日志存储：filesystem、JDBC、Redis 或 etcd，支持 provider SPI。 |
| connection | 可复用的 endpoint、凭据和客户端属性。由 source 和 sink 引用。 |
| catalog | 安装到 Spark session 的表元数据，例如 Hive、Iceberg、Hudi、Paimon 或 Gravitino。 |
| source | 通过 Spark data source 读取数据。接口由 `DataSource` 定义。 |
| transform | 应用 Spark 数据处理。接口由 `DataTransform` 定义。 |
| sink | 通过 Spark data sink 写入数据。接口由 `DataSink` 定义；每个模式只写入一个 sink。 |

## 仓库布局

```text
client/     提交 SDK 和命令行客户端
core/       稳定的 SPI、配置模型、provider 注册和 Spark 入口
connector/  共享 connector 支持库（当前为 jdbc-common）
provider/   connection、catalog 和可选状态后端 provider 实现
runtime/    共享 connector runtime 和 Hadoop 对象存储包（S3A/OSS/Azure/GCS/COS、CDC）
source/     source connector
transform/  transform 实现
sink/       sink connector
maintenance/ 表维护 API、Spark runner 和 CLI
build/      发行包组装、启动器和 Kubernetes 资源
integration/ 可选的对象存储、lakehouse 和原生 CDC 回归套件
```
