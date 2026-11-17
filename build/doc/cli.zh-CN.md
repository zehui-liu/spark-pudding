# pudding CLI 使用指南

`bin/pudding` 是 spark-pudding 发行包唯一受支持的命令行入口。它把多个内部脚本收敛为一个
入口：参数原样传递，`bin/lib/` 下的脚本属于内部实现，不应直接调用。

```shell
"$PUDDING_HOME/bin/pudding" help
"$PUDDING_HOME/bin/pudding" <command> --help
```

本文为每个命令提供可直接照抄的示例。提交 SDK、执行目标和生命周期 API 见
[../../client/README.md](../../client/README.md)；Kubernetes 集群准备见
[../k8s/README.md](../k8s/README.md)。

## 发行包结构

压缩包解压后是一个目录：

```text
spark-pudding-1.0.0/
  bin/
    pudding                  统一入口
    pudding-client.jar       `pudding submit` 使用的提交 client
    lib/                     内部命令实现
  conf/
    env.properties           导出到提交进程的环境变量
  libs/                      引擎、连接器和 provider jar
  examples/                  可直接运行的作业配置
  doc/                       本文档及其他参考文档
  log/                       后台作业的默认输出目录
  k8s/                       Kubernetes 示例作业和镜像文件
```

运行命令前先设置环境：

```shell
export PUDDING_HOME=/opt/spark-pudding-1.0.0
export JAVA_HOME=/path/to/jdk-11          # 也支持 JDK 17
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_HOME=/path/to/spark-3.4.0-bin-hadoop3
```

`checkpoint`、`state`、`maintenance`、`maintenance-server`、`k8s build-image` 以及会调用
`spark-submit` 的提交目标都需要 Spark 或 Hadoop 的类。未设置 `SPARK_HOME` 时，`checkpoint`
和 `state` 会退回到 `hadoop classpath`（要求 `hadoop` 在 `PATH` 中）。

## 命令总览

| 命令 | 用途 |
| --- | --- |
| `pudding submit` | 通过内置 client 提交作业 |
| `pudding checkpoint` | 查看或删除 streaming checkpoint |
| `pudding state` | 查看或修复增量同步状态 |
| `pudding capabilities` | 校验或比较能力 manifest |
| `pudding maintenance` | 执行一次湖仓表维护 |
| `pudding maintenance-server` | 启动 HTTP 表维护控制面 |
| `pudding k8s submit` | 提交 Kubernetes 作业 |
| `pudding k8s build-image` | 构建 Spark + spark-pudding 容器镜像 |
| `pudding k8s kind-setup` | 创建本地 kind 集群和 driver RBAC |
| `pudding verify` | 在源码树中校验发行包 |

## 提交作业

```shell
"$PUDDING_HOME/bin/pudding" submit <env.properties> <job-file> [log-file]
```

- `env.properties` 必填，可以为空。每个条目都会导出到提交进程，例如 `HADOOP_CONF_DIR`、
  `YARN_CONF_DIR`、`KUBECONFIG` 或 `JAVA_HOME`。包含凭据时请保持 `0600` 权限。
- `job-file` 是 JSON、YAML 或 HOCON 作业配置，必须声明 `job.mode` 且只有一个 sink。client
  会解析该文件，并以文件或 Secret 的方式交给 driver，不会出现在 `spark-submit` 命令行中。
- `log-file` 可选。提供时，client 使用 `nohup` 在后台启动作业后立即返回，并打印作业 PID 和
  日志路径；不提供时，client 保持附着，作业结束后返回真实退出码。

执行目标根据 `job.master` 推导：

| `job.master` | 目标 | deploy mode |
| --- | --- | --- |
| `local`、`local[*]`、`local[N]` | `local` | 只支持 `client` |
| `yarn`、`yarn-client`、`yarn-cluster` | `yarn` | 默认 `cluster`，也接受 `client` |
| `k8s://...` | `kubernetes` | 只支持 `cluster` |

也可以在 client 中用 `--target` 强制指定目标，用 `--master` 覆盖 `job.master`。统一入口的
`submit` 不暴露这些选项；需要时请直接使用 client jar（见
[client 覆盖选项](#client-覆盖选项)）。

只有作业到达 `FINISHED` 时 `pudding submit` 才返回 `0`。`FAILED`、`KILLED`、`LOST`、提交
失败或 client 被中断都会返回非零退出码。后台提交在启动成功后总是立即返回 `0`，真实结果只能
从日志文件中查看。

YARN cluster 模式下 spark-submit 提交后即退出；client 从提交输出中读取 application id，
再通过 ResourceManager REST API 跟踪最终状态。它从 `YARN_CONF_DIR`/`HADOOP_CONF_DIR` 下的
`yarn-site.xml` 读取 `yarn.resourcemanager.webapp.address` 或 HTTPS 地址，也可用
`PUDDING_YARN_RM_URL` 覆盖。没有 web 地址时会在提交前失败。Kerberos 保护的 ResourceManager
需要通过已认证的网关访问。YARN client 模式仍使用 spark-submit 退出码。

### 本地示例

`examples/read_csv_example.yaml` 是一个完整的本地 batch 作业。先设置 `job.sparkHome`、
`job.sparkPuddingHome`、`job.master: local[*]`、`job.deployMode: client` 和 CSV 路径，然后
前台提交：

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml"
```

console sink 会打印数据行，查询结束后 client 返回。后台运行并跟踪日志：

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  "$PUDDING_HOME/examples/read_csv_example.yaml" \
  "$PUDDING_HOME/log/my-job.log"

tail -f "$PUDDING_HOME/log/my-job.log"
```

调度器应使用前台模式：进程只在作业结束时退出，调度器才能拿到真实结果。后台模式适合运维手工
启动，或配合外部监管的长时间 streaming 作业。

### YARN 示例

设置 `job.master: yarn`、`job.deployMode: cluster`，并确保 jar 对集群可见。提交主机上仍需
保留一份发行包，client 需要读取其中的库布局；`job.sparkPuddingHome` 指向该本地目录：

```yaml
job:
  mode: sync
  strategy: incremental
  appName: orders-incremental-sync
  sparkHome: /opt/spark-3.4.0-bin-hadoop3
  sparkPuddingHome: /opt/spark-pudding-1.0.0
  master: yarn
  deployMode: cluster
  config:
    spark.yarn.queue: root.data
    spark.executor.instances: '4'
source:
  - type: jdbc-source
    id: orders_source
    config:
      url: jdbc:postgresql://db.internal:5432/analytics
      table: public.orders
      user: pudding_ro
      password: ${secret:env/ORDERS_DB_PASSWORD}
      cursorField: updated_at
      incremental:
        semantics: upsert
        keyFields: [id]
        initial: full
        statePath: hdfs:///pudding/state/orders
sink:
  - type: jdbc-sink
    id: orders_target
    dependency: orders_source
    config:
      url: jdbc:postgresql://warehouse.internal:5432/warehouse
      table: public.orders
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
```

在 `conf/env.properties` 中设置集群配置目录：

```properties
HADOOP_CONF_DIR=/etc/hadoop/conf
YARN_CONF_DIR=/etc/hadoop/conf
```

然后提交：

```shell
"$PUDDING_HOME/bin/pudding" submit \
  "$PUDDING_HOME/conf/env.properties" \
  /etc/pudding/orders-sync.yaml
```

client 会把作业配置写入仅提交用户可读的临时文件；cluster 模式下通过
`spark.yarn.dist.files` 以 `pudding-job.conf` 分发给 driver，作业结束后删除。作业配置不会
出现在 `spark-submit` 参数或环境变量中。

如果集群节点无法访问本机 jar，可把发行包上传到 HDFS，并用 client jar 的 `--resourceBase`
指定该位置：

```shell
hdfs dfs -put "$PUDDING_HOME" /apps/spark-pudding/1.0.0

"$PUDDING_HOME/bin/pudding-client.jar" \
  -c "$PUDDING_HOME/conf/env.properties" \
  -j /etc/pudding/orders-sync.yaml \
  --target yarn \
  --resourceBase hdfs:///apps/spark-pudding/1.0.0
```

### Kubernetes 示例

`pudding k8s submit` 封装 client，自动为作业配置创建 Secret，只把 Secret 引用传给 driver：

```shell
"$PUDDING_HOME/bin/pudding" k8s submit \
  -c "$PUDDING_HOME/conf/env.k8s.properties" \
  -j "$PUDDING_HOME/k8s/job-example.yaml" \
  --master "k8s://https://kubernetes.example:443" \
  --image registry.example/spark-pudding:1.0.0 \
  --namespace data-platform \
  --service-account spark \
  --resource-base local:///opt/spark-pudding
```

`--master` 是提交主机访问 API server 的地址。如果 driver 必须使用不同地址（例如 kind 的端口
映射），加 `--driver-master`。镜像构建、kind 环境和 Secret 流程见
[../k8s/README.md](../k8s/README.md)。

直接使用 client 提交 Kubernetes 作业时必须提供 `--k8s-job-secret`；推荐使用封装命令，它会
自动创建并删除该 Secret。

### client 覆盖选项

统一入口的 `submit` 只暴露常用路径。需要覆盖执行目标或部署选项时，直接使用发行包内的
client jar：

```shell
"$PUDDING_HOME/bin/pudding-client.jar" \
  -c "$PUDDING_HOME/conf/env.properties" \
  -j "$PUDDING_HOME/examples/read_csv_example.yaml" \
  --target local \
  --sparkHome /opt/spark-3.4.0-bin-hadoop3 \
  --sparkPuddingHome "$PUDDING_HOME" \
  --conf spark.driver.memory=1g \
  --verbose
```

常用 client 选项：

| 选项 | 说明 |
| --- | --- |
| `-c, --confFile` | 环境 properties 文件（必填） |
| `-j, --jobFile` | JSON、YAML 或 HOCON 作业文件（必填） |
| `-t, --target` | `local`、`yarn` 或 `kubernetes`；默认从 master 推导 |
| `--master` | 覆盖 `job.master` |
| `--deployMode` | 在目标支持时覆盖 `job.deployMode` |
| `--resourceBase` | 包含 `libs` 的基础位置，例如 HDFS URI |
| `--sparkHome` | 覆盖 `job.sparkHome` |
| `--sparkPuddingHome` | 覆盖 `job.sparkPuddingHome` |
| `--conf key=value` | 追加 Spark 配置，可重复 |
| `--verbose` | 打印生成的 `spark-submit` 命令 |

`--verbose` 可能打印 `--conf` 的值，涉及凭据时不要用于共享日志。作业配置本身通过文件或
Secret 传递，不会被打印。

## Checkpoint

`pudding checkpoint` 无需提交 Spark 作业即可查看或删除 Structured Streaming checkpoint。
HDFS 需设置 `HADOOP_CONF_DIR`；对象存储通过 `--conf-file` 传入设置。

```shell
# 人类可读摘要：是否存在、结构、大小、年龄、commit/offset 数量
"$PUDDING_HOME/bin/pudding" checkpoint show hdfs:///pudding/checkpoints/orders

# 供监控使用的 JSON
"$PUDDING_HOME/bin/pudding" checkpoint status hdfs:///pudding/checkpoints/orders --json

# 先预览，再删除单个 checkpoint
"$PUDDING_HOME/bin/pudding" checkpoint delete hdfs:///pudding/checkpoints/orders --dry-run
"$PUDDING_HOME/bin/pudding" checkpoint delete hdfs:///pudding/checkpoints/orders

# 删除父目录下超过 7 天的 checkpoint
"$PUDDING_HOME/bin/pudding" checkpoint cleanup hdfs:///pudding/checkpoints \
  --older-than 7d --dry-run
"$PUDDING_HOME/bin/pudding" checkpoint cleanup hdfs:///pudding/checkpoints --older-than 7d

# 只保留最新的两个 checkpoint
"$PUDDING_HOME/bin/pudding" checkpoint cleanup hdfs:///pudding/checkpoints --keep-last 2
```

对象存储示例：

```shell
"$PUDDING_HOME/bin/pudding" checkpoint show s3a://bucket/pudding/checkpoints/orders \
  --conf-file /etc/pudding/checkpoint-s3a.properties
```

`--conf-file` 只接受 `fs.*` 键。使用 IAM role、实例 profile、IRSA 或 `0600` 凭据文件，不要把
access key 放在命令行上。`--hadoop-conf <dir>` 会加载 Hadoop 配置目录中的 XML 文件。

删除或清理前必须停止所有写入者，工具无法检测 writer 是否仍在运行。`cleanup` 只扫描指定 root
的直接子目录，root 必须是 checkpoint 目录的父目录，不能是某个 checkpoint 本身。同时指定
`--older-than` 和 `--keep-last` 时，checkpoint 必须同时满足两个条件。保留策略和
`delete-on-success` 的完成证据见 [streaming-checkpoints.md](streaming-checkpoints.md)。

## 增量状态

`pudding state` 查看或修复增量游标日志。状态目标可以是文件系统路径，也可以是
`@/path/to/state.properties`；properties 文件选择后端（`filesystem`、`jdbc`、`redis` 或
`etcd`）并提供其选项。

```shell
# 查看全部 source，或指定一个 source
"$PUDDING_HOME/bin/pudding" state show hdfs:///pudding/state
"$PUDDING_HOME/bin/pudding" state show hdfs:///pudding/state orders

# 供监控使用的 JSON
"$PUDDING_HOME/bin/pudding" state status hdfs:///pudding/state orders --json

# 列出 source id
"$PUDDING_HOME/bin/pudding" state list hdfs:///pudding/state

# 检查 sink 后处理中断的事务
"$PUDDING_HOME/bin/pudding" state resolve hdfs:///pudding/state orders --abort
"$PUDDING_HOME/bin/pudding" state resolve hdfs:///pudding/state orders --commit

# 显式设置游标，或清除状态以重新 bootstrap
"$PUDDING_HOME/bin/pudding" state reset hdfs:///pudding/state orders --cursor 20260101000000
"$PUDDING_HOME/bin/pudding" state reset hdfs:///pudding/state orders --clear

# 确认没有 writer 存活后，清除失败进程留下的锁
"$PUDDING_HOME/bin/pudding" state unlock hdfs:///pudding/state orders

# 创建或升级 JDBC 状态表
"$PUDDING_HOME/bin/pudding" state migrate @/etc/pudding/state-jdbc.properties
```

properties 目标示例：

```properties
backend=jdbc
url=jdbc:postgresql://state-db:5432/pudding
user=pudding
password=${secret:env/STATE_DB_PASSWORD}
table=pudding_incremental_state
lockTtlMs=600000
```

`${secret:...}` 引用由工具解析。JDBC driver 不在 Spark classpath 时，通过
`PUDDING_STATE_CLASSPATH` 加入 driver jar。Redis 和 etcd provider jar 会自动从 `libs` 加载。
包含明文凭据的 properties 文件必须保持 `0600`。commit/abort 协议、fencing 和恢复规则见
[incremental-sync.md](incremental-sync.md)。

## 能力 manifest

`pudding capabilities` 只读取 `capabilities.yaml` 和 jar 元数据，不加载 Spark 或连接器类。

```shell
# 校验 manifest 与发行包内的 jar 是否一致
"$PUDDING_HOME/bin/pudding" capabilities verify "$PUDDING_HOME"

# 输出合并后的 manifest
"$PUDDING_HOME/bin/pudding" capabilities show "$PUDDING_HOME"

# 升级前比较两个版本
"$PUDDING_HOME/bin/pudding" capabilities diff \
  /opt/spark-pudding-previous /opt/spark-pudding-1.0.0
```

声明的组件或 jar 缺失、manifest 格式错误时，`verify` 返回非零。`diff` 报告新增、删除和变化
的组件。schema 和 fragment 规则见 [capability-manifest.md](capability-manifest.md)。

## 表维护

`pudding maintenance` 通过原生 Spark SQL 对 Iceberg、Paimon 或 Hudi 执行一次维护动作。执行
频率由外部调度器控制，命令不会修改 checkpoint 或增量状态。

```shell
# 只读健康检查
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg status \
  --catalog lake --table db.orders \
  --conf-file /etc/pudding/iceberg-catalog.properties --json

# 合并小文件
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg rewrite-data-files \
  --catalog lake --table db.orders \
  --target-file-size 128MB --min-input-files 5 \
  --conf-file /etc/pudding/iceberg-catalog.properties

# 破坏性操作需要 --confirm；可先用 --dry-run 预览
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance iceberg expire-snapshots \
  --catalog lake --table db.orders --older-than 7d --retain-last 5 --confirm \
  --conf-file /etc/pudding/iceberg-catalog.properties

# Hudi path 模式
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance hudi compact \
  --path hdfs:///warehouse/orders --option op=scheduleAndExecute

# 注册在 catalog 中的 Hudi 表
SPARK_MASTER=local[*] "$PUDDING_HOME/bin/pudding" maintenance hudi clean \
  --table default.hudi_orders --retain-last 10 --confirm
```

支持的动作：

| 格式 | 动作 |
| --- | --- |
| Iceberg | `status`、`rewrite-data-files`、`rewrite-manifests`、`expire-snapshots`、`remove-orphan-files` |
| Paimon | `status`、`compact`、`compact-manifests`、`expire-snapshots`、`remove-orphan-files` |
| Hudi | `status`、`clean`、`compact`、`cluster`、`archive` |

`SPARK_MASTER` 会传给 `spark-submit`；Spark 已有默认 master 时可省略。`--conf-file` 只能包含
`spark.*` 键，建议保持 `0600` 权限。动作专属选项见
`pudding maintenance <format> <action> --help`。安全规则和各格式行为见
[table-maintenance.md](table-maintenance.md)。

## 表维护 HTTP 服务

`pudding maintenance-server` 把同一套维护能力通过 HTTP 暴露给平台或调度器。

```shell
SPARK_MASTER=yarn "$PUDDING_HOME/bin/pudding" maintenance-server \
  --host 127.0.0.1 --port 8080 \
  --state-file /var/lib/pudding/maintenance-tasks.json \
  --audit-file /var/lib/pudding/maintenance-audit.jsonl \
  --auth-token-file /etc/pudding/maintenance-token \
  --conf-file /etc/pudding/iceberg-catalog.properties
```

Bearer token 从 `--auth-token-file` 或 `PUDDING_MAINTENANCE_TOKEN` 读取，不接受命令行值。
`--allow-anonymous` 只用于本地开发。状态文件保存请求和结果，但不保存 Spark 配置或凭据。端点
列表和请求 schema 见 [table-maintenance.md](table-maintenance.md#http-control-plane)。

## Kubernetes

三个 Kubernetes 命令的完整说明见 [../k8s/README.md](../k8s/README.md)。

```shell
# 1. 创建 kind 集群、namespace、service account 和 driver RBAC
"$PUDDING_HOME/bin/pudding" k8s kind-setup \
  --cluster pudding --namespace pudding-k8s-test --service-account spark

# 2. 构建镜像并加载到 kind
"$PUDDING_HOME/bin/pudding" k8s build-image \
  --spark-home "$SPARK_HOME" \
  --image spark-pudding:1.0.0 \
  --kind-cluster pudding

# 3. 提交作业
"$PUDDING_HOME/bin/pudding" k8s submit \
  -c "$PUDDING_HOME/conf/env.k8s.properties" \
  -j "$PUDDING_HOME/k8s/job-example.yaml" \
  --master "k8s://https://127.0.0.1:6443" \
  --image spark-pudding:1.0.0 \
  --namespace pudding-k8s-test \
  --service-account spark \
  --resource-base local:///opt/spark-pudding
```

`k8s build-image` 只把 Spark、`libs` 和演示 CSV 复制到临时构建上下文，不会包含宿主机的 Spark
`conf` 目录和其他配置目录。`env.k8s.properties` 包含凭据时保持 `0600`。

## 校验发行包

在源码树中，`pudding verify` 校验已组装的发行包：

```shell
"$PUDDING_HOME/bin/pudding" verify build/target/spark-pudding-1.0.0
```

它检查预期目录结构、jar、示例文件和能力 manifest。该命令只在源码检出中可用，不随发行包
分发。

## 安全说明

- 不要把密码或 access key 作为命令行参数。使用 `--conf-file`、`env.properties`、环境变量或
  `${secret:...}` 引用。
- 含凭据的作业文件和状态 properties 必须保持 `0600`。client 以仅属主可读的临时文件暂存作业
  配置，并在作业结束后删除。
- `pudding submit` 和 `spark-submit` 不会记录作业内容，只传文件或 Secret 引用。`--verbose`
  会打印生成的命令，其中可能包含 `--conf` 的值。
- checkpoint 和增量状态只保存游标值和 offset，不保存凭据或 source 配置。
- 维护服务既没有 token 也没有 `--allow-anonymous` 时拒绝启动；只应在可信网络中暴露。

## 常见问题

| 现象 | 检查项 |
| --- | --- |
| `environment file is not readable` | `pudding submit` 要求环境文件存在；本地作业可以传空文件 |
| `cannot infer execution target` | 把 `job.master` 设为受支持的值，或对 client jar 使用 `--target` |
| `yarn target requires master 'yarn'` | `--master` 不能把 YARN 目标改成其他 master；删除该参数或修正目标 |
| `spark-pudding libs directory does not exist` | 把 `job.sparkPuddingHome` 或 `--sparkPuddingHome` 指向解压后的发行包 |
| 后台提交启动后立即失败 | 查看指定的日志文件；后台提交无法返回作业结果 |
| `pudding checkpoint` 找不到 Hadoop 类 | 设置 `SPARK_HOME`，或把 `hadoop` 加入 `PATH` |
| JDBC state 命令加载不到 driver | 把 driver jar 加入 `PUDDING_STATE_CLASSPATH` |
| Kubernetes 提交读不到作业配置 | 使用 `pudding k8s submit`，或创建 `--k8s-job-secret` 指定的 Secret |
