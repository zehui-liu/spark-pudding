# spark-pudding Release Notes

[English](RELEASE_NOTES.en.md) | 简体中文

## v1.0.0 — 2026-11-12

> 状态：已于 2026-11-17 发布为 `v1.0.0`，tag 指向通过最终 `mvn -B clean verify` 的提交。
> 分支：`v-1.0`

发布产物：

- `spark-pudding-1.0.0.tar.gz`
- `spark-pudding-1.0.0.tar.gz.sha512`

源文件头和 `NOTICE` 已与 Apache License 2.0 对齐，发行包组装后会校验能力清单和全部模块 jar。

v1.0.0 是从“单一批处理框架”扩展为“批处理 / 增量同步 / 流式集成”数据集成框架的版本。

这个版本的重点不是简单地增加连接器数量，而是把以下边界固定下来：

- **统一组件模型**：source、transform、sink 都通过能力声明参与校验；
- **统一资源模型**：connection 管理连接，catalog 管理表元数据，secret 只保存引用；
- **统一增量语义**：`append` / `upsert` / `cdc` 由 source 选择，sink 自动应用；
- **统一作业模式**：`batch` / `sync` / `streaming`；
- **统一提交边界**：所有模式只允许一个 sink；
- **统一运行时隔离**：connector adapter 与第三方 runtime 分离；
- **统一运维入口**：`bin/pudding` 提供 checkpoint、state、capabilities、maintenance 和 k8s 命令；
- **统一能力发现**：发行包生成并校验 `capabilities.yaml`。

## 目录

- [版本能力总览](#版本能力总览)
- [历史版本](#历史版本)
- [每日开发记录](#每日开发记录)
- [兼容性和升级注意](#兼容性和升级注意)
- [当前已知限制](#当前已知限制)
- [质量验证](#质量验证)
- [后续建议](#后续建议)

---

## 版本能力总览

### 作业模式

| 模式 | 用途 | Source | Sink | Transforms | 进度 |
| --- | --- | --- | --- | --- | --- |
| `batch` | 有界批处理 | 可以多个 | 一个 | 自由组合 | 无持久游标 |
| `sync` + `full` | 全量同步 | 一个 | 一个，必须支持 full refresh | 允许 | 无游标 |
| `sync` + `incremental` | 有界增量同步 | 一个，必须支持增量 | 一个，必须支持对应语义 | 仅保留增量元数据的算子 | 增量状态后端 |
| `streaming` | 持续流处理 | 一个 | 一个 | 按流式能力校验 | Spark checkpoint |

所有模式都只允许一个 sink，目的是保证一个作业只有一个提交边界。

框架本身不提供调度器，调度交给 Airflow、cron、Kubernetes CronJob 等外部系统。

### Source 能力矩阵

| Source | Batch | Streaming | Sync incremental |
| --- | --- | --- | --- |
| `file-common-source` | 是 | 否 | 否 |
| `hive-source` | 是 | 否 | 否 |
| `jdbc-source` / `mysql-source` | 是 | 否 | `append` / `upsert`（`cursorField`） |
| `mongodb-source` | 是 | 否 | 否 |
| `elasticsearch-source` | 是 | 否 | 否 |
| `kafka-source` | 是 | 是 | 否 |
| `hudi-source` | 是 | 是 | `upsert` / `cdc` |
| `iceberg-source` | 是 | 是 | `append` / `upsert` / `cdc` |
| `paimon-source` | 是 | 是 | `append` / `cdc` |
| `mysql-cdc-source` | 否 | 是 | 不适用（unbounded） |
| `postgres-cdc-source` | 否 | 是 | 不适用（unbounded） |
| `mongodb-cdc-source` | 否 | 是 | 不适用（unbounded） |

### Sink 能力矩阵

| Sink | Batch | Streaming | Sync incremental | Full refresh |
| --- | --- | --- | --- | --- |
| `console-sink` | 是 | 是（展示） | 否 | 否 |
| `file-sink` | 是 | 是 | `append` | 非原子，显式 opt-in |
| `hive-sink` | 是 | 否 | 否 | 非原子，显式 opt-in |
| `kafka-sink` | 是 | 是 | 否 | 否 |
| `elasticsearch-sink` | 是 | 是 | 否 | 否 |
| `mongodb-sink` | 是 | 是 | 否 | 否 |
| `jdbc-sink` / `mysql-sink` | 是 | 是 | `append` / `upsert` / `cdc` | 原子替换 |
| `hudi-sink` | 是 | 是 | `append` / `upsert` / `cdc` | 原子替换 |
| `iceberg-sink` | 是 | 是 | `append` / `upsert` / `cdc` | 原子替换 |
| `paimon-sink` | 是 | 是 | `append` / `upsert` / `cdc` | 非原子 opt-in |

### Catalog 和 connection

| 类型 | 支持 |
| --- | --- |
| Connection | Kafka、JDBC/MySQL、MongoDB、Elasticsearch、Hive Metastore、Gravitino |
| Catalog | Hive、Iceberg、JDBC、Hudi、Paimon、Gravitino |
| Secret | `${secret:env/NAME}`、`${secret:file:/path#key}`、`${secret:file:/path}` |

Gravitino 只做元数据目录，不引入 SDK、不做版本绑定，也不增加 core 依赖。

### 增量状态后端

| Backend | 锁 | Lease / fencing | 适用场景 |
| --- | --- | --- | --- |
| Filesystem / HDFS / S3A | best effort | 有限支持 | 单 writer、测试 |
| JDBC | 强一致 | 是 | 生产推荐 |
| Redis | 强一致 | 是 | 生产推荐 |
| etcd | 强一致 | 原生 lease | 生产推荐 |

状态记录只保存游标、pending transaction、计划指纹、尝试次数、失败信息和 fencing token；
不保存凭据或原始连接配置。

### Runtime 隔离

| 组件 | Runtime |
| --- | --- |
| Hudi | `hudi-runtime` |
| Paimon | `paimon-runtime` |
| Iceberg | `iceberg-runtime` |
| Elasticsearch | `elasticsearch-runtime` |
| MongoDB | `mongodb-runtime` |
| Kafka | `kafka-runtime` |
| Kafka Schema Registry | `kafka-schema-registry-runtime`（可选） |
| Native CDC | `cdc-runtime` |
| S3A/OSS/Azure/GCS/COS | 对应 Hadoop object-store runtime |

client 会根据 source、sink、catalog 和 `valueFormat` 自动选择 runtime；直接 `spark-submit`
需要手动加入对应 jar。

---

## 历史版本

### 2026-09-02

- 新增 `elasticsearch-source`：
  - 支持 Elasticsearch 8；
  - 支持登录认证参数；
  - 支持字段选择；
  - 支持 scroll 读取；
  - 支持 `limit`；
  - 增加 `elasticsearch-source.yaml` 示例和模块 README。
- 将 Hudi、Iceberg、Elasticsearch 配置说明拆分到模块级 README。
- 修复 IDEA 中 Scala Map 初始化的类型问题。

### 2026-03-31

- 新增 `iceberg-source` 初版能力。
- 完成 Iceberg table/path 读取和基础配置。

### 2025-03-06

- 修复 Kafka source 编译问题。

### 2025-03-05

- 升级到 `0.0.2-SNAPSHOT`（历史版本；当前开发版本已到 `1.0.0-SNAPSHOT`）。
- 增加 Pudding client 基础能力。

### 2024-12-31

- 初始化项目。
- 建立作业配置解析、source / transform / sink 扩展点。
- 增加最初的 Maven 模块和 README。

---

## 每日开发记录

### 2026-10-24

主题：连接与元数据目录模型

背景：

早期 source 把 endpoint、用户名、密码直接写在自己的 config 中。每接一个 source 都要重复配置
连接信息，也无法复用一个 Hive Metastore、Kafka 集群或 JDBC 数据库连接。

本日完成：

- 设计统一 connection 模型：
  - `name`
  - `type`
  - `endpoint`
  - `auth`
  - `properties`
  - `options`
- 设计统一 catalog 模型：
  - `name`
  - `type`
  - `connection`（可选）
  - `properties`
- 明确 source / sink 可以通过 `connection: <name>` 和 `catalog: <name>` 引用资源。
- 明确配置解析顺序：
  1. parser 保留引用名称；
  2. `PipelineValidator` 校验引用和组件能力；
  3. driver 解析 secret、connection、catalog；
  4. 组件只接收解析后的连接和资源信息。
- 接入 Gravitino，并确定边界：
  - Gravitino 只作为元数据目录；
  - 通过 REST 读取 catalog 信息；
  - 映射到 Hive、Iceberg、JDBC、Hudi、Paimon 原生 Spark catalog；
  - 不引入 Gravitino SDK；
  - 不接受 Gravitino 版本绑定；
  - 不增加 core 依赖体积。
- 明确 Kafka、MongoDB、Elasticsearch 通过 connection 接入，不强行建模成表 catalog。
- 明确后续 HBase 等系统也采用相同方式扩展，而不是把连接信息塞进 core。

设计结果：

```yaml
connection:
  - name: kafka-prod
    type: kafka
    endpoint: broker-1:9092,broker-2:9092
    auth:
      mechanism: SCRAM-SHA-512
    properties:
      security.protocol: SASL_SSL

catalog:
  - name: lake
    type: iceberg
    properties:
      type: rest
      uri: https://catalog.example
```

后续天完成 provider 实现、secret 解析和 Spark catalog 安装。

### 2026-10-25

主题：基础 transform 与 Hudi/Paimon 能力

本日完成：

- 新增 `basic-transform` 模块，提供通用 DataFrame 算子：
  - `filter-transform`：SQL 条件过滤；
  - `aggregate-transform`：groupBy + SQL 聚合；
  - `union-transform`：`union` / `unionByName`；
  - `column-transform`：选择、重命名、类型转换；
  - `limit-transform`：限制行数；
  - `join-transform`：两路 DataFrame join。
- 每个算子支持可选的 `numPartitions`。
- `union-transform` 支持按位置或按名称合并。
- `join-transform` 支持显式指定左、右输入名称，避免依赖 map 顺序。
- 增加 `basic-transform/README.md`，说明：
  - 每个算子的配置；
  - 依赖数量；
  - join 语义；
  - 后续 streaming 的约束。
- 扩展 Hudi/Paimon 的 catalog、source、sink 能力：
  - catalog 和 path 两种使用方式；
  - Hudi/Paimon 表属性；
  - Paimon primary key 表；
  - 后续 streaming、incremental、CDC 的基础。
- 讨论 capability manifest：
  - 发行包根目录生成 `capabilities.yaml`；
  - 后端固定扫描；
  - 第三方 connector 通过 `META-INF/spark-pudding/component.yaml` 动态加入；
  - 未声明能力默认拒绝。

设计原则：

- transform 是纯逻辑，按 family 放到一个模块；
- source/sink 是依赖和隔离边界，保持一连接器一模块；
- 不把多个无关连接器塞进同一个 runtime。

相关测试：

- 基础算子单元测试；
- console sink 展示测试；
- Iceberg source 基础读取测试。

### 2026-10-26

主题：轻量化 core、runtime 拆分和新 sink

本日完成：

- 将 catalog / connection 的具体实现从 core 拆出：
  - `provider/connection-providers`
  - `provider/catalog-providers`
- core 只保留：
  - SPI；
  - `ConnectionConf` / `CatalogConf`；
  - 配置模型；
  - registry / resolver 接口；
  - Spark job 入口。
- 新增 shared runtime 架构：
  - `hudi-runtime`
  - `paimon-runtime`
  - `iceberg-runtime`
- runtime 负责第三方库、Spark extension、Hudi/Parquet/HFile 等实现；connector 只保留 Pudding adapter。
- 明确 client 的 runtime 选择规则：
  - `hudi-source` / `hudi-sink` / Hudi catalog → `hudi-runtime`；
  - Paimon、Iceberg 同理；
  - 一个作业同类 runtime 只加一次；
  - 直接 `spark-submit` 时必须手动加入 runtime jar。
- 分析 Hudi runtime 中的 HBase/HFile：
  - Hudi metadata table 使用 HFile；
  - 因此 runtime 需要相应的 HBase/HFile 依赖；
  - 这些依赖不能进 core 或 connector adapter。
- 新增 sink：
  - `elasticsearch-sink`
  - `mongodb-sink`
  - `kafka-sink`
- 明确 sink 的 manifest/capability 注册方式。
- 清理未使用代码、注释和 README。

相关设计文档：

- `runtime/README.md`
- `provider/connection-providers`
- `provider/catalog-providers`
- 各 sink README。

### 2026-10-27

主题：统一增量语义、JDBC 公共模块和质量门禁

本日完成：

- 实现 Hudi、Paimon、Iceberg 的增量读取和写入。
- 建立统一增量语义：
  - `append`：每行都是新增；
  - `upsert`：每行是 key 的最新状态；
  - `cdc`：每行带 `I` / `U` / `D` 操作列。
- source 在 `incremental.semantics` 中选择语义，sink 自动应用。
- 增加 source/sink 能力声明：
  - `supportedIncrementalSemantics`；
  - `isIdempotentIncremental`；
  - `supportsLakeIncremental`（后续在 JDBC 增量能力中扩展为
    `supportsIncrementalSync`）。
- 配置校验阶段拒绝不支持的 source/sink 组合，不会静默降级为 append。
- 增加 keyFields、opColumn 和 metadata 保留校验。
- 增加 transform 的增量元数据保留规则：
  - 删除 key/op/order 列的算子不能用于 sync incremental。
- 将 JDBC 公共配置抽到 `connector/jdbc-common`：
  - URL、user、password、driver；
  - 表名；
  - save mode；
  - Spark JDBC options；
  - 连接类型校验。
- 引入 checkstyle Maven 集成。
- 做一轮代码审查、测试补充、未使用代码清理和 README 更新。
- 明确单 sink 设计：一个作业只有一个提交边界。
- 评估增量任务之间的依赖与调度：框架不负责调度和任务依赖。

相关模块：

- `connector/jdbc-common`
- `source/hudi-source`、`source/iceberg-source`、`source/paimon-source`
- `sink/hudi-sink`、`sink/iceberg-sink`、`sink/paimon-sink`

### 2026-10-28

主题：初版作业模式与 Gravitino 目录

本日完成：

- 实现初版作业模式：
  - `full`：只做全量集成，不允许自由 transform；
  - `incremental`：面向湖仓增量；
  - `etl`：允许自由 transform 的复杂数据流。
- 在 parser、validator 和 BuildJob 中接入 mode 约束。
- 明确不兼容旧配置迁移，直接拒绝旧模式。
- 完成 Gravitino catalog provider：
  - REST 调用；
  - bearer token / basic auth；
  - vendor media type；
  - 读取 `metalake` / catalog；
  - 映射到原生 Spark catalog；
  - 校验 vendor type 与声明类型一致。
- 增加 client target alias 和 submission plan 的校验。
- 增加 Iceberg batch 测试和 catalog manager 测试。
- 记录本日模式模型后续会在 2026-11-01 被 `batch` / `sync` / `streaming` 替换。

关键接口：

```scala
CatalogProvider
ConnectionProvider
ResolvedCatalog
ResolvedConnection
```

### 2026-10-29

主题：增量状态可靠性

本日完成：

- append sink 事务适配：
  - 记录 transaction id；
  - 支持 replay 时判断 append 是否幂等；
  - 不满足幂等条件时要求 `incremental.recovery` 人工决策。
- 增加增量状态后端：
  - filesystem / HDFS / S3A；
  - JDBC；
  - Redis；
  - etcd。
- 增加锁能力：
  - lock owner / host / pid；
  - acquiredAt；
  - lease TTL；
  - fencing token；
  - force unlock。
- 增加状态审计：
  - lock acquired / released；
  - transaction begin / commit / abort；
  - replay / failure。
- 增加 HDFS/S3A 状态路径和进程崩溃恢复测试。
- 增加文件系统旧状态文件迁移和读取兼容。
- 增加 `IncrementalStateTool` 测试：
  - show；
  - status；
  - reset；
  - resolve；
  - unlock。
- 增加 `ConfigRedactor`，统一脱敏日志和指纹。
- 评估 Kafka → Hudi/Paimon/Iceberg 流式增量方案。
- 清理未使用代码、更新注释和文档。

状态记录包含：

```text
committedCursor
pending transaction
plan fingerprint
attempts
lastTransactionId
lastCommittedAt
lastFailureAt / lastFailureReason
fencingToken
lock info
```

状态记录不包含：

```text
credentials
connection URL
source configuration
```

### 2026-10-30

主题：Kafka 流式摄入

本日完成：

- 实现 `kafka-source` streaming 模式：
  - raw/string；
  - JSON value schema；
  - DDL 字符串或字段 map；
  - `parseMode: failfast / dropMalformed / permissive`；
  - `includeMetadata`；
  - `keepRawValue`；
  - `badRecordsPath`（batch）；
  - `includeCorruptRecord`（streaming）。
- CDC 解码：
  - 通用 `op-column`；
  - Debezium `before` / `after`；
  - `I/U/D` 规范化；
  - Kafka topic/partition/offset/timestamp 排序元数据。
- 实现 Kafka → Hudi/Paimon/Iceberg：
  - streaming append；
  - streaming upsert；
  - streaming CDC；
  - 每个 sink 独立的 checkpoint。
- 明确 streaming 作业只允许一个 source、一个 sink。
- 增加 Kafka runtime。
- 增加 Kafka JSON/CDC 和容器测试。
- 记录限制：
  - Kafka source 不支持 Schema Registry（后续 11-05 补齐）；
  - Kafka ordering 只在 partition 内保证；
  - 跨分区同 key 需要事件时间或显式 order fields。

### 2026-11-01

主题：batch / sync / streaming 三模式重构

背景：

初版 `full` / `incremental` / `etl` 模式把“用户目标”和“执行引擎”混在了一起，导致：

- 全量、增量、批处理、流处理边界不清；
- transform 适用范围难解释；
- 后续能力扩展需要不断加模式。

本日完成：

- 将模式统一为：
  - `batch`：通用有界批处理；
  - `sync`：单 source / 单 sink 同步，支持 `full` 和 `incremental`；
  - `streaming`：Structured Streaming。
- 统一规则：
  - 所有模式只允许一个 sink；
  - `sync` 和 `streaming` 只允许一个 source；
  - 调度由外部系统负责；
  - 旧模式不再兼容。
- 按模式整理 transform 约束：
  - batch 可以使用通用算子；
  - sync incremental 只能使用保留增量元数据的算子；
  - streaming 需要算子声明 streaming 能力；
  - 有状态 streaming 需要 watermark 和可持久化 checkpoint。
- 删除旧模式代码和无用逻辑。
- 更新设计文档、README 和示例。
- 保留“不迁移旧配置”的明确边界。

### 2026-11-02

主题：checkpoint、对象存储、流式状态和维护控制面

本日完成：

#### Capability manifest

- 生成发行包 `capabilities.yaml`。
- manifest schema 升级到 2.1。
- 支持：
  - `verify`；
  - `show`；
  - `diff`；
  - capability set hash。
- 描述：
  - source / sink / transform；
  - connection provider；
  - catalog provider；
  - execution mode；
  - connection type / catalog type；
  - jar / module。
- 支持第三方 connector 通过 `META-INF/spark-pudding/component.yaml` 扩展。

#### Checkpoint

- 增加 `checkpointRetention`：
  - `keep`；
  - `delete-on-success`。
- 增加 `bin/pudding checkpoint`：
  - `show`；
  - `status --json`；
  - `delete`；
  - `cleanup --older-than --keep-last`。
- 增加 Hadoop-only 文件系统操作，后续扩展对象存储。
- 明确 `delete-on-success` 只适用于有限 trigger。

#### 对象存储状态和 checkpoint

- 支持 S3A checkpoint / state。
- 增加 OSS、Azure ABFS/WASB、GCS、COS runtime pack。
- checkpoint CLI 支持：
  - `--conf-file`；
  - `--hadoop-conf`。
- 明确对象存储 checkpoint 的约束：
  - 独立前缀；
  - 单 writer；
  - 禁止生命周期规则自动过期；
  - 优先 HDFS/NFS/PVC。

#### Streaming 可观测性

- 增加 streaming 指标：
  - `streaming.query.started`；
  - `streaming.query.progress`；
  - `streaming.query.completed`；
  - `streaming.query.failed`；
  - `streaming.query.last.progress.timestamp.ms`；
  - `streaming.source.lag.*`；
  - `streaming.source.healthy`；
  - `streaming.state.*`；
  - watermark/lag。
- 增加 JMX MBean。
- 增加结构化日志。

#### 有状态 streaming

- 增加：
  - 事件时间窗口聚合；
  - watermark 去重；
  - inner stream-stream join；
  - state TTL 由 watermark 约束。

#### Maintenance API 和 CLI

- 增加 `maintenance-api`：
  - `TableMaintenanceService`；
  - `MaintenanceRequest` / `MaintenanceResult`；
  - action 定义和参数校验。
- 增加 `maintenance-spark`：
  - Iceberg status / rewrite / expire / orphan；
  - Paimon status / compact / expire / orphan；
  - Hudi status / clean / compact / cluster / archive。
- 增加 `maintenance-cli`：
  - `bin/pudding maintenance ...`；
  - `--dry-run`；
  - `--confirm`；
  - `--json`；
  - exit code 0/1/2。
- 增加中文 README。
- 增加 Testcontainers 和 MinIO/S3A 回归。

### 2026-11-03

主题：配置契约、维护 HTTP、流式状态和公开 CLI

本日完成：

#### Spark 配置统一解析

- 增加 `SparkConfigResolver`：
  - 统一 `job.config`、connection、catalog、runtime 配置；
  - 明确优先级；
  - 同名 key 冲突在启动前失败；
  - 不再让 client 和 driver 两次应用配置产生覆盖。
- 记录解析链：
  - job config；
  - client target；
  - catalog properties；
  - connection properties；
  - runtime 默认配置。

#### 对象存储 runtime

- 增加：
  - `hadoop-oss-runtime`；
  - `hadoop-azure-runtime`；
  - `gcs-runtime`；
  - `hadoop-cos-runtime`。
- Iceberg 数据层接入 `S3FileIO`，与 S3A checkpoint 区分。
- 增加 MinIO/S3FileIO 测试。

#### Maintenance HTTP 控制面

- 新增 `maintenance-server`：
  - `GET /healthz`；
  - `GET /v1/capabilities`；
  - `POST /v1/maintenance`；
  - `GET /v1/maintenance`；
  - `GET /v1/maintenance/{id}`；
  - `POST /v1/maintenance/{id}/cancel`。
- 支持：
  - bearer token；
  - `Idempotency-Key`；
  - 任务持久化；
  - 审计 JSONL；
  - 单 worker 限流。
- 明确运行中的 Spark action 不强制中断，避免表提交中途失败。

#### JDBC / MySQL streaming upsert

- 增加 JDBC streaming `outputMode=update`：
  - `recordKeyFields`；
  - 目标表主键/唯一索引校验；
  - staging table；
  - PostgreSQL/H2 `MERGE`；
  - MySQL/MariaDB keyed update + insert；
  - 单事务提交；
  - null/duplicate key 校验。
- 保持 JDBC streaming 不接受 `cdcOpColumn`，删除语义只在后续 sync incremental CDC 中支持。

#### 流式状态和指标

- 增加 `StreamingStateContract`。
- 增加 streaming `update` / `upsert` 输出。
- 增加 Kafka lag/health 指标。
- 增加 state metrics。

#### CLI 和配置

- 将脚本入口统一到 `bin/pudding`。
- 删除旧 shim 和重复脚本。
- 删除 `job.enableHiveSupport`。
- Hive 支持改为通过 `job.config` 传递 Spark 配置。
- 增加 breaking change 说明。
- 做一轮代码审计、未使用代码清理和文档更新。

### 2026-11-04

主题：Lakehouse 语义、原生 CDC 和 JDK 11/17

本日完成：

#### Iceberg

- `iceberg-sink` 增加 Upsert / CDC：
  - `recordKeyFields`；
  - `cdcOpColumn`；
  - `foreachBatch` + `MERGE INTO`；
  - 按 key 保留最后一条变更；
  - append transaction 保留 snapshot marker/fencing token；
  - merge 路径不写 fencing marker（明确限制）。
- `iceberg-source` 增加 Streaming 和 Upsert。
- 明确 Iceberg 1.5 没有行级 changelog streaming，删除语义需要 sync incremental CDC。

#### Paimon

- 增加 Paimon catalog streaming sink。
- Paimon 1.3.2 + Spark 3.4 没有原生 catalog `toTable` streaming writer：
  - append 使用解析后的 location；
  - update/CDC 使用 direct primary-key writer；
  - `_row_kind_` 传递操作。
- 明确 Paimon full refresh 非原子，需要 `allowNonAtomicReplace`。
- 明确 direct writer 不应用 sink `tableOptions`。

#### Hudi

- 增加 Hudi catalog 增量读。
- 增加 Hudi catalog streaming 写。
- 增加 Hudi 原子 full refresh：
  - `insert_overwrite_table` replace commit；
  - COW/MOR；
  - 失败回滚测试。

#### Lakehouse 回归

- 增加 Hudi/Paimon/Iceberg 本地 Spark 测试。
- 增加 Hudi/Paimon MinIO/S3A 对象存储测试：
  - table data；
  - timeline/snapshot；
  - incremental read；
  - full refresh；
  - checkpoint。
- 增加 Iceberg S3FileIO 测试。

#### 原生 CDC

- 新增 `database-cdc-source`：
  - `mysql-cdc-source`；
  - `postgres-cdc-source`；
  - `mongodb-cdc-source`。
- 基于 Debezium 1.9.8 / MongoDB change stream：
  - 初始 snapshot；
  - binlog / WAL / resume token；
  - `I/U/D` 输出；
  - `_cdc_timestamp`；
  - `_cdc_sequence`；
  - `_cdc_table`。
- 明确：
  - 仅 streaming；
  - 每 source 一张表/一个 collection；
  - schema 显式；
  - DDL 不作为行输出；
  - MySQL snapshot 中断会重跑；
  - PostgreSQL slot 不会自动删除。
- 增加 CDC → Hudi/Paimon/Iceberg 端到端容器测试。
- 增加 MySQL/PostgreSQL/MongoDB CDC 容器测试。

#### JDK

- 删除 JDK 8 支持。
- 支持 JDK 11 和 JDK 17：
  - Java release 11；
  - Maven Enforcer 拒绝 JDK 8 和 JDK 18+；
  - JDK 17 profile；
  - 子 JVM 增加 `--add-opens`；
  - CI 增加 JDK 17 编译和定向测试。
- 评估 Spark 版本升级，保留 Spark 3.4.0 基线。

#### 工程整理

- 清理未使用 import、注释和 README。
- 增加 compatibility matrix 和对象存储测试说明。

### 2026-11-05

主题：代码审计、checkpoint 安全、Schema Registry 和 JDBC 增量/CDC

本日完成：

#### 代码审计和可靠性修复

- 清理编译器可见的未使用 import、局部定义、不可达分支和无用成员。
- CDC：
  - `Long` 时间戳按 epoch milliseconds；
  - 数值转换使用精确 `BigInt` 和范围检查；
  - heartbeat/transaction metadata 跳过；
  - null JSON 使用 `NullNode`；
  - Debezium sequence 只在追加记录时递增；
  - checkpoint offset 只读最近已提交 batch；
  - schema history 路径写入 try/finally；
  - MongoDB `fullDocument` 为 null 的 update 跳过。
- Core：
  - 组件异常脱敏；
  - trigger 传递到 source context；
  - incremental lock 释放失败告警；
  - `IncrementalState.isEmpty` 考虑 attempts/failure；
  - JMX 注册失败不锁存；
  - 对象存储状态路径识别 `cosn`/`cos`；
  - state tool 锁信息读取失败标记 `<unreadable>`；
  - JobConfigureParser 裁剪空白；
  - checkpoint 清理拒绝文件系统根目录和非 checkpoint 目录。
- JDBC：
  - `tableExists` 只把明确“表不存在”状态码当作不存在；
  - 连接/权限错误继续抛出；
  - 增加 `JdbcTablesTest`。
- Maintenance：
  - SQL literal 先转义反斜杠再转义引号；
  - Paimon SQL `-Xlint` 修复；
  - Spark session 初始化失败时停止 session；
  - 拒绝不支持的 retention 参数；
  - duration/size 溢出转为 usage error。
- Client/Kubernetes：
  - driver pod 删除失败报告 FAILED；
  - 删除不存在的 pod 幂等成功；
  - SubmissionManager.close 等待 secret cleanup；
  - k8s-submit Secret namespace 修复；
  - shell `compgen` 空结果处理。
- Lakehouse：
  - Iceberg snapshot sequence 排序；
  - upsert 丢弃内部列；
  - 无排序字段重复 key 拒绝；
  - merge null key 拒绝；
  - Paimon `supportsAtomicReplace=false`；
  - Paimon direct writer 列顺序对齐；
  - Hudi/Paimon 空 op 校验；
  - Hudi/Paimon 重复 key 校验。

#### Checkpoint 外部停止语义

原问题：

```text
正常结束：exception=None, isActive=false
外部 query.stop()：exception=None, isActive=false
```

原实现无法区分，可能删除 checkpoint 导致重放。

修复：

- `available-now`：
  - 要求最终零行 progress；
  - 该 progress 的 batch id 大于 checkpoint offset log 最新已提交 batch id；
  - 零行 cleanup batch 因会提交 offset，不会被误判。
- `once`：
  - 要求单 batch 已提交并产生 progress。
- 外部停止在完成证据出现前：
  - 保留 checkpoint。
- 失败、超时、非有限 trigger：
  - 保留 checkpoint。
- 增加 `StreamingCheckpointCompletionTest`。

#### Kafka Schema Registry / Avro / Protobuf

新增：

- `runtime/kafka-schema-registry-runtime`；
- `ConfluentValueDecoder`；
- `FixedSubjectNameStrategy`；
- `kafka-source` 的 `valueFormat: avro|protobuf`。

支持：

- Confluent wire format；
- Avro/Protobuf；
- subject 默认从 topic 推导；
- 固定 subject；
- Basic Auth；
- TLS / schema registry properties；
- Avro nested、array、map、union、decimal、date、timestamp；
- Protobuf scalar、enum、bytes、nested、repeated、map、Timestamp；
- `cdcFormat: op-column`；
- `includeMetadata` / `keepRawValue`。

关键配置：

```yaml
source:
  - type: kafka-source
    id: orders
    config:
      subscribe: orders
      valueFormat: avro
      valueSchema:
        id: long
        name: string
        amount: decimal(12,2)
      schemaRegistry:
        url: https://schema-registry.internal
        username: ${secret:env/SCHEMA_REGISTRY_USER}
        password: ${secret:env/SCHEMA_REGISTRY_PASSWORD}
        subject: orders-value
        properties:
          basic.auth.credentials.source: USER_INFO
```

实现细节：

- connector 通过反射加载可选 runtime；
- runtime 使用 Confluent 7.4.0；
- Confluent/Avro/Protobuf/Jackson/Guava 等依赖 shade 到
  `org.pudding.kafka.schema.shaded.*`；
- client 只在 `valueFormat=avro|protobuf` 时加入 schema registry runtime；
- JSON-only Kafka 作业不加载这 18 MB runtime。

限制：

- `valueSchema` 必须显式配置；
- 不从 registry schema 自动推导 Spark schema；
- `badRecordsPath` / `includeCorruptRecord` 仍只对 JSON；
- Debezium `before`/`after` 仍只对 JSON；
- Protobuf well-known types 目前重点支持 `Timestamp`。

#### JDBC 增量 source

`jdbc-source` / `mysql-source` 支持：

```yaml
source:
  - type: jdbc-source
    id: orders
    config:
      url: jdbc:postgresql://db:5432/analytics
      table: public.orders
      user: pudding_ro
      password: ${secret:env/ORDERS_DB_PASSWORD}
      cursorField: updated_at
      incremental:
        semantics: upsert
        keyFields: [id]
        initial: full
        statePath: hdfs://namenode:8020/pudding/state/orders
```

能力：

- `append` / `upsert`；
- 单游标列；
- numeric / date / time / timestamp / string；
- `initial: full|latest`；
- `incremental.start` / `incremental.end`；
- `cursorField` 放顶层，进入计划指纹；
- 表和游标列按 JDBC 元数据引用；
- 游标值按 JDBC 类型转义；
- `upsert` 在范围内按 key 保留最新行；
- 表快照无法感知 delete；
- 不支持复合游标；
- 不支持可恢复 snapshot。

#### JDBC CDC sink

`jdbc-sink` / `mysql-sink` 支持 sync incremental：

```yaml
sink:
  - type: jdbc-sink
    id: orders_target
    dependency: orders_cdc
    config:
      url: jdbc:postgresql://warehouse:5432/warehouse
      table: public.orders
      user: pudding_rw
      password: ${secret:env/WAREHOUSE_DB_PASSWORD}
      cdcOpColumn: __op
      cdcOrderFields: [_cdc_timestamp, _cdc_sequence]
```

行为：

- 按 key 保留最后一次操作；
- `D` → delete；
- `I/U` → upsert；
- delete + upsert 使用一个数据库事务；
- staging table 写入 upsert 和 delete keys；
- PostgreSQL/H2 使用 `MERGE` + `DELETE ... WHERE EXISTS`；
- MySQL/MariaDB 使用 keyed `UPDATE JOIN` + `INSERT ... WHERE NOT EXISTS` +
  `DELETE ... JOIN`；
- 目标表必须有匹配的主键/唯一索引；
- upsert/cdc 重放幂等；
- append 不保证重放幂等；
- streaming JDBC 的 `cdcOpColumn` 仍然拒绝。

#### 版本号

- Maven 项目版本从 `0.0.2-SNAPSHOT` 更新为 `1.0.0-SNAPSHOT`。
- Git 分支从 `v-0.2` 重命名为 `v-1.0`。
- 所有 POM 的 parent / dependency 版本、README 示例、jar 名、K8s 镜像 tag 和
  capability manifest 示例同步更新。
- `@deprecated` 版本标记更新为 `1.0.0`。

#### 文档和示例

- 更新 README、中文 README、设计文档、矩阵和模块 README。
- 新增：
  - `build/examples/incremental_jdbc_to_jdbc.yaml`
  - `build/examples/read_kafka_avro_example.yaml`
- 新增中英文 CLI 指南 `build/doc/cli.md` 和 `build/doc/cli.zh-CN.md`，覆盖 `bin/pudding`
  所有子命令的参数和可运行示例。
- README 和中英文 README 增加 `pudding submit` 章节，说明前台/后台提交、目标推导、退出码和
  client jar 覆盖选项。
- 更新 runtime、client、compatibility、JDBC、Schema Registry 文档。
- 新增 `RELEASE_NOTES.md`。

### 2026-11-06

主题：本地全链路 smoke 与 CDC 修复

- **本地 client 失败传播**：
  - `LocalTarget` 改用 `startProcess(launcher)`，读取 `spark-submit` 子进程
    退出码；
  - `ProcessRunningApplication` 对非 Kubernetes target 在 exit code 0 时返回
    `FINISHED`，不再误报 `LOST`；
  - 修复前 driver 失败时 `pudding submit` 仍返回 0，修复后返回非零。
- **Elasticsearch Scala 2.12 兼容**：
  - `elasticsearch-runtime` 从 `elasticsearch-hadoop:8.9.1` 切换到
    `elasticsearch-spark-30_2.12:8.9.1`；
  - 原构件按 Spark 2.3 / Scala 2.11 编译，Spark 3.4 下类加载失败；
  - ES source/sink 的本地 smoke 已恢复通过。
- **MongoDB CDC 两处修复**：
  - 初始快照后的首条 change event 复用了最后一个快照的 sequence，被
    `CdcRecordBuffer` 以 “non-increasing CDC sequence” 拒绝；现在先分配下一个
    sequence 再编码；
  - 首次启动的 change stream 不再使用 `hello` 返回的 `startAtOperationTime`
    （可能落后于最后一次 oplog 写入），改为从 cursor 创建点开始，避免重放快照
    之前的 DDL（例如聚合 `$out` 产生的 rename）导致任务失败；中断快照恢复仍按
    已记录的 `startAt` 重放。
- **JDBC 增量 upsert 保留业务排序列**：
  - `IncrementalFrames.latestByKey` 不再删除作为排序字段的真实业务列
    （例如 JDBC cursor `updated_at`），只删除以 `_` 开头的内部元数据列；
  - 修复 sync incremental 写入 JDBC 目标时因 `updated_at` 被提前丢弃而报
    “missing target column” 的问题。
- **CDC 连接瞬断重试与重连**：
  - `DebeziumCdcPoller` 对可重试的连接类异常（`RetriableException`、
    SQLState `08xxx`、`ConnectException`/`SocketException`/`EOFException`、
    MySQL `CommunicationsException`、超时）记录告警；
  - Debezium 在连接失败后会停止内部 task，因此 poller 关闭并重建
    connector/task，从最后一条已发射记录的 source offset 继续，按 2 秒间隔
    重试；
  - 数据库重启或短时网络分区不再直接终止 streaming query；
  - 配置、认证和 schema 错误仍然立即失败；
  - Debezium 的 binlog client 也可能自行重连；RG-05 中 MySQL 重启后客户端
    约 2 分钟自行恢复，poller 级重建作为未自愈错误的兜底。
- **JDBC/MySQL 流式 CDC 写入**：
  - `jdbc-sink` / `mysql-sink` 的 streaming 写入支持 `cdcOpColumn`；
  - 每个 micro-batch 在一个数据库事务中应用 delete 与 upsert，先写私有
    staging 表再合并；
  - 需要 `recordKeyFields`、目标表上匹配的主键/唯一索引、
    `outputMode: append` 和 `checkpointLocation`；
  - 未配置 `cdcOpColumn` 时，`outputMode: update` 仍只做完整行 upsert，
    不删除目标行。
- **JDBC 增量读并行度**：
  - 无 `partitionColumn` 的 JDBC 读只有一个分区，窗口去重后容易被单个 task
    承担；`JdbcIncremental` 现在按业务 key repartition 到
    `spark.sql.shuffle.partitions` 再执行窗口；
  - 10,000,000 行 PostgreSQL → JDBC 规模用例不再因单 task 写入 OOM。
- **Paimon 流式 append 到已存在表**：
  - 不再把建表属性（`primary-key`、`bucket`、`tableOptions`）传给流式
    DataFrameWriter，只传递 `options` 中的运行时写选项；
  - 修复已预建 Paimon 表的 Kafka Avro/Protobuf 流式作业报
    “Change 'primary-key' is not supported yet” 的问题；
  - 流式 append 写入前按表 schema 名称对齐列，避免 `valueSchema` map
    顺序丢失导致主键列位置错位。
- 测试床 `pudding-test` 的 P0 组件 smoke：30/30 有限批处理用例通过，
  E2E-01 组合 transform 链路通过；CDC smoke SR-11（MySQL）、SR-12
  （PostgreSQL，10,000 行初始快照）、SR-13（MongoDB，10,000 行快照 +
  I/U/D）通过。SR-11 之前的 MySQL 1236 错误由超时后残留的 Spark driver
  占用同一 Debezium `server_id` 引起；本地 runner 现在按进程组终止提交进程。
  E2E-02（union/join/dedup）、E2E-03（validate/quarantine/schema contract）、
  E2E-04（JDBC 增量 upsert）、E2E-05（MySQL CDC → Paimon）、E2E-06
  （MySQL CDC → Hudi/Iceberg）、E2E-07（MongoDB CDC → Paimon）、E2E-08
  （PostgreSQL CDC → JDBC）、E2E-09（Kafka → Hudi，checkpoint 重启）、
  E2E-10（Kafka Avro/Protobuf
  + Schema Registry → Paimon）、E2E-11（窗口聚合、dedup）、E2E-12
  （Hudi → Paimon、Iceberg → Hudi 增量同步）、E2E-13（Hive/Kafka/MongoDB/
  Elasticsearch 双向 round trip）、E2E-14（file/Hive/JDBC/MySQL/Hudi/Paimon/
  Iceberg 全量替换）、RG-01（kill -9 恢复）、RG-02（并发锁）、RG-03
  （pending transaction abort/commit）、RG-04（Kafka broker 重启续跑）、
  RG-05（MySQL 重启后 CDC 续跑）、RG-07（state CLI
  status/reset/unlock/migrate/list）通过；负面用例
  NG-01/02/03/04/05/07/08（多 sink、未知依赖、流式模式不支持、非法
  outputMode、缺失 `url`、schema 缺列、缺失 secret）均在启动前失败，
  错误信息只包含 key 名，不泄露凭据。

### 2026-11-07

主题：对象存储 checkpoint/state、Iceberg S3FileIO 与 checkpoint CLI 修复

- **job.config 中的 secret 引用与 spark-submit 转发冲突**：
  - client 会把 `job.config` 原样作为 `--conf` 传给 `spark-submit`，driver 的
    runtime SparkConf 中仍是未解析的 `${secret:...}`，而 `BuildJob` 已解析
    `job.config`，`SparkConfigResolver` 误判为两个来源值不一致并拒绝启动；
  - 现在在 driver 上同步解析 runtime SparkConf 中的 secret 引用后再比较和合并；
  - 支持在 `job.config` 中配置
    `spark.hadoop.fs.s3a.access.key` / `spark.hadoop.fs.s3a.secret.key`
    等带凭据的 Spark/Hadoop 设置，日志仍只显示 `******`。
- **对象存储 checkpoint 年龄计算**：
  - S3A 对目录返回合成的“当前时间”作为 modification time，
    `CheckpointFiles.lastModified` 之前把目录时间也计入，导致
    `checkpoint status` 的 `ageSeconds` 始终接近 0、`cleanup --older-than`
    永不清理、`--keep-last` 可能保留错误对象；
  - 现在只用文件时间计算年龄，空目录仍回退到目录时间以便正常过期；
  - 新增单测覆盖“目录时间为当前时间、文件时间为旧时间”的场景。
- **对象存储全链路验证**（pudding-test）：
  - S3A checkpoint：首次 10 行，重启后仍 10 行，新增文件后 12 行；
  - S3A 增量状态：未设置 `allowBestEffortLock` 时启动前拒绝，开启后
    100 → 102 行且无重复，`state status` 显示已提交游标、无 pending、无锁；
  - Iceberg JDBC catalog + `S3FileIO`：写入并读回 1,000 行，warehouse 位于
    `s3://pudding/warehouse/os-02`；
  - 两个受限 MinIO 用户验证 S3A 与 S3FileIO 凭据双向隔离（交叉读取 403），
    错误凭据写入在 S3FileIO 数据层返回 403；
  - checkpoint CLI 的 JSON 字段、`delete --dry-run`、`cleanup --keep-last`、
    `--older-than`、非法 `--conf-file`/duration 与安全拒绝全部通过；
  - 渲染后的 job 文件和提交日志中只出现 `${secret:file:...}` 引用，不出现明文。
- **规模门禁 E2E-14（Iceberg 1.3 GiB 全量替换）**：
  - 既有 200 行目标表通过 `sync full` 原子替换为 10,000,000 行；
  - Iceberg commit 指标为 `removedDataFiles=1`、`removedRecords=200`、
    `addedDataFiles=11`、`addedRecords=10000000`，读回断言行数一致。
- **规模门禁 E2E-06（10,000,013 行 MySQL CDC 初始快照 → Hudi）**：
  - 首次运行写入 6,200,031 行后，本机 Hudi HTTP marker server 的地址
    因宿主网络地址变化不可达；
  - 从 checkpoint 以 `hoodie.write.markers.type=DIRECT` 和 `upsert`
    续跑，Debezium 重放初始快照时按 `order_id` 幂等更新；
  - 最终 Hudi 表精确为 10,000,013 行，无重复；
  - 同时验证了 CDC 队列容量修复（`queueCapacity` 至少
    `maxRecordsPerBatch + 1`）和 Hudi 空 microbatch 修复。
- **规模门禁 E2E-09（10,000,000 条 Kafka JSON → Hudi）**：
  - 4 分区 `orders-scale` topic，生产端约 301k msg/s，全部消息投递成功；
  - `maxOffsetsPerTrigger=500000` 的流式 bulk insert 消费 10,000,000 条，
    Hudi 目标表 10,000,000 行、484 MB；
  - 使用 `hoodie.write.markers.type=DIRECT`，长任务不依赖本机 HTTP
    marker server。
- **E2E-11 自 join**：
  - 同一 Kafka source 派生 PAID 左右两条流，按 `order_id` 做 inner
    stream-stream join，双侧 1 分钟 watermark；
  - 首跑输出 5 行，checkpoint 重启后不再重复输出；
- **Schema Registry subject 与错误路径**：
  - 自定义 subject 解码 1,000 行 Avro 消息；
  - Confluent wire format 的 schema id 是权威来源；`schemaRegistry.subject`
    在 id-based 消息上不是必需项，缺失 subject 不影响解码；
  - 未知 schema id 999999 失败，错误信息包含 topic 和 id，便于定位。
- **RG-06 checkpoint 保留语义**：
  - `delete-on-success` + `available-now` 自然排空后在成功路径删除
    checkpoint；
  - `timeoutMs` 触发外部停止时保留 checkpoint，并记录
    “did not prove that its input was drained”；
  - 直接 `query.stop()` 场景由 core 单测覆盖。
- **NG-06 streaming CDC 校验**：
  - 非法 op 值返回 `unsupported cdc operation in op: X`；
  - 缺失 op 列返回 `cdc input is missing operation column: missing_op`；
  - 未配置 `cdcOrderFields` 且无 Kafka/Debezium 排序元数据时拒绝执行。
- **Hudi streaming append 空 microbatch**：
  - Hudi 在每个 streaming commit 更新元数据表；0 行 microbatch 会触发
    `Positive number of partitions required` 并使查询失败；
  - CDC 快照启动、Kafka backlog 排空时出现空批次是正常现象；
  - 现在 append 路径使用 `foreachBatch`，空批次只推进 offset，不写 Hudi；
  - 由 10,000,000 行 MySQL CDC 初始快照用例覆盖。
- **CDC 队列容量自锁死**：
  - 默认 `queueCapacity` 在 `maxRecordsPerBatch >= 10000` 时等于
    `maxRecordsPerBatch`，队列可以被一批恰好填满；
  - Spark 只在“构造出含新数据的下一批”时提交上一批的 source offset；
    队列满后 poller 阻塞、没有新数据、上一批永不提交，形成死锁；
  - 实际容量现在至少为 `maxRecordsPerBatch + 1`，保留一条记录的余量；
  - 由 10,000,000 行 MySQL CDC 初始快照用例发现并覆盖，
    `CdcSourceOptionsTest` 增加容量回归测试。
- **OS-03 ～ OS-08 运维用例**（pudding-test）：
  - HDFS checkpoint/state 在 NameNode/DataNode 重启后恢复；
  - JDBC/Redis/etcd 状态后端的 migrate、status、reset、unlock、 fencing；
  - Iceberg/Paimon/Hudi 维护命令的 status/compact/expire/orphan/archive、
    `--confirm`/`--dry-run` 与安全拒绝；
  - maintenance-server 的 token、幂等、取消、审计与确认门禁；
  - capabilities verify/show/diff 与缺失 jar/manifest 检测。

### 2026-11-08

主题：代码审计、文档与配置整理

- 使用 `-Ywarn-unused` 对全量模块做编译审计：
  - 移除 MongoDB change stream 中未使用的 `database` 参数；
  - 清理 CDC 编码、增量状态读取中未使用的 pattern 变量；
  - 仅保留 SPI 默认方法中必须保留的签名参数（如
    `DataSink.isIdempotentIncremental` 的 `semantics`）；
- 确认所有 main 组件类都由 `META-INF/services` 或 CLI 脚本引用，没有未注册
  的 source/sink/transform/provider；
- 更新 `CdcRecordBuffer` 的容量说明、Hudi 空 microbatch、checkpoint 年龄、
  Kafka Schema Registry subject 等注释和文档；
- `conf/env.properties` 增加 `JAVA_HOME`/`SPARK_HOME` 示例，并注明 JDK 11/17。

### 2026-11-11

主题：代码审查 P0/P1 修复

- 修复 P0 数据正确性问题：Hudi CDC 改为按 Avro schema 解码 DATE、TIMESTAMP、DECIMAL、
  BINARY，不再使用 `from_json`；sync incremental 禁止使用 dedup；Iceberg 变更排序包含
  insert/delete 优先级；Iceberg merge 路径执行并持久化 fencing；Kafka CDC 排序优先使用
  partition 和 offset；MongoDB Binary、ObjectId、Decimal128 键统一编码；Hudi append 重放
  会检查 archived timeline。
- 修复 P1 失败路径与配置问题：JDBC 表探测回退到大小写不敏感的 metadata；YARN client
  使用 spark-submit 退出码；JDBC CDC 校验 upsert 键；脱敏替换串使用安全引用，指纹序列化
  失败时 fail-closed；file sink 尊重 `options.checkpointLocation`；batch 湖仓 CDC 支持
  sink 的 `cdcOrderFields`。
- 每项修复均补充回归测试。Hudi CDC 解码器改为可序列化的 Catalyst UDF，避免全量测试
  reactor 中的 Spark closure cleaner 问题。
- 后续审计修复：Hudi CDC 解码器区分 Avro CDC-log 镜像与 InternalRow JSON，解码 Base64
  BINARY 和嵌套类型，并对有损 DECIMAL fail-closed；DECIMAL 文本解析改为按字节精确解析；
  Iceberg MERGE fencing 同时持久化并校验 transaction id；归档 Hudi commit metadata 不可读时
  fail-closed。
- 处理剩余审计 Minor：Hudi 归档重放不再用来源 cursor 限定目标 timeline；file checkpoint
  路径做规范化；JDBC metadata 匹配 catalog/schema；JDBC upsert 支持 `cdcOrderFields`；
  Iceberg snapshot sequence 使用 broadcast UDF 查找；`PipelineValidator` 只校验增量 sink
  可达的 transform。
- 后续审计修复后的 JDK 11 全量 `mvn clean verify` 通过（61 个模块，14 分 08 秒）。

### 2026-11-12

主题：YARN cluster 生命周期与当前代码基准

- YARN cluster 提交从 spark-submit 输出读取 application id，并通过 ResourceManager REST API
  跟踪最终状态。地址来自 `YARN_CONF_DIR`/`HADOOP_CONF_DIR` 下的 `yarn-site.xml`，也可用
  `PUDDING_YARN_RM_URL` 覆盖；缺少 web application address 时在提交前失败。
- 产品决策：Hudi CDC 对 InternalRow JSON 镜像中的 DECIMAL 继续保持 fail-closed。Hudi 在该
  编码中没有保留数值，因此 source 明确报错，而不是写入错误金额；base-file 回读留待后续按需
  实现。
- 当前代码的本地规模基线：E2E-01 23s；Iceberg/Hudi/Paimon 1.3 GiB 全量替换
  25s/244s/27s；E2E-04 10M PostgreSQL 增量 163s；E2E-06 10M MySQL CDC → Hudi 3037s、
  → Iceberg 1021s；E2E-09 10M Kafka → Hudi 195s。以上为单节点本地测量，不是生产 SLA。

---

## 兼容性和升级注意

### 版本号

- Maven 项目版本从 `0.0.2-SNAPSHOT` 更新为 `1.0.0-SNAPSHOT`；
- Git 分支从 `v-0.2` 重命名为 `v-1.0`；
- 发行包、jar 名和示例路径统一使用 `1.0.0-SNAPSHOT`；
- 远端 `origin/v-0.2` 不会被自动修改或删除。

### 作业模式

旧模式：

```text
full / incremental / etl
```

已替换为：

```text
batch / sync / streaming
```

旧配置不会自动迁移，启动时直接拒绝。

### sink 约束

所有模式只允许一个 sink。多目标写入请拆分为多个作业。

框架不提供内部调度器，调度由 Airflow、cron、Kubernetes CronJob 等外部系统负责。

### JDK

- 不再支持 JDK 8。
- 支持 JDK 11 和 JDK 17。
- Maven 在 `validate` 阶段拒绝 JDK 8 和 JDK 18+。

### Hive

- 移除 `job.enableHiveSupport`。
- 使用 `job.config` 传递 Spark 配置：

```yaml
job:
  config:
    spark.sql.catalogImplementation: hive
```

### 公开 CLI

所有入口统一为：

```text
bin/pudding
```

旧的 shim 和重复脚本入口已删除。

### capability manifest

manifest schema 已升级到 2.1。后端应保存每个发行版本的 manifest，并使用 `diff` 比较新增、删除和变化的组件。

### checkpoint

`delete-on-success` 现在需要完成证据：

- `available-now`：最终零行 progress 的 batch id 高于最新提交 offset；
- `once`：单个 batch 已提交并产生 progress。

外部停止、失败、超时都会保留 checkpoint。

### Schema Registry

Avro/Protobuf 需要显式配置 `valueSchema`，不会自动从 registry schema 推导 Spark schema。

直接使用 `spark-submit` 时需要同时加入：

```text
libs/kafka-runtime-<version>.jar
libs/kafka-schema-registry-runtime-<version>.jar
```

### JDBC 增量

`cursorField` 放在 source 配置顶层，不属于 `incremental` 块：

```yaml
config:
  table: public.orders
  cursorField: updated_at
  incremental:
    semantics: upsert
    keyFields: [id]
    initial: full
```

JDBC 表快照无法感知 delete；需要删除传播时使用 CDC source。

### 升级检查清单

- 将依赖、镜像 tag 和发行包路径更新到 `1.0.0-SNAPSHOT`；
- 将旧作业模式改为 `batch` / `sync` / `streaming`；
- 确认每个作业只有一个 sink；
- 升级到 JDK 11 或 JDK 17；
- 将 Hive 配置迁移到 `job.config`；
- 将脚本调用切换到 `bin/pudding`；
- 重新生成或校验 capability manifest；
- 检查 `delete-on-success` 的 checkpoint 策略；
- Kafka Avro/Protobuf 作业增加 Schema Registry runtime；
- JDBC 增量作业在 source 顶层配置 `cursorField`。

---

## 当前已知限制

- 流式通用 DLQ/quarantine 尚未整合到同一作业。
- 没有 Prometheus `/metrics`、OpenTelemetry 和 streaming `/healthz` / `/readyz`。
- CDC 尚未覆盖 Oracle、SQL Server 和多表 include list。
- CDC schema 变化仍需调整配置并使用新 checkpoint。
- 流式 join 目前仅支持 inner，outer/semi/anti 和高级 state timer 尚未覆盖。
- Secret 仍以 env/file 为主，尚未接入 Vault/AWS/GCP/Azure Secret Manager。
- Iceberg REST/Glue/Nessie/Unity 等 catalog 尚未覆盖。
- 湖仓表自动创建、自动注册和 schema evolution 策略尚未形成闭环。
- Paimon catalog full refresh 仍为非原子 opt-in。
- YARN cluster 生命周期通过 ResourceManager REST 地址跟踪，未实现 Kerberos 认证；受保护集群
  需要通过已认证的网关访问。
- Hudi CDC 对 base-file/replace-commit 镜像（包括 `bulk_insert` 和纯插入提交）中的 DECIMAL
  采用 fail-closed：Hudi 的 InternalRow JSON 只保留有损的零/非零标记。表配置
  `hoodie.populate.meta.fields=false` 时，BINARY 无法区分两种编码，CDC 读取会被拒绝。
- File/Hive full refresh 仍为 reader 非原子。
- 真实 OSS/Azure/GCS/COS 端点、MySQL/PostgreSQL CDC sink 容器回归仍需在完整 CI 环境验证。
- JDBC incremental 只支持单游标列，不支持复合游标、delete 检测和可恢复 snapshot。
- Schema Registry Protobuf 的 well-known types 支持有限，当前重点为 `Timestamp`。

---

## 质量验证

本版本开发过程中已完成：

- JDK 11 全量 `mvn clean verify`：通过（2026-11-08，61 个模块，0 失败；
  本机耗时 7h44m，包含宿主机休眠时间）；
- 代码审查 P0/P1 及后续审计修复后的 JDK 11 全量 `mvn clean verify`：通过
  （2026-11-11，61 个模块，0 失败，14 分 08 秒）；
- YARN cluster 跟踪和最终 v1.0 审查后的 JDK 11 全量 `mvn clean verify`：通过
  （2026-11-12，61 个模块，0 失败，13 分 50 秒）；
- 对象存储、HDFS、表维护、maintenance-server、capabilities 的本地全链路
  用例通过（OS-01 ～ OS-08、E2E-15）；
- 规模门禁：1.3 GiB transform、10,000,000 行 JDBC 增量、
  1.3 GiB Iceberg 全量替换、10,000,013 行 MySQL CDC 初始快照 → Hudi、
  10,000,000 条 Kafka JSON → Hudi；
- JDK 17 定向测试：
  - schema registry runtime；
  - Kafka source；
  - JDBC incremental source；
  - JDBC CDC / streaming；
  - 子进程崩溃恢复；
- core、CDC、JDBC、maintenance、client、lakehouse 回归测试；
- H2 JDBC 增量/CDC 测试；
- Mock Schema Registry Avro/Protobuf 测试；
- MinIO/S3A、HDFS、Redis、etcd、Testcontainers 测试按环境启用。

容器测试在 Docker 不可用时按项目约定跳过；`PUDDING_TESTCONTAINERS_REQUIRED=true` 可用于 CI 强制失败。

---

## 后续建议

优先补生产运维能力：

1. Prometheus/OTel + streaming health endpoint；
2. 通用流式 DLQ/quarantine；
3. Secret Manager 和 maintenance-server TLS/RBAC；
4. Oracle/SQL Server CDC、DDL/schema evolution；
5. 湖仓表自动创建、注册和 schema evolution。

不建议优先做：

- 内置调度器；
- 多 sink 全局事务；
- 旧配置自动迁移；
- 将 Gravitino 变为强 SDK 依赖；
- 把所有连接器 SDK 塞进 core。
