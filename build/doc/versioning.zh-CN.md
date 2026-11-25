# 版本与发布策略

[English](versioning.md) | 简体中文

本文档定义 spark-pudding 产品版本、Spark 兼容线、分支和发布产物的管理规则。它是维护者和
使用者的策略说明，不是历史发布记录。

## 版本号规则

spark-pudding 使用 `MAJOR.MINOR.PATCH`：

```text
1.1.0
│ │ └─ PATCH：仅 bug 修复
│ └─── MINOR：向后兼容的新功能和新的 Spark 小版本支持
└───── MAJOR：Spark 大版本基线或破坏性 API/配置变更
```

产品版本和 Spark 版本相互独立：

```text
spark-pudding 1.1.0 + Spark 3.4
spark-pudding 1.1.0 + Spark 3.5
spark-pudding 2.0.0 + Spark 4.0
```

## 变更对应的版本位

| 变更 | 版本位 | 示例 |
| --- | --- | --- |
| 仅 bug 修复 | PATCH | `1.1.1` |
| 新增 source、sink 或 transform 能力 | MINOR | `1.2.0` |
| 新增 Spark 3.5 支持 | MINOR | `1.1.0` |
| 新增 Spark 4.0 支持 | MAJOR | `2.0.0` |
| 破坏性 API、配置或默认行为变更 | MAJOR | `2.0.0` |

规则：

- PATCH 版本不能新增 connector、transform 或新的配置项；
- MINOR 版本可以新增组件和 Spark 小版本支持，但现有 job 文件必须保持有效，现有行为必须
  保持兼容；
- MAJOR 版本可以改变 Spark 基线、Scala/JDK 基线、公共 API 或 job 语义，必须提供迁移说明；
- 新增 source、sink 或 transform 属于 MINOR 功能，不是 patch；
- 支持新的 Spark 大版本属于 MAJOR，即使 spark-pudding 自身 API 没有变化，因为运行时、
  依赖和兼容基线已经改变。

## 发布线

| 产品线 | Spark 基线 | 状态 |
| --- | --- | --- |
| `1.0.x` | Spark 3.4 | 当前维护线 |
| `1.1.x` | Spark 3.4、3.5 | 计划中的 minor 线 |
| `1.2.x` | Spark 3.4、3.5，并增加新组件 | 后续新增能力时的 minor 线 |
| `2.0.x` | Spark 4.0 | 计划中的 major 线 |

每条 minor 线维护自己的 patch 序列。例如 `1.2.0` 的修复发布为 `1.2.1`、`1.2.2`，而不是
`1.1.x`。

## 分支模型

```text
main                  下一个 minor 或 major 版本的开发主线
v-1.0                 1.0.x 维护
v-1.1                 1.1.x 维护
v-2.0                 2.0.x 维护
```

规则：

- 新功能和新的 Spark 支持在 `main` 上开发；
- 发布维护分支只接收 bug 修复；
- 修复先合入 `main`，再 cherry-pick 到受影响的维护分支；
- 产品线发布时创建维护分支，不为每个 Spark 构建单独建分支；
- Spark 3.4 和 3.5 使用同一分支和同一份源码构建，不拆分支；
- 只有在源码或依赖基线无法共享时，才创建独立分支。

## Tag 和发布产物

一次发布使用一个不可变的产品 tag：

```text
v1.1.0
```

同一个 tag 可以发布多个 Spark 专用发行包：

```text
spark-pudding-1.1.0-spark3.4.tar.gz
spark-pudding-1.1.0-spark3.5.tar.gz
```

除非产品线确实需要独立发布，否则不要创建 `v1.1.0-spark3.4`、`v1.1.0-spark3.5` 这类
按 Spark 版本拆分的 tag。

每个发行包必须标识兼容基线：

```yaml
productVersion: 1.1.0
sparkVersion: 3.5.x
sparkCompatVersion: "3.5"
scalaBinaryVersion: "2.12"
```

客户端在发行包 Spark 版本与目标运行时不匹配时，必须拒绝或给出明确警告。

## 兼容承诺

| 发布类型 | 源码兼容 | Job 配置 | 组件 |
| --- | --- | --- | --- |
| PATCH | 不破坏公共 API | 不新增必填字段 | 仅 bug 修复 |
| MINOR | 只做增量 API | 现有 job 保持有效 | 可以新增 source/sink/transform |
| MAJOR | 允许破坏性变更 | 需要迁移 | Spark/API 基线可变 |

在同一个 Spark 大版本内，spark-pudding 自身的公共 API 应在 PATCH 和 MINOR 版本之间保持
稳定。Spark internal API 不属于兼容承诺范围。

## 发布检查清单

1. 按上表选择版本位；
2. 使用 `mvn versions:set` 更新 Maven reactor 版本；
3. 为每个支持的 Spark profile 更新 Spark 和 runtime 版本属性；
4. 矩阵变化时更新 `build/doc/compatibility.md` 和本文档；
5. 每个支持的 Spark profile 执行 `mvn clean verify`；
6. 每个 Spark profile 跑容器回归测试；
7. 校验组装后的发行包和 capability manifest；
8. 创建一个不可变的产品 tag；
9. 在该 tag 下发布各 Spark 专用发行包；
10. 发布 Release Notes；MAJOR 版本必须包含迁移说明；
11. 通过 cherry-pick 保持维护分支同步。

## 示例

| 变更 | 版本 |
| --- | --- |
| 修复 1.1.0 中的 CDC checkpoint bug | `1.1.1` |
| 新增 Elasticsearch source | `1.2.0` |
| 在 1.0 线上新增 Spark 3.5 构建 | `1.1.0` |
| 新增 Spark 4.0 支持 | `2.0.0` |
| 改变默认 sink 行为 | `2.0.0` |

## Spark 4.0

Spark 4.0 会改变平台基线，包括 Scala/JDK、依赖矩阵以及部分 Spark API。即使 spark-pudding
对用户暴露的 API 基本不变，Spark 4.0 支持仍按 `2.0.0` 处理。Spark 3.4/3.5 继续保留在
1.x 产品线上。
