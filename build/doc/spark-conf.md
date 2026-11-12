# Spark configuration flow

`job.config` is the job-level Spark configuration map. It is delivered to both the Spark
submission and the driver session, so it is useful to separate the two paths.

## 1. Configuration sources

| Source | Examples | Applied by |
| --- | --- | --- |
| `job.config` | `spark.executor.memory`, `spark.sql.shuffle.partitions` | client submission and driver `SessionHandler` |
| Client `--conf` | `spark.kubernetes.namespace`, target overrides | `AbstractSparkExecutionTarget` |
| Target configuration | master, deploy mode, Kubernetes image/namespace | execution target |
| Catalog configuration | `spark.sql.catalog.*`, Iceberg/Hudi/Paimon settings | `CatalogManager.install` |
| Spark runtime | `spark-submit --conf`, `spark-defaults.conf` | Spark itself |

The job configuration file is also passed to the driver through `--jobFile`, `--jobEnv` or a
Kubernetes Secret. Therefore `job.config` reaches Spark twice: once as submission configuration
and once when the driver creates the `SparkSession`.

## 2. Submission path

```text
job file
  -> JobConfigureParser
  -> JobConf.config: Option[Map[String, String]]
  -> JobConfiguration.getSparkConf()
  -> execution target builds SubmissionPlan
  -> spark-submit --conf ...
```

`JobConfiguration.getSparkConf()` reads only `job.config`. The target then merges configuration
in this order:

```text
job.config
  -> target-specific Spark settings
  -> client --conf values
  -> remove spark.master / spark.submit.deployMode / spark.app.name
```

The last step is important: master, deploy mode and application name are owned by the target,
not by `job.config`.

## 3. Driver path

Inside the Spark application:

```text
PuddingSpoon
  -> JobConfigSource.resolve(jobText | jobEnv | jobFile)
  -> JobConfigureParser
  -> JobPipelineConf.job
  -> BuildJob
       -> SecretResolver.resolveConfig(job.config)
       -> SessionHandler.sparkSessionBuilder
            -> new SparkConf()
            -> sparkConf.setAll(jobConf.config)
            -> CatalogManager.install(catalogs, sparkConf)
            -> SparkSession.builder().config(sparkConf)
            -> SparkSession.getOrCreate()
```

`SecretResolver` resolves secret references in `job.config` on the driver before the session is
created. The resolved values are used for SparkConf; logs still use the redacted configuration.
The submit client forwards `job.config` entries as `spark-submit --conf` arguments, so the
runtime SparkConf also contains the unresolved references. The resolver resolves those runtime
values on the driver before comparing job, catalog and runtime sources, which keeps an identical
setting from being reported as a conflict.

## 4. Resolved precedence

The driver now resolves the session configuration in `SparkConfigResolver` before creating the
`SparkSession`:

| Setting | Behavior |
| --- | --- |
| `spark.master` | runtime master wins; `job.master` is used only when no runtime master is set |
| `spark.submit.deployMode` | runtime deploy mode is authoritative; a mismatch only logs a warning |
| `spark.app.name` | owned by the execution target / `job.appName` |
| target-owned keys in `job.config` | ignored with a warning; use `job.master` and `job.deployMode` |
| same key in job/catalog/runtime | values must agree; otherwise the session fails before Spark starts |
| different keys from different sources | merged |
| `spark.sql.extensions` | merged from job, catalog and runtime instead of overwritten |

The resolver's conflict message names the key and the source labels (`job.config`,
`catalog '<name>'`, `runtime SparkConf`) but never prints the values.

At submission time the client still merges `job.config`, target settings and client `--conf`; the
target owns `spark.master`, `spark.submit.deployMode` and `spark.app.name`. The driver resolver is
the final guard for the values that reach the `SparkSession`.

## 5. Hive support

The former `job.enableHiveSupport` special case has been removed. Use the standard Spark key in
the job configuration:

```yaml
job:
  config:
    spark.sql.catalogImplementation: hive
```

The parser flattens nested `job.config` maps into dotted keys, so this is equivalent to the
`spark-submit --conf spark.sql.catalogImplementation=hive` form.

Hudi catalog properties still have an `enableHiveSupport` setting. That is a Hudi catalog-level
setting, not a job-level Hive switch; it controls the catalog provider's Spark session
configuration.

## 6. Contract and future work

The driver resolver already enforces the shared precedence: conflicting values fail before Spark
starts, deployment keys are target-owned, and the resolver's messages name keys and source labels
without values. The remaining diagnostic improvement is to expose the resolved SparkConf
(redacted) through a command so operators can inspect effective values without reading logs.
