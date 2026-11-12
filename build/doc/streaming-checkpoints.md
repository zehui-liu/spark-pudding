# Streaming checkpoints

A streaming job writes exactly one sink and stores that query's progress in the sink's Spark
checkpoint. The checkpoint contains Kafka offsets, committed micro-batch metadata and streaming
state. It is part of the job's correctness boundary: keep the same `checkpointLocation` across
restarts. Stateful operators store windows and deduplication keys in the checkpoint too;
changing their event-time, watermark, window or keys can make an existing checkpoint
incompatible. A stateful checkpoint also stores a source/transform/Spark-version contract and
fails the restart when it changes. See [streaming-state.md](streaming-state.md).

Native database CDC sources also use the sink checkpoint. MySQL/PostgreSQL offsets are binlog
positions or LSNs and MongoDB offsets are snapshot positions or change-stream resume tokens; no
connection strings or credentials are written to checkpoint state. MySQL schema history is
stored under `<checkpoint>/sources/<source-id>/cdc-schema-history/<logical-server-name>/`.
Keep that directory with the
checkpoint: deleting it forces a schema recovery failure or a re-snapshot, depending on the
connector. See [database-cdc.md](database-cdc.md).

## Retention policy

Every streaming sink accepts one additional config key:

| Key | Values | Default | Meaning |
| --- | --- | --- | --- |
| `checkpointRetention` | `keep`, `delete-on-success` | `keep` | Keep the checkpoint after the query terminates, or delete it after a successful finite run. |

```yaml
sink:
  - type: hudi-sink
    id: orders
    dependency: kafka_orders
    config:
      tablePath: hdfs://namenode:8020/hudi/orders
      checkpointLocation: hdfs://namenode:8020/checkpoints/orders
      checkpointRetention: keep
      outputMode: append
```

`keep` is the default and is the only safe policy for a long-running query or for a job that must
resume after a failure.

`delete-on-success` is opt-in and is accepted only when `job.trigger` is a finite trigger:
`once` or `available-now`. The query must consume the data available at start and terminate by
itself. Before deleting, the runtime requires positive evidence that the finite run completed:

- `available-now`: the last progress update must be the final zero-row trigger that occurs after
  the last committed offset. The runtime temporarily disables Spark's zero-row progress throttle
  so this evidence is observable. A zero-row state-cleanup batch is not accepted because it still
  commits an offset.
- `once`: the single batch must have committed and produced a progress update.

An external `query.stop()` before that evidence exists, a failed query, a timeout or a
continuous/processing-time trigger leaves the checkpoint in place. Spark reports an external stop
and a naturally finished finite trigger with the same `exception=None, isActive=false` state, so
the completion evidence is required instead of relying on `awaitTermination` alone.

Deleting a checkpoint does not delete lakehouse data. It makes the next run start from the
source's configured starting position again (for example Kafka `startingOffsets`). Use
`delete-on-success` only for disposable runs that are externally coordinated, not for a job that
is restarted by an orchestrator. The engine does not schedule jobs; see
[job-modes.md](job-modes.md#scheduling).

`console-sink` does not use a checkpoint, so it rejects `delete-on-success`.

## Maintenance CLI

`bin/pudding checkpoint` inspects or removes checkpoints without submitting a Spark job. It uses
`SPARK_HOME` for Hadoop classes, or `hadoop classpath` when `SPARK_HOME` is not set. Set
`HADOOP_CONF_DIR` for HDFS or object-store credentials; the object-store runtimes are loaded from `../libs`
automatically. Additional object-store providers can be placed in
`PUDDING_OBJECT_STORE_DIR` or supplied through `PUDDING_CHECKPOINT_CLASSPATH`.

```shell
# show existence, shape, size, newest modification, age and commit/offset counts
bin/pudding checkpoint show hdfs:///pudding/checkpoints/orders

# machine-readable status for monitoring and automation
bin/pudding checkpoint status hdfs:///pudding/checkpoints/orders --json

# delete one checkpoint; --dry-run prints the action only
bin/pudding checkpoint delete hdfs:///pudding/checkpoints/orders --dry-run
bin/pudding checkpoint delete hdfs:///pudding/checkpoints/orders

# remove checkpoint directories older than 7 days under a parent directory
bin/pudding checkpoint cleanup hdfs:///pudding/checkpoints \
  --older-than 7d --dry-run
bin/pudding checkpoint cleanup hdfs:///pudding/checkpoints \
  --older-than 7d

# retain only the newest two checkpoint directories
bin/pudding checkpoint cleanup hdfs:///pudding/checkpoints --keep-last 2
```

Object-store settings can be supplied without a Spark session:

```shell
bin/pudding checkpoint show s3a://bucket/pudding/checkpoints/orders \
  --conf-file /etc/pudding/checkpoint-s3a.properties
bin/pudding checkpoint cleanup s3a://bucket/pudding/checkpoints \
  --older-than 7d --hadoop-conf /etc/hadoop/conf
```

`--conf-file` accepts only `fs.*` properties, for example:

```properties
fs.s3a.endpoint=https://s3.ap-southeast-1.amazonaws.com
fs.s3a.path.style.access=false
fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider
```

Never put access keys or passwords on the command line. Use an IAM role, IRSA, instance
profile, environment credential provider, or a `0600` configuration file. `--hadoop-conf`
adds the XML files in a Hadoop configuration directory.

`status` returns the newest modification time, checkpoint age, commit/offset counts and the
last numeric commit/offset names. Add `--json` for automation; it is the recommended input for
a monitoring probe or an external janitor.

`cleanup` scans only direct directory children of the given root. The root must be the parent
of the checkpoint directories, not a checkpoint itself. It refuses a filesystem root and
refuses to treat a checkpoint as the cleanup root. `--older-than` selects by age; `--keep-last
N` retains the N newest and deletes the rest. When both are present, a checkpoint must satisfy
both conditions. Age is based on the newest modification time of any file in the checkpoint
tree, not on the checkpoint directory itself: new commits update nested `commits/` and
`offsets/` entries, so checking only the top-level directory could delete an active checkpoint.
Directory modification times are ignored because object stores such as S3A synthesize them from
the current clock; ignoring them keeps `status`, `--older-than` and `--keep-last` correct on
object storage. A directory with no files falls back to its own modification time.
The tool does not verify whether a writer is still active: stop the jobs that own the target
checkpoints before deleting them.

The duration syntax is a positive integer followed by `ms`, `s`, `m`, `h` or `d`. Deletion is
recursive for the selected checkpoint directory only.

## Object storage

S3A is supported through the bundled `hadoop-aws-runtime` and `fs.s3a.*`. The distribution also
ships optional OSS, Azure ABFS/WASB, GCS and COS runtime packs; the client selects them when the
job configuration contains `oss://`, `abfs://`, `abfss://`, `wasb://`, `wasbs://`, `gs://` or
`cosn://`. Each provider still needs its own `fs.*` settings. Use `s3a://`, not EMR's `s3://`,
for the Hadoop S3A runtime.

Checkpoint metadata is more sensitive to rename and consistency semantics than data files. The
recommended production layout is:

```text
data:       s3a://lakehouse/warehouse/...
checkpoint: hdfs:///pudding/checkpoints/<job>/  or a persistent PVC/NFS path
```

If checkpoint must live on object storage:

- use one dedicated prefix per job and only one writer;
- do not put checkpoint under the lakehouse warehouse or table path;
- disable lifecycle expiration for the checkpoint prefix;
- grant `ListBucket`, `GetObject`, `PutObject`, `DeleteObject` and `AbortMultipartUpload`
  for the prefix;
- prefer a strongly consistent provider and test restart and `delete-on-success` behavior.

For Iceberg data on object storage, `warehouse: s3://...` selects `S3FileIO` from the
`iceberg-runtime`; the Spark checkpoint still uses Hadoop S3A. GCS/OSS/Azure table data should
set the matching Iceberg `io-impl` explicitly in the catalog properties when the selected
Iceberg version provides it.

## Metrics

`SessionHandler` registers a Spark `StreamingQueryListener` for every application. The
`org.pudding.core.streaming:type=StreamingMetrics` JMX bean exposes active query, batch, input
row and batch duration counters. The driver also logs structured `streaming.query.started`,
`streaming.query.progress`, `streaming.query.completed` and `streaming.query.failed` events.
The progress log contains numeric batch data only; checkpoint paths and configuration values are
not logged.

## Relationship to incremental state

The checkpoint is not the `sync` + `incremental` cursor journal. `sync incremental` progress,
pending transactions and its lock live in the incremental state backend, inspected with
`bin/pudding state`. A `streaming` job does not use the incremental state journal, and a
`sync incremental` job does not use a Spark checkpoint. See
[incremental-sync.md](incremental-sync.md) for the batch state protocol.
