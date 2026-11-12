# File sink

`file-sink` writes a DataFrame to CSV, text, JSON, Parquet or ORC files. Batch and streaming
writes are supported. The component type is `file-sink`; the Maven module and jar are named
`file-common-sink`.

## Configuration

```yaml
sink:
  - type: file-sink
    id: sink-1
    config:
      path: hdfs:///data/orders
      fileType: parquet
      saveMode: append
      options:
        compression: snappy
    dependency: source-1
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | string | required | Output directory or file path. |
| `fileType` | string | required | `csv`, `text`, `json`, `parquet` or `orc`. |
| `saveMode` | string | — | Required for batch writes: `append`, `overwrite`, `ignore`, `error` or `errorifexists`. |
| `options` | map | — | Spark writer options. |
| `isStreaming` | boolean | `false` | Uses `writeStream`. Aliases `streamWrite`, `streamWriter` and `streamingWriter` are accepted. |
| `outputMode` | string | `append` | Streaming output mode; only `append` is supported. |
| `checkpointLocation` | string | `<path>_checkpoint` | Streaming checkpoint directory. |
| `checkpointRetention` | string | `keep` | `keep` or `delete-on-success`; the latter requires a finite trigger. |
| `timeoutMs` | number | — | Optional wait before the streaming query is stopped. |
| `allowNonAtomicReplace` | boolean | `false` | Required for `sync` full refresh. The sink stages a complete snapshot and swaps it with a backup; a fixed directory path is not reader-atomic. |

A streaming file sink supports only append output. Use append or `append`-compatible downstream
processing.

For `sync` with `strategy: full`, set `allowNonAtomicReplace: true`. The sink writes a staging
directory, renames the previous target to a backup, publishes the staging directory and restores
the backup if publication fails. This is crash-safe but not reader-atomic: a reader can briefly
see a missing target while the directories are swapped. Only one full-refresh writer may target a
path at a time; overlapping runs share the staging and backup names and can corrupt each other.
Coordinate replacements with the external scheduler. Object-store paths fall back to an in-place
overwrite and are never advertised as atomic.
