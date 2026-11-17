# Lakehouse object-store regression tests

Opt-in integration tests for Hudi and Paimon table data on an S3-compatible object store. These
are data-path tests: they write table files, timeline/snapshot commits, incremental batches and
streaming checkpoints to object storage, then read the results back.

They are separate from `ContainerRemoteStateTest`, which only covers incremental state and
checkpoint files, and from `IcebergS3FileIOTest`, which covers Iceberg's `S3FileIO` data layer.

## Run with Testcontainers

```shell
PUDDING_TESTCONTAINERS=true \
PUDDING_TESTCONTAINERS_REQUIRED=true \
  mvn -pl integration/lakehouse-object-store-test -am test \
    -DwildcardSuites=org.pudding.integration.HudiObjectStoreTest,org.pudding.integration.PaimonObjectStoreTest
```

The default image is `quay.io/minio/minio:RELEASE.2023-09-04T19-57-37Z`; override it with
`PUDDING_TEST_MINIO_IMAGE`.

## Run against an existing endpoint

```shell
PUDDING_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
PUDDING_TEST_S3_ACCESS_KEY=pudding \
PUDDING_TEST_S3_SECRET_KEY=pudding-secret \
  mvn -pl integration/lakehouse-object-store-test -am test \
    -DwildcardSuites=org.pudding.integration.HudiObjectStoreTest,org.pudding.integration.PaimonObjectStoreTest
```

The endpoint must permit creating the generated test bucket. Use a disposable account or bucket.
The suites do not delete the bucket on an external endpoint.

## Coverage

- Hudi: S3A table creation, upsert, incremental read, atomic full refresh, failed-refresh
  rollback, streaming update with an S3A checkpoint.
- Paimon: S3A catalog table creation, CDC, full refresh, streaming update with an S3A checkpoint.
- Both suites exercise Spark's Hadoop S3A configuration and the packaged `hadoop-aws-runtime`
  dependency rather than the local filesystem.

When the environment is not configured, the suites are skipped. With
`PUDDING_TESTCONTAINERS_REQUIRED=true` or an explicit external endpoint, setup failures fail the
suite instead of skipping it.
