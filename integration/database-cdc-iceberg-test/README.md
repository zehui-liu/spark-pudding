# Native CDC Iceberg regression

This test-only module runs a real MySQL binlog stream through `mysql-cdc-source` into an
Iceberg table using `iceberg-sink` CDC streaming. MySQL is the representative CDC engine;
PostgreSQL and MongoDB source behavior is covered by their dedicated suites.

The case covers initial snapshot, insert/update/delete, checkpoint restart and final target-row
verification. It is intentionally separate from the Hudi/Paimon regression: Paimon's
`paimon-spark` artifact bundles Spark compatibility classes, including `MergeRows`, which are
incompatible with the Spark/Iceberg class loaded by Iceberg 1.5.0 in one test classpath.

```shell
PUDDING_TESTCONTAINERS=true \
PUDDING_TESTCONTAINERS_REQUIRED=true \
  mvn -pl integration/database-cdc-iceberg-test -am test \
    -DwildcardSuites=org.pudding.integration.DatabaseCdcIcebergTest
```

The suite uses `mysql:8.0`; `PUDDING_TEST_MYSQL_IMAGE` can override the image.
