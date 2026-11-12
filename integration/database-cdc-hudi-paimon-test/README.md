# Native CDC Hudi/Paimon regression

This test-only module runs a real MySQL binlog stream through `mysql-cdc-source` and writes it
to Hudi and Paimon with their CDC streaming sinks. MySQL is the representative source here;
the PostgreSQL and MongoDB change semantics are covered by their dedicated source suites.

Each case covers:

1. initial snapshot (including a stale pre-created Hudi row to prove snapshot upsert);
2. insert, update and delete;
3. stopping the query and restarting from the same checkpoint while inserting another row;
4. verifying the final target rows.

The suite is opt-in:

```shell
PUDDING_TESTCONTAINERS=true \
PUDDING_TESTCONTAINERS_REQUIRED=true \
  mvn -pl integration/database-cdc-hudi-paimon-test -am test \
    -DwildcardSuites=org.pudding.integration.DatabaseCdcHudiPaimonTest
```

It uses `mysql:8.0` and `PUDDING_TEST_MYSQL_IMAGE` can override the image. Hudi and Paimon run
in one Spark session because their Spark classes are compatible. Iceberg is in a separate module
so Paimon's bundled Spark compatibility classes cannot shadow Iceberg's `MergeRows` class.
