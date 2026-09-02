# Iceberg Source Configuration

The Iceberg source supports reading data from Apache Iceberg tables with various options:

```json
{
  "source": [
    {
      "type": "iceberg-source",
      "describe": "read iceberg table",
      "id": "source-1",
      "config": {
        "table": "database.table_name",
        "catalog": "spark_catalog",
        "filterExpr": "date >= '2023-01-01'",
        "snapshotId": 123456789,
        "options": {
          "basePath": "hdfs://path/to/hive/warehouse"
        },
        "forceConvertSchema": true,
        "schema": {
          "column1": "string",
          "column2": "int",
          "column3": "double"
        }
      }
    }
  ]
}
```

## Supported configuration options:
- `table`: The table name in the format `database.table_name` or full table identifier
- `path`: Alternative to table, specify the direct path to the Iceberg table
- `catalog`: The catalog name (default: "spark_catalog")
- `filterExpr`: SQL filter expression to apply to the data
- `snapshotId`: Read from a specific snapshot ID
- `asOfTimestamp`: Read from a specific timestamp
- `branch`: Read from a specific branch
- `tag`: Read from a specific tag
- `options`: Additional Iceberg options as key-value pairs
- `forceConvertSchema`: Whether to force convert the schema (default: false)
- `schema`: Schema definition for forceConvertSchema (required if forceConvertSchema is true)
