# Hudi Source Configuration

The Hudi source supports reading data from Apache Hudi tables with various query types:

```json
{
  "source": [
    {
      "type": "hudi-source",
      "describe": "read hudi table with incremental query",
      "id": "source-1",
      "config": {
        "table": "default.test_table",
        "tablePath": "hdfs://namenode:port/path/to/hudi/table",
        "queryType": "incremental",
        "beginTime": "20230101000000",
        "endTime": "20230102000000",
        "options": {
          "hoodie.datasource.read.use.newer.timeline": "true"
        }
      }
    },
    {
      "type": "hudi-source",
      "describe": "read hudi table with cdc query",
      "id": "source-2",
      "config": {
        "table": "default.test_table_cdc",
        "tablePath": "hdfs://namenode:port/path/to/hudi/table_cdc",
        "queryType": "cdc"
      }
    },
    {
      "type": "hudi-source",
      "describe": "read hudi table with read_optimized query",
      "id": "source-3",
      "config": {
        "table": "default.test_table_ro",
        "tablePath": "hdfs://namenode:port/path/to/hudi/table_ro",
        "queryType": "read_optimized"
      }
    }
  ]
}
```

## Supported configuration options:
- `table`: The table name
- `tablePath`: The path to the Hudi table
- `queryType`: The query type to use (default: "snapshot")
  - `snapshot`: Read the latest snapshot of the table
  - `incremental`: Read incremental changes between beginTime and endTime
  - `read_optimized`: Read optimized query for read-heavy workloads
  - `cdc`: Change Data Capture query to read change records
- `beginTime`: Start time for incremental queries (required for incremental query type)
- `endTime`: End time for incremental queries (required for incremental query type)
- `options`: Additional Hudi options as key-value pairs
