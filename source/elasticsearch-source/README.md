# Elasticsearch Source Configuration

The Elasticsearch source supports reading data from Elasticsearch 8 with various options.

With a named connection, cluster addresses and credentials live in the connection while the source
only selects the index:

```yaml
connection:
  - name: es-logs
    type: elasticsearch
    endpoint: https://es-1:9200,es-2:9200
    auth:
      username: elastic
      password: ${secret:env/ES_PASSWORD}

source:
  - type: elasticsearch-source
    id: logs
    connection: es-logs
    config:
      esIndex: app-logs-*
      fields: timestamp,level,message
```

The legacy inline form is still supported:

```json
{
  "source": [
    {
      "type": "elasticsearch-source",
      "describe": "read data from elasticsearch 8",
      "id": "source-1",
      "config": {
        "esNodes": "http://elasticsearch-host:9200",
        "esIndex": "my_index",
        "esUser": "elastic",
        "esPass": "your_password",
        "esPort": "9200",
        "fields": "col1,col2,col3",
        "scrollSize": 1000,
        "limit": 10000,
        "options": {
          "es.nodes.wan.only": "true"
        }
      }
    }
  ]
}
```

## Supported configuration options:

- `esNodes`: The Elasticsearch nodes address (required in the legacy inline form; ignored with a warning when a connection is referenced)
- `esIndex`: The index name to read from (Required)
- `esPort`: The Elasticsearch port (Optional, default: "9200")
- `esUser`: Username for authentication (Optional)
- `esPass`: Password for authentication (Optional)
- `fields`: A comma-separated string or list of fields to include in the result (Optional)
- `scrollSize`: The size of each scroll batch for reading large datasets (Optional)
- `limit`: The total number of rows to read from the index (Optional)
- `options`: Additional `elasticsearch-hadoop` options as key-value pairs (Optional). Useful options include:
  - `es.read.field.as.array.include`: Specify fields that should be read as arrays even if they contain only one element.

The source performs batch reads. It does not expose structured streaming or change-feed options.

## Runtime

The Elasticsearch connector runtime is packaged in `elasticsearch-runtime`. The client selects it
automatically; a direct `spark-submit` invocation must add the runtime jar to `--jars`.
