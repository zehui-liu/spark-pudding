# Elasticsearch Source Configuration

The Elasticsearch source supports reading data from Elasticsearch 8 with various options:

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

- `esNodes`: The Elasticsearch nodes address (Required)
- `esIndex`: The index name to read from (Required)
- `esPort`: The Elasticsearch port (Optional, default: "9200")
- `esUser`: Username for authentication (Optional)
- `esPass`: Password for authentication (Optional)
- `fields`: A comma-separated string or list of fields to include in the result (Optional)
- `scrollSize`: The size of each scroll batch for reading large datasets (Optional)
- `limit`: The total number of rows to read from the index (Optional)
- `options`: Additional `elasticsearch-hadoop` options as key-value pairs (Optional). Useful options include:
  - `es.read.field.as.array.include`: Specify fields that should be read as arrays even if they contain only one element.
