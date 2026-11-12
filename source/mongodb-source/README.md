# MongoDB Source Configuration

`mongodb-source` reads a collection with the official MongoDB Spark connector. Credentials and
connection options belong to a reusable connection; the source selects the resource.

```yaml
connection:
  - name: mongo-prod
    type: mongodb
    endpoint: mongodb://mongo-1:27017,mongo-2:27017
    auth:
      username: pudding
      password: ${secret:env/MONGO_PASSWORD}
    properties:
      authSource: admin
      replicaSet: rs0

source:
  - type: mongodb-source
    id: orders
    connection: mongo-prod
    config:
      database: app
      collection: orders
      options:
        readPreference.name: secondaryPreferred
```

The connection provider injects `auth` into the MongoDB connection string and appends other
connection properties as URI options. Properties that already start with `spark.mongodb.` are
passed to the Spark connector unchanged.

## Source options

- `database`: database name (required)
- `collection`: collection name (required)
- `options`: additional reader options. Keys starting with `spark.mongodb.` are passed through;
  other keys are prefixed with `spark.mongodb.read.` (optional)

## Inline URI

For an existing job that does not use a connection yet, `config.uri` or `config.connectionUri`
is accepted. Credentials embedded in that URI are the caller's responsibility.

```yaml
source:
  - type: mongodb-source
    id: orders
    config:
      uri: mongodb://localhost:27017
      database: app
      collection: orders
```

The source performs batch reads. Change-stream and other streaming reads are not exposed by this
connector; use [`mongodb-cdc-source`](../database-cdc-source/README.md) for a native change stream.

The MongoDB connector runtime is packaged in `mongodb-runtime`; the client selects it
automatically. A direct `spark-submit` invocation must add the runtime jar to `--jars`.
