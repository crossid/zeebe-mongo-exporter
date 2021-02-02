# Zeebe Mongo Exporter

The _Zeebe Mongo Exporter_ acts as a bridge between
[Zeebe](https://zeebe.io/) and [MongoDB](https://www.mongodb.com),
by exporting records written to Zeebe streams as documents into several collections.

## Usage

You can configure the Mongo Exporter with the following arguments:

* `url` (`string`): a valid Mongo URI as a string (e.g. `http://localhost:27017/`)

All other options fall under a two categories, both expressed as nested maps: `bulk` and `col`.

### Bulk

To avoid doing too many expensive requests to the Mongo cluster, the exporter
performs batch updates by default. The size of the batch, along with how often
it should be flushed (regardless of size) can be controlled by configuration.

For example:

```yaml
...
  exporters:
    mongo:
      args:
        delay: 5
        size: 1000
```

With the above example, the exporter would aggregate records and flush them to Mongo
either:
  1. when it has aggregated 1000 records.
  2. 5 seconds have elapsed since the last flush (regardless of how many records were aggregated).

More specifically, each option configures the following:

* `delay` (`integer`): a specific delay, in seconds, before we force flush the current batch. This ensures
that even when we have low traffic of records we still export every once in a while.
* `size` (`integer`): how many records a batch should have before we export.

### Collection

In most cases, you will not be interested in exporting every single record produced by a
Zeebe cluster, but rather only a subset of them. This can also be configured to limit the
kinds of records being exported (e.g. only events, no commands), and the value type of these
records (e.g. only job and workflow values).


Here is a complete, default configuration example:

```yaml
...
  exporters:
    mongo:
      # Mongo Exporter ----------
      # An example configuration for the mongo exporter:
      #

      className: io.crossid.zeebe.exporter.MongoExporter
      jarPath: exporters/zeebe-mongo-exporter.jar

      args:
        url: http://localhost:27017/
        dbName: zeebe

        bulk:
          delay: 5
          size: 1000
        col:
          prefix: zeebe-record
          createCollections: true
          command: false
          event: true
          rejection: false
          deployment: true
          error: true
          incident: true
          job: true
          jobBatch: false
          message: true
          messageSubscription: true
          variable: true
          variableDocument: true
          workflowInstance: true
          workflowInstanceCreation: false
          workflowInstanceSubscription: false
          timers: true
```
