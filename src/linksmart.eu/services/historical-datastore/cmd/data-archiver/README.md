Data Archiver
=====

Data Archiver is a simmple component that given a LinkSmart® deployment model (https://linksmart.eu/redmine/projects/model-repository) OR Resource Catalog subscribes to data streams of all LinkSmart® Resources defined in the model / published in the catalog and persists their output in the HDS.

# Use Cases

Data Archiver can be configured to use either LinkSmart® Resource Catalog or LinkSmart® Model Repository. These use cases are described separately below.

## Resource Catalog

**TODO:** NOT IMPLEMENTED
When configured to use Resource Catalog, Data Archiver retrieves whole catalog and subscribes to data streams of all Resources that have MQTT protocol defined, **unless** the Resource has `meta.data_archiver_ignore` set to `true`.

## Model Repository

**TODO:** NOT IMPLEMENTED
When configured to use Model Repository, Data Archiver parases the Domain Model (either downloading it from Model Repository or reading from file) and subscribes to data streams of all Resources that have MQTT protocol defined, **unless** the Resource has `meta.data_archiver_ignore` set to `true`.


# Configuration

The configuration is provided in JSON:

```json
{
    "rc": {
        "endpoint": "string",
        "auth": {}
    },
    "mr": {
        "defaultHost": "string",
        "endpoint": "string",
        "model": ""
    },
    "hds": {
        "endpoint": "string",
        "aggregations": [],
        "auth": {}
    },
    "mqtt": {
        "url": "string",
        "username": "string",
        "password": "string",
        "caFile": "string",
        "keyFile": "string"
    }
}
```

Example config:
```json
{
  "mr": {
    "defaultHost": "http://localhost",
    "endpoint": "file:///tmp/model.json"
  },
  "hds": {
    "endpoint": "http://localhost:8085"
  },
  "mqtt": {
    "tcp://mqtt-broker.local:1883": {
      "url": "tcp://localhost:1883"
    }
  }
}
```