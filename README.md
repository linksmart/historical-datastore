Historical Datastore
===================

Implementation of the [Historical Datastore Service](https://linksmart.eu/redmine/projects/historical-datastore).

## Code structure

The code can be found in `src/linksmart.eu/services/historical-datastore` where:

* `/registry` - implementation of [Registry API](https://linksmart.eu/redmine/projects/historical-datastore/wiki/Historical_Datastore_API#Registry-API)
* `/data` - implementation of [Data API](https://linksmart.eu/redmine/projects/historical-datastore/wiki/Historical_Datastore_API#Data-API)
* `/aggregation` - implementation of [Aggregation API](https://linksmart.eu/redmine/projects/historical-datastore/wiki/Historical_Datastore_API#Aggregation-API)
* `/cmd` - executables (`main` package)
    - `/cmd/historical-datastore`: implementation of a standalone service providing full API.
    - `/cmd/data-archiver`: [Data-Archiver](https://www.linksmart.eu/redmine/projects/historical-datastore/wiki/Data_Archiver)

## Compile from source
This project and its dependencies are managed with [GB](http://getgb.io/) build tool. Once you have it installed, go to project root and compile:

```
gb build
```

## Run
Use -conf flag to set the config file path. If not set, `./conf/historical-datastore.json` will be used.
```
./bin/historical-datastore -conf historical-datastore.json
```
