Historical Datastore
===================

Implementation of the [Historical Datastore Service](https://linksmart.eu/redmine/projects/historical-datastore)

## Deployment
The repository contains the code and its dependencies managed with [gb](http://getgb.io/)

To compile & run:
* `go get github.com/constabulary/gb/...`
* `gb build all`
* `./bin/historical-datastore`


## Code structure

The code can be found in `src/linksmart.eu/services/historical-datastore` where:
* `/cmd` - executables (`main` package)
    - `/cmd/historical-datastore`: implementation of a standalone service providing full API
    - TBD: tools?
* `/registry` - implementation of [Registry API](https://linksmart.eu/redmine/projects/historical-datastore/wiki/Historical_Datastore_API#Registry-API)
* `/data` - implementation of [Data API](https://linksmart.eu/redmine/projects/historical-datastore/wiki/Historical_Datastore_API#Data-API)
* `/aggregation` - implementation of [Aggregation API](https://linksmart.eu/redmine/projects/historical-datastore/wiki/Historical_Datastore_API#Aggregation-API)

## TODO
* Enforce resource name uniqueness
* Show logs after the start of http server
* Disconnect MQTT Connectors on shutdown
* Handle influx no database error