Historical Datastore
===================

Implementation of the [Historical Datastore Service](https://docs.linksmart.eu/display/HDS).

## Code structure

The code can be found in `src/linksmart.eu/services/historical-datastore` where:

* `/` - implementation of a standalone service providing full API.
* `/registry` - implementation of Registry API
* `/data` - implementation of Data API
* `/aggregation` - implementation of Aggregation API


## Compile from source

```
git clone https://code.linksmart.eu/scm/hds/historical-datastore.git src/code.linksmart.eu/hds/historical-datastore
export GOPATH=`pwd`
go install code.linksmart.eu/hds/historical-datastore
```


## Run
Use -conf flag to set the config file path. If not set, `./conf/historical-datastore.json` will be used.
```
./bin/historical-datastore -conf historical-datastore.json
```

