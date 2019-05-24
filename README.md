Historical Datastore
===================
[![GoDoc](https://godoc.org/github.com/linksmart/historical-datastore?status.svg)](https://godoc.org/github.com/linksmart/historical-datastore)
[![Docker Pulls](https://img.shields.io/docker/pulls/linksmart/historical-datastore.svg)](https://hub.docker.com/r/linksmart/historical-datastore/tags)
[![GitHub tag (latest pre-release)](https://img.shields.io/github/tag-pre/linksmart/historical-datastore.svg)](https://github.com/linksmart/historical-datastore/tags)
[![Build Status](https://travis-ci.com/linksmart/historical-datastore.svg?branch=master)](https://travis-ci.com/linksmart/historical-datastore)

Implementation of the [Historical Datastore Service](https://docs.linksmart.eu/display/HDS).

## Code structure

The code consists of four packages locate at:

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

## Development
The dependencies of this package are managed by [dep](https://github.com/golang/dep).
