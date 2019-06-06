Historical Datastore
===================
[![GoDoc](https://godoc.org/github.com/linksmart/historical-datastore?status.svg)](https://godoc.org/github.com/linksmart/historical-datastore)
[![Docker Pulls](https://img.shields.io/docker/pulls/linksmart/hds.svg)](https://hub.docker.com/r/linksmart/hds/tags)
[![GitHub tag (latest pre-release)](https://img.shields.io/github/tag-pre/linksmart/historical-datastore.svg?label=pre-release)](https://github.com/linksmart/historical-datastore/tags)
[![Build Status](https://travis-ci.com/linksmart/historical-datastore.svg?branch=master)](https://travis-ci.com/linksmart/historical-datastore)

Implementation of the [Historical Datastore Service](https://docs.linksmart.eu/display/HDS).

## Code structure

The code consists of four packages locate at:

* `/` - implementation of a standalone service providing full API.
* `/registry` - implementation of Registry API
* `/data` - implementation of Data API


## Compile from source

```
git https://github.com/linksmart/historical-datastore.git
cd historical-datastore
go build -mod=vendor -o historical-datastore
```


## Run
Use -conf flag to set the config file path. If not set, `./conf/historical-datastore.json` will be used.
```
historical-datastore -conf historical-datastore.json
```

## Development
The dependencies of this package are managed by [mod](https://github.com/golang/go/wiki/Modules).
