Historical Datastore
===================
[![GoDoc](https://godoc.org/github.com/linksmart/historical-datastore?status.svg)](https://godoc.org/github.com/linksmart/historical-datastore)
[![Docker Pulls](https://img.shields.io/docker/pulls/linksmart/hds.svg)](https://hub.docker.com/r/linksmart/hds/tags)
[![GitHub tag (latest pre-release)](https://img.shields.io/github/tag-pre/linksmart/historical-datastore.svg?label=pre-release)](https://github.com/linksmart/historical-datastore/tags)
![Docker Build](https://github.com/linksmart/historical-datastore/workflows/Docker%20Build/badge.svg)

LinkSmart Historical Datastore is a modular service for time-series data storage. It is designed to store timeseries data on low powered devices and single board computers. It uses [Sensor Measurement Lists (SenML)](https://tools.ietf.org/html/rfc8428) as the data format for storage and retrieval. Metadata related to the series is stored in Historical Datastore's registry.

* [Documentation](https://github.com/linksmart/historical-datastore/wiki)


## Running the binary

The binaries can be found in the [release](https://github.com/linksmart/historical-datastore/releases). 

Use -conf flag to set the config file path. If not set, `./conf/historical-datastore.json` will be used.
```
./historical-datastore -conf historical-datastore.json
```

### Docker
`amd64` images are built and available on [Dockerhub](https://hub.docker.com/r/linksmart/hds/tags). To run the latest:
```
docker run -p 8085:8085  linksmart/hds
```

Measurements and the registry data is stored by default in `/data` directory of the container. 
To make it persistent across multiple docker runs, the container directory `/data`  must be mounted to a 
host directory (e.g. `/data/hds` in the host) as shown below:
```
docker run -p 8085:8085 -v /data/hds:/data linksmart/hds
```


Images for other architectures (e.g. `arm`, `arm64`) can be build locally by running:
```
git clone https://github.com/linksmart/historical-datastore.git
cd historical-datastore
docker build -t linksmart/hds .
```

### Demo mode
To run Historical Datastore in demo mode (with continuously growing dummy senml data)
```
docker run -p 8085:8085  linksmart/hds -demo 
```
## Development
The dependencies of this package are managed by [Go Modules](https://github.com/golang/go/wiki/Modules).

### Code structure

The code consists of four packages locate at:

* `/` - implementation of a standalone service providing full API.
* `/registry` - implementation of Registry API
* `/data` - implementation of Data API

### Compile from source
```
git clone https://github.com/linksmart/historical-datastore.git
cd historical-datastore
go build -mod=vendor -o historical-datastore
```

## Contributing
Contributions are welcome. 

Please fork, make your changes, and submit a pull request. For major changes, please open an issue first and discuss it with the other authors.
