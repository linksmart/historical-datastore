// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"strings"
	"time"

	"github.com/farshidtz/senml"
	"github.com/linksmart/historical-datastore/registry"
)

const (
	INFLUXDB   = "influxdb"
	MONGODB    = "mongodb"
	SENMLSTORE = "senmlstore"
)

// SupportedBackends returns true if the backend is listed as true
func SupportedBackends(name string) bool {
	supportedBackends := map[string]bool{
		INFLUXDB:   false, // Influxdb is not supported from HDS v1.0.0
		MONGODB:    false, // Mongodb is not supported after HDS v0.5.3
		SENMLSTORE: true,
	}
	return supportedBackends[strings.ToLower(name)]
}

// Storage is an interface of a Data storage backend
type Storage interface {
	// Adds data points for multiple data sources
	// data is a map where keys are data source ids
	// sources is a map where keys are data source ids
	Submit(data map[string]senml.Pack) error

	// Queries data for specified data sources
	//Query(q Query, page, perPage int, sources ...*registry.DataSource) (senml.Pack, int, error)
	Query(q Query, sources ...*registry.DataStream) (senml.Pack, int, *time.Time, error)

	// EventListener includes methods for event handling
	registry.EventListener
}
