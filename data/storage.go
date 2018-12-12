// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/cisco/senml"
	"strings"
)

const (
	INFLUXDB = "influxdb"
	MONGODB  = "mongodb"
)

// SupportedBackends returns true if the backend is listed as true
func SupportedBackends(name string) bool {
	supportedBackends := map[string]bool{
		INFLUXDB: true,
		MONGODB:  false, // Mongodb is not supported after HDS v0.5.3
	}
	return supportedBackends[strings.ToLower(name)]
}

// Storage is an interface of a Data storage backend
type Storage interface {
	// Adds data points for multiple data sources
	// data is a map where keys are data source ids
	// sources is a map where keys are data source ids
	Submit(data map[string][]senml.SenMLRecord, sources map[string]*registry.DataSource) error

	// Queries data for specified data sources
	Query(q Query, page, perPage int, sources ...*registry.DataSource) (senml.SenML, int, error)

	// EventListener includes methods for event handling
	registry.EventListener
}
