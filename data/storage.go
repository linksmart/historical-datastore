// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"fmt"
	"strings"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/registry"
)

const (
	INFLUXDB       = "influxdb"
	MONGODB        = "mongodb"
	SENMLSTORE     = "senmlstore"
	SQLITE         = "sqlite"
	DRIVER_SQLITE3 = "sqlite3"
)

// SupportedBackends returns true if the backend is listed as true
func SupportedBackends(name string) bool {
	supportedBackends := map[string]bool{
		INFLUXDB:   false, // Influxdb is not supported from HDS v1.0.0
		MONGODB:    false, // Mongodb is not supported after HDS v0.5.3
		SENMLSTORE: false,
		SQLITE:     true,
	}
	return supportedBackends[strings.ToLower(name)]
}

// Storage is an interface of a Data storage backend
type Storage interface {
	// Adds data points for multiple data sources
	// data is a map where keys are data source ids
	// sources is a map where keys are data source ids
	Submit(data map[string]senml.Pack, sources map[string]*registry.DataStream) error

	// Queries data for specified data sources
	//Query(q Query, page, PerPage int, sources ...*registry.DataSource) (senml.Pack, int, error)
	Query(q Query, sources ...*registry.DataStream) (pack senml.Pack, total *int, err error)

	// EventListener includes methods for event handling
	registry.EventListener
}

func validateRecordAgainstRegistry(r senml.Record, ds *registry.DataStream) error {
	// Check if type of value matches the data source type in registry
	switch ds.Type {
	case registry.Float:
		if r.Value == nil {
			return fmt.Errorf("missing value for %s", r.Name)
		}
	case registry.String:
		if r.StringValue == "" {
			return fmt.Errorf("missing String Value for %s", r.Name)
		}
	case registry.Bool:
		if r.BoolValue == nil {
			return fmt.Errorf("missing Boolean Value for %s", r.Name)
		}
	case registry.Data:
		if r.DataValue == "" {
			return fmt.Errorf("missing Data Value for %s", r.Name)
		}
	}

	return nil
}
