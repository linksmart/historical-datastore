// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"context"
	"fmt"
	"strings"
	"time"

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
		SENMLSTORE: false, //SenmlStore is not supported after HDS v1.0.0-beta.5
		SQLITE:     true,
	}
	return supportedBackends[strings.ToLower(name)]
}

type sendFunction func(pack senml.Pack) error

// Storage is an interface of a Data storage backend
type Storage interface {
	// Adds data points for multiple time series
	// data is a map where keys are time series ids
	// series is a map where keys are time series ids
	Submit(ctx context.Context, data map[string]senml.Pack, series map[string]*registry.TimeSeries) error

	// Queries data for specified time series
	//QueryPage(q QueryPage, page, PerPage int, series ...*registry.TimeSeries) (senml.Pack, int, error)
	QueryPage(ctx context.Context, q Query, series ...*registry.TimeSeries) (pack senml.Pack, total *int, err error)

	QueryStream(ctx context.Context, q Query, sendFunc sendFunction, series ...*registry.TimeSeries) error

	Count(ctx context.Context, q Query, series ...*registry.TimeSeries) (total int, err error)

	// Delete the data within a given time range
	Delete(ctx context.Context, series []*registry.TimeSeries, from time.Time, to time.Time) (err error)

	// EventListener includes methods for event handling
	registry.EventListener
}

func validateRecordAgainstRegistry(r senml.Record, ts *registry.TimeSeries) error {
	// Check if type of value matches the data value type in registry
	switch ts.Type {
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
