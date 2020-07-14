// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"strings"
	"time"
)

const (
	MEMORY  = "memory"
	LEVELDB = "leveldb"
)

// Storage is an interface of a Registry storage backend
type Storage interface {
	// CRUD
	Add(ts TimeSeries) (*TimeSeries, error)
	Update(name string, ts TimeSeries) (*TimeSeries, error)
	Get(name string) (*TimeSeries, error)
	Delete(name string) error
	// Utility functions
	GetMany(page, perPage int) ([]TimeSeries, int, error)
	FilterOne(path, op, value string) (*TimeSeries, error)
	Filter(path, op, value string, page, perPage int) ([]TimeSeries, int, error)
	// needed internally
	getTotal() (int, error)
	getLastModifiedTime() (time.Time, error)
}

// SupportedBackends returns true if the backend is listed as true
func SupportedBackends(name string) bool {
	supportedBackends := map[string]bool{
		MEMORY:  true,
		LEVELDB: true,
	}
	return supportedBackends[strings.ToLower(name)]
}

