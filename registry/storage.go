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
	add(ts TimeSeries) (*TimeSeries, error)
	update(name string, ts TimeSeries) (*TimeSeries, error)
	get(name string) (*TimeSeries, error)
	delete(name string) error
	// Utility functions
	getMany(page, perPage int) ([]TimeSeries, int, error)
	filterOne(path, op, value string) (*TimeSeries, error)
	filter(path, op, value string, page, perPage int) ([]TimeSeries, int, error)
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

