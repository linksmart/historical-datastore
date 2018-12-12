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

// SupportedBackends returns true if the backend is listed as true
func SupportedBackends(name string) bool {
	supportedBackends := map[string]bool{
		MEMORY:  true,
		LEVELDB: true,
	}
	return supportedBackends[strings.ToLower(name)]
}

// Storage is an interface of a Registry storage backend
type Storage interface {
	// CRUD
	Add(ds DataSource) (DataSource, error)
	Update(id string, ds DataSource) (DataSource, error)
	Get(id string) (DataSource, error)
	Delete(id string) error
	// Utility functions
	GetMany(page, perPage int) ([]DataSource, int, error)
	FilterOne(path, op, value string) (*DataSource, error)
	Filter(path, op, value string, page, perPage int) ([]DataSource, int, error)
	// needed internally
	getTotal() (int, error)
	getLastModifiedTime() (time.Time, error)
}
