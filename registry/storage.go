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

// Storage is an interface of a DataStreamList storage backend
type Storage interface {
	// CRUD
	Add(ds DataStream) (*DataStream, error)
	Update(name string, ds DataStream) (*DataStream, error)
	Get(name string) (*DataStream, error)
	Delete(name string) error
	// Utility functions
	GetMany(page, perPage int) ([]DataStream, int, error)
	FilterOne(path, op, value string) (*DataStream, error)
	Filter(path, op, value string, page, perPage int) ([]DataStream, int, error)
	// needed internally
	getTotal() (int, error)
	getLastModifiedTime() (time.Time, error)
}
