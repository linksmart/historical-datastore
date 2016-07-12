// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

// Package registry implements Registry API
package registry

import (
	"fmt"
	"hash/crc32"
	"net/url"
	"sort"
	"strings"
	"time"

	"linksmart.eu/services/historical-datastore/common"
)

// Registry describes a registry of registered Data Sources
type Registry struct {
	// URL is the URL of the Registry API
	URL string `json:"url"`
	// Entries is an array of Data Sources
	Entries []DataSource `json:"entries"`
	// Page is the current page in Entries pagination
	Page int `json:"page"`
	// PerPage is the results per page in Entries pagination
	PerPage int `json:"per_page"`
	// Total is the total #of pages in Entries pagination
	Total int `json:"total"`
}

// DataSource describes a single data source such as a sensor (LinkSmart Resource)
type DataSource struct {
	// ID is a unique ID of the data source
	ID string `json:"id"`
	// URL is the URL of the Data Source in the Registry API
	URL string `json:"url"`
	// Data is the URL to the data of this Data Source Data API
	Data string `json:"data"`
	// Resource is the URL identifying the corresponding
	// LinkSmart Resource (e.g., @id in the Resource Catalog)
	Resource string `json:"resource"`
	// Meta is a hash-map with optional meta-information
	Meta map[string]interface{} `json:"meta"`
	// Retention is the retention duration for data
	Retention string `json:"retention"`
	// Aggregation is an array of configured aggregations
	Aggregation []Aggregation `json:"aggregation"`
	// Type is the values type used in payload
	Type string `json:"type"`
	// Format is the MIME type of the payload
	Format string `json:"format"`
}

func (ds *DataSource) ParsedResource() *url.URL {
	parsedResource, _ := url.Parse(ds.Resource)
	return parsedResource
}

// Aggregation describes a data aggregatoin for a Data Source
type Aggregation struct {
	ID string `json:"id"`
	// Interval is the aggregation interval
	Interval string `json:"interval"`
	// Data is the URL to the data in the Aggregate API
	Data string `json:"data"`
	// Aggregates is an array of aggregates calculated on each interval
	// Valid values: mean, stddev, sum, min, max, median
	Aggregates []string `json:"aggregates"`
	// Retention is the retention duration
	Retention string `json:"retention"`
}

// Generate ID and Data attributes for a given aggregation
// ID is the checksum of aggregation interval and all its aggregates
func (a *Aggregation) Make(dsID string) {
	sort.Strings(a.Aggregates)
	a.ID = fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(a.Interval+strings.Join(a.Aggregates, ""))))
	a.Data = fmt.Sprintf("%s/%s/%s", common.AggrAPILoc, a.ID, dsID)
}

// Storage is an interface of a Registry storage backend
type Storage interface {
	// CRUD
	add(ds DataSource) (DataSource, error)
	update(id string, ds DataSource) (DataSource, error)
	get(id string) (DataSource, error)
	delete(id string) error

	// Utility functions
	getMany(page, perPage int) ([]DataSource, int, error)
	getCount() (int, error)
	modifiedDate() (time.Time, error)

	// Path filtering
	pathFilterOne(path, op, value string) (DataSource, error)
	pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error)
}

// Client is an interface of a Registry client
type Client interface {
	// CRUD
	Add(d DataSource) (DataSource, error)
	Update(id string, d DataSource) (DataSource, error)
	Get(id string) (DataSource, error)
	Delete(id string) error

	// Returns a slice of DataSources given:
	// page - page in the collection
	// perPage - number of entries per page
	GetDataSources(page, perPage int) ([]DataSource, int, error)

	// Returns a single DataSource given: path, operation, value
	FindDataSource(path, op, value string) (*DataSource, error)

	// Returns a slice of DataSources given: path, operation, value, page, perPage
	FindDataSources(path, op, value string, page, perPage int) ([]DataSource, int, error)
}
