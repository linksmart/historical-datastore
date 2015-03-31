// Package registry implements Registry API
package registry

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
	// Retention is the retention policy for data
	Retention RetentionPolicy `json:"retention"`
	// Aggregation is an array of configured aggregations
	Aggregation []AggregatedDataSource `json:"aggregation"`
	// Type is the values type used in payload
	Type string `json:"type"`
	// Format is the MIME type of the payload
	Format string `json:"format"`
}

// AggregatedDataSource describes a data aggregatoin for a Data Source
type AggregatedDataSource struct {
	// ID is a unique ID of the aggregated data source
	ID string `json:"id"`
	// Data is the URL to the data in the Aggregate API
	Data string `json:"data"`
	// Source is the id of the parent DataSource
	Source string `json:"source"`
	// Interval is the aggregation interval
	Interval string `json:"interval"`
	// Aggregates is an array of aggregates calculated on each interval
	// Valid values: mean, stddev, sum, min, max, median
	Aggregates []string `json:"aggregates"`
	// Retention is the retention policy
	Retention RetentionPolicy `json:"retention"`
}

// RetentionPolicy describes the retention policy
type RetentionPolicy struct {
	// Policy is the period of time the data will be kept around
	// (at least that long) specified as a decimal number with units, e.g., "1h"
	// Valid time units are "h" (hours), "d" (days), "w" (weeks), and "m" (months)
	Policy string `json:"policy"`
	// Duration is the period of time the data will be kept around
	// after the Policy period (how often the old data will be removed)
	Duration string `json:"duration"`
}

// Storage is an interface of a Registry storage backend
type Storage interface {
	// CRUD
	add(ds DataSource) error
	update(id string, ds DataSource) error
	delete(id string) error
	get(id string) (DataSource, error)

	// Utility functions
	getMany(page, perPage int) ([]DataSource, int, error)
	getCount() int

	// Path filtering
	pathFilterOne(path, op, value string) (DataSource, error)
	pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error)
}
