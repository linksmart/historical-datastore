// Package data implements Data API
package data

import (
	"time"

	senml "github.com/krylovsk/gosenml"
	"linksmart.eu/services/historical-datastore/registry"
)

const (
	// ASC stands for ascending
	ASC = "asc"
	// DESC stands for descending
	DESC = "desc"
)

// DataPoint is a data record embedding a SenML Entry
type DataPoint struct {
	*senml.Entry
}

// DataSet is a set of DataPoints embedding a SenML Message
type DataSet struct {
	*senml.Message
}

// NewDataPoint returns a DataPoint given an SenML Entry
func NewDataPoint(e senml.Entry) DataPoint {
	return DataPoint{&e}
}

// NewDataSet returns a DataSet given an SenML Message
func NewDataSet(m senml.Message) DataSet {
	return DataSet{&m}
}

// RecordSet describes the recordset returned on querying the Data API
type RecordSet struct {
	// URL is the URL of the returned recordset in the Data API
	URL string `json:"url"`
	// Data is a SenML object with data records, where
	// Name (bn and n) constitute the resource URL of the corresponding Data Source(s)
	Data DataSet `json:"data"`
	// Time is the time of query in milliseconds
	Time int `json:"time"`
	// Error is the query error (or null)
	Error string `json:"error"`
	// Page is the current page in Data pagination
	Page int `json:"page"`
	// PerPage is the results per page in Data pagination
	PerPage int `json:"per_page"`
	// Total is the total #of pages in Data pagination
	Total int `json:"total"`
}

type query struct {
	start                time.Time
	end                  time.Time
	sort                 string
	limit, page, perPage int
}

// Storage is an interface of a Data storage backend
type Storage interface {
	// Adds data points for multiple data sources
	// data is a map where keys are data source ids
	// sources is a map where keys are data source ids
	submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error

	// Retrieves last data point of every data source
	getLast(ds ...registry.DataSource) (int, DataSet, error)
	// Queries data for specified data sources
	query(q query, ds ...registry.DataSource) (int, DataSet, error)
}
