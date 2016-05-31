// Package data implements Data API
package data

import (
	"time"

	senml "github.com/krylovsk/gosenml"
	"linksmart.eu/services/historical-datastore/registry"
)

// DataPoint is a data record embedding a SenML Entry
type DataPoint struct {
	*senml.Entry
}

// NewDataPoint returns a DataPoint given an SenML Entry
func NewDataPoint() DataPoint {
	return DataPoint{&senml.Entry{}}
}

// FromEntry returns a DataPoint given an SenML Entry
func (p *DataPoint) FromEntry(e senml.Entry) DataPoint {
	p.Entry = &e
	return *p
}

// DataSet is a set of DataPoints embedding a SenML Message
type DataSet struct {
	*senml.Message
	Entries []DataPoint `json:"e"`
}

// NewDataSet returns a DataSet given an SenML Message
func NewDataSet() DataSet {
	return DataSet{&senml.Message{}, []DataPoint{}}
}

// FromMessage returns a DataSet given a SenML Message
func (s *DataSet) FromMessage(m senml.Message) DataSet {
	s.Message = &m
	return *s
}

// RecordSet describes the recordset returned on querying the Data API
type RecordSet struct {
	// URL is the URL of the returned recordset in the Data API
	URL string `json:"url"`
	// Data is a SenML object with data records, where
	// Name (bn and n) constitute the resource URL of the corresponding Data Source(s)
	Data DataSet `json:"data"`
	// Time is the time of query in milliseconds
	Time float64 `json:"time"`
	// Page is the current page in Data pagination
	Page int `json:"page"`
	// PerPage is the results per page in Data pagination
	PerPage int `json:"per_page"`
	// Total is the total #of pages in Data pagination
	Total int `json:"total"`
}

type Query struct {
	Start time.Time
	End   time.Time
	Sort  string
	Limit int
}

// func (q *query) isValid() bool {
// 	// time
// 	if q.end.Before(q.start) {
// 		return false
// 	}

// 	// sort
// 	validSort := map[string]bool{
// 		ASC:  true,
// 		DESC: true,
// 	}
// 	_, ok := validSort[q.sort]
// 	if !ok {
// 		return false
// 	}
// 	return true
// }

// Storage is an interface of a Data storage backend
type Storage interface {
	// Adds data points for multiple data sources
	// data is a map where keys are data source ids
	// sources is a map where keys are data source ids
	Submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error

	// Queries data for specified data sources
	Query(q Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error)

	// Methods for handling notifications
	NtfCreated(ds registry.DataSource, callback chan error)
	NtfUpdated(old registry.DataSource, new registry.DataSource, callback chan error)
	NtfDeleted(ds registry.DataSource, callback chan error)
}

// Supported content-types for data ingestion
var SupportedContentTypes = map[string]bool{
	"application/senml+json": true,
}
