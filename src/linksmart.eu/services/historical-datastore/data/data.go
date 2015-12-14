// Package data implements Data API
package data

import (
	"math"
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
	// Error is the query error (or null)
	Error string `json:"error"`
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

	// Retrieves last data point of every data source
	GetLast(sources ...registry.DataSource) (DataSet, error)
	// Queries data for specified data sources
	Query(q Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error)

	// Methods for handling notifications
	ntfCreated(ds registry.DataSource, callback chan error)
	ntfUpdated(old registry.DataSource, new registry.DataSource, callback chan error)
	ntfDeleted(ds registry.DataSource, callback chan error)
}

// Calculate perItem and offset given the page, perPage, limit, and number of sources
func perItemPagination(_qLimit, _page, _perPage, _numOfSrcs int) ([]int, []int) {
	qLimit, page, perPage, numOfSrcs := float64(_qLimit), float64(_page), float64(_perPage), float64(_numOfSrcs)
	limitIsSet := qLimit > 0
	page-- // make page number 0-indexed

	// Make qLimit and perPage divisible by the number of sources
	if limitIsSet && math.Remainder(qLimit, numOfSrcs) != 0 {
		qLimit = math.Floor(qLimit/numOfSrcs) * numOfSrcs
	}
	if math.Remainder(perPage, numOfSrcs) != 0 {
		perPage = math.Floor(perPage/numOfSrcs) * numOfSrcs
	}

	//// Get equal number of entries for each item
	// Set limit to the smallest of qLimit and perPage (adapts to each page)
	limit := perPage
	if limitIsSet && qLimit-page*perPage < perPage {
		limit = qLimit - page*perPage
		if limit < 0 { // blank page
			limit = 0
		}
	}
	perItem := math.Ceil(limit / numOfSrcs)
	perItems := make([]int, _numOfSrcs)
	for i := range perItems {
		perItems[i] = int(perItem)
	}
	perItems[_numOfSrcs-1] += int(limit - numOfSrcs*perItem) // add padding to the last item

	//// Calculate offset for items
	// Set limit to the smallest of qLimit and perPage (regardless of current page number)
	Limit := perPage
	if limitIsSet && qLimit < perPage {
		Limit = qLimit
	}
	offset := page * math.Ceil(Limit/numOfSrcs)
	offsets := make([]int, _numOfSrcs)
	for i := range offsets {
		offsets[i] = int(offset)
	}
	offsets[_numOfSrcs-1] += int(page * (limit - numOfSrcs*perItem)) // add padding to the last offset

	return perItems, offsets
}
