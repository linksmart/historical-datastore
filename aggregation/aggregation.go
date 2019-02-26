// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package aggregation implements Aggregation API
package aggregation

import (
	"encoding/json"
)

type Index struct {
	Aggrs []Aggregation `json:"aggregations"`
}

type Aggregation struct {
	ID         string   `json:"id"`
	Interval   string   `json:"interval"`
	Aggregates []string `json:"aggregates"`
	Retention  string   `json:"retention"`
	Sources    []string `json:"sources"`
}

// RecordSet describes the recordset returned on querying the Aggr API
type RecordSet struct {
	// URL is the URL of the returned recordset in the Data API
	URL string `json:"url"`
	// Data is a SenML object with data records, where
	// Name (bn and n) constitute the resource URL of the corresponding Data Sources(s)
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

type DataSet struct {
	BaseName      string      `json:"bn,omitempty"`
	BaseTimeStart int64       `json:"bts,omitempty"`
	BaseTimeEnd   int64       `json:"bte,omitempty"`
	Entries       []DataEntry `json:"e"`
}

type DataEntry struct {
	Name       string
	TimeStart  int64
	TimeEnd    int64
	Aggregates map[string]float64
}

func (e *DataEntry) MarshalJSON() ([]byte, error) {
	x := make(map[string]interface{})

	if e.Name != "" {
		x["n"] = e.Name
	}
	if e.TimeStart != 0 {
		x["ts"] = e.TimeStart
	}
	if e.TimeEnd != 0 {
		x["te"] = e.TimeEnd
	}
	for k, v := range e.Aggregates {
		x[k] = v
	}
	return json.Marshal(&x)
}

func NewDataEntry() DataEntry {
	return DataEntry{Aggregates: make(map[string]float64)}
}
