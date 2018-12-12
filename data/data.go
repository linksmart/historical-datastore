// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package data implements Data API
package data

import (
	"time"

	"github.com/cisco/senml"
)

// RecordSet describes the recordset returned on querying the Data API
type RecordSet struct {
	// URL is the URL of the returned recordset in the Data API
	URL string `json:"url"`
	// Data is a SenML object with data records, where
	// Name (bn and n) constitute the resource URL of the corresponding Data Source(s)
	Data []senml.SenMLRecord `json:"data"`
	// Time is the time of query in milliseconds
	Time float64 `json:"time"`
	// Page is the current page in Data pagination
	Page int `json:"page"`
	// PerPage is the results per page in Data pagination
	PerPage int `json:"per_page"`
	// Total is the total records in Data pagination
	Total int `json:"total"`
}

type Query struct {
	Start time.Time
	End   time.Time
	Sort  string
	Limit int
}
