// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package data implements Data API
package data

import (
	"time"

	"github.com/farshidtz/senml"
)

// RecordSet describes the recordset returned on querying the Data API
type RecordSet struct {
	// SelfLink is the SelfLink of the returned recordset in the Data API
	SelfLink string `json:"selflink"`
	// Data is a SenML object with data records, where
	// Name (bn and n) constitute the resource BrokerURL of the corresponding Data Sources(s)
	Data senml.Pack `json:"data"`
	// Time is the time of query in milliseconds
	Time float64 `json:"time"`
	//Next link for the same query, in case there more entries to follow for the same query
	NextLink string `json:"nextlink"`
	// Total is the total records in Data pagination
	Total int `json:"total"`
}

type Query struct {
	Start   time.Time //TODO: Change to from and to
	End     time.Time
	Sort    string
	Limit   int
	perPage int
}
