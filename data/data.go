// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package data implements Data API
package data

import (
	"time"

	"github.com/farshidtz/senml/v2"
)

type DenormMask int

const (
	FName DenormMask = 1 << iota
	FTime
	FUnit
	FValue
	FSum
)

// RecordSet describes the recordset returned on querying the Data API
type RecordSet struct {
	// SelfLink is the SelfLink of the returned recordset in the Data API
	SelfLink string `json:"selfLink"`

	// Data is a SenML object with data records, where
	// Name (bn and n) constitute the resource BrokerURL of the corresponding Data streams(s)
	Data senml.Pack `json:"data"`

	// Time is the time of query in seconds
	TimeTook float64 `json:"took"`

	//Next link for the same query, in case there more entries to follow for the same query
	NextLink string `json:"nextLink,omitempty"`

	//Total number of entries
	Count *int `json:"count,omitempty"`
}

type Query struct {
	From        time.Time
	To          time.Time
	Sort        string
	Page        int
	Limit       int
	PerPage     int
	Denormalize DenormMask
	count       bool
}
