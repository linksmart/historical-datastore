// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package data implements Data API
package data

import (
	"time"

	"github.com/farshidtz/senml/v2"
)

//Specifying which field needs to be denormalized
type DenormMask int32

const (
	//name field
	FName DenormMask = 1 << iota
	//time field
	FTime
	//unit field
	FUnit
	//Value field
	FValue
	//sum field
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
	//Time from which the data needs to be fetched
	From time.Time
	//Time to which the data needs to be fetched
	To time.Time
	//Sort either ASC or DESC
	SortAsc     bool
	Page        int
	PerPage     int
	Denormalize DenormMask
	count       bool
}
