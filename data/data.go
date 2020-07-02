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
	Count *int `json:"Count,omitempty"`
}

type Query struct {
	// From is the Time from which the data needs to be fetched
	From time.Time
	// Time to which the data needs to be fetched
	To time.Time

	// SortAsc if set to true, oldest measurements are listed first in the resulting pack. If set to false, latest entries are listed first.
	SortAsc bool

	// PerPag: in case of paginated query, number of measurements returned as part of the query. In case of streamed query, number of measurements per pack in the stream
	PerPage int

	// Denormalize is a set of flags to be set based on the fields to be denormalized (Base field)
	Denormalize DenormMask

	// Count: if enabled, it will return the total number of entries to the query
	Count bool

	// Limit Applicable only for streamed queries
	Limit int

	// Page is applicable only for paginated queries
	Page int
}
