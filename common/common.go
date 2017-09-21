// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package common

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
)

const (
	// IDSeparator is used for separation of IDs in the URL
	IDSeparator = ","
	// APIVersion defines the API version
	APIVersion = "0.3.3"
	// Default MIME type for all responses
	DefaultMIMEType = "application/vnd.eu.linksmart.hds+json;version=" + APIVersion

	// Location of APIs
	RegistryAPILoc = "/registry"
	DataAPILoc     = "/data"
	AggrAPILoc     = "/aggr"

	// Query parameters
	ParamPage    = "page"
	ParamPerPage = "per_page"
	ParamLimit   = "limit"
	ParamStart   = "start"
	ParamEnd     = "end"
	ParamSort    = "sort"
	// Values for ParamSort
	ASC  = "asc"  // ascending
	DESC = "desc" // descending
)

// Data source types
const (
	STRING = "string"
	FLOAT  = "float"
	BOOL   = "bool"
)

var (
	supportedTypes      = []string{STRING, BOOL, FLOAT}
	supportedAggregates = []string{"mean", "stddev", "sum", "min", "max", "median"}
	supportedIntervals  = []string{"m", "h", "d", "w"}
)

// Error describes an API error (serializable in JSON)
type Error struct {
	// Code is the (http) code of the error
	Code int `json:"code"`
	// Message is the (human-readable) error message
	Message string `json:"message"`
}

// ErrorResponse writes error to HTTP ResponseWriter
func ErrorResponse(code int, msg string, w http.ResponseWriter) {
	e := &Error{
		code,
		msg,
	}
	logger.DebugOutput(2, "Error: "+msg)
	b, _ := json.Marshal(e)
	w.Header().Set("Content-Type", "application/json;version="+APIVersion)
	w.WriteHeader(code)
	w.Write(b)
}

func SupportedInterval(i string) bool {
	// Create regexp: ^[0-9]*(h|d|w|m)$
	intervals := strings.Join(supportedIntervals, "|")
	re := regexp.MustCompile("^[0-9]*(" + intervals + ")$")
	return re.MatchString(i)
}
func SupportedType(t string) bool {
	return stringInSlice(t, supportedTypes)
}
func SupportedAggregate(a string) bool {
	return stringInSlice(a, supportedAggregates)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
