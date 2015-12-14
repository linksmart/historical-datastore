package common

import (
	"encoding/json"
	"net/http"
)

const (
	// IDSeparator is used for separation of IDs in the URL
	IDSeparator = ","
	// APIVersion defines the API version
	APIVersion = "0.1.0"
	// Default MIME type for all responses
	DefaultMIMEType = "application/vnd.eu.linksmart.hds+json;version=" + APIVersion

	// Location of APIs
	RegistryAPILoc = "/registry"
	DataAPILoc     = "/data"
	AggrAPILoc     = "/aggr"

	// Pagination parameters
	GetParamPage    = "page"
	GetParamPerPage = "per_page"
	// Max DataSources displayed in each page of registry
	MaxPerPage = 100
)

var (
	supportedTypes      = []string{"string", "bool", "float"}
	supportedAggregates = []string{"mean", "stddev", "sum", "min", "max", "median"}
	retentionPeriods    = []string{"m", "h", "d", "w"}
)

func SupportedTypes() []string {
	return supportedTypes
}
func SupportedAggregates() []string {
	return supportedAggregates
}
func RetentionPeriods() []string {
	return retentionPeriods
}

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
	b, _ := json.Marshal(e)
	w.Header().Set("Content-Type", "application/json;version="+APIVersion)
	w.WriteHeader(code)
	w.Write(b)
}

func ValidatePagingParams(page, perPage, maxPerPage int) (int, int) {
	// use defaults if not specified
	if page == 0 {
		page = 1
	}
	if perPage == 0 || perPage > maxPerPage {
		perPage = maxPerPage
	}

	return page, perPage
}
