package common

import (
	"encoding/json"
	"math"
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
	// ASC stands for ascending
	ASC = "asc"
	// DESC stands for descending
	DESC = "desc"
)

const (
	STRING = "string"
	FLOAT  = "float"
	BOOL   = "bool"
)

var (
	supportedTypes      = []string{STRING, BOOL, FLOAT}
	supportedAggregates = []string{"mean", "stddev", "sum", "min", "max", "median"}
	retentionPeriods    = []string{"m", "h", "d", "w"}
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

// Calculate perItem and offset given the page, perPage, limit, and number of sources
func PerItemPagination(_qLimit, _page, _perPage, _numOfSrcs int) ([]int, []int) {
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

func RetentionPeriods() []string {
	return retentionPeriods
}
func SupportedTypes() []string {
	return supportedTypes
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
