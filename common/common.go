// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package common

import (
	"regexp"
	"strings"
)

const (
	// IDSeparator is used for separation of IDs in the BrokerURL
	IDSeparator = ","

	// Location of APIs
	RegistryAPILoc = "/registry"
	DataAPILoc     = "/data"
	// Query parameters
	ParamPage        = "page"
	ParamPerPage     = "perPage"
	ParamLimit       = "limit"
	ParamFrom        = "from"
	ParamTo          = "to"
	ParamSort        = "sort"
	ParamDenormalize = "denormalize"
	ParamCount       = "count"

	// Values for ParamSort
	Asc  = "asc"  // ascending
	Desc = "desc" // descending
)

var (
	// APIVersion defines the API version
	APIVersion string

	// Default MIME type for all responses
	DefaultMIMEType string

	// supported aggregates
	supportedAggregates = []string{"mean", "stddev", "sum", "min", "max", "median"}
	// supported period suffixes
	supportedPeriods = []string{"m", "h", "d", "w"}
)

func SetVersion(version string) {
	APIVersion = version
	DefaultMIMEType = "application/json"
	if version != "" {
		DefaultMIMEType += ";version=" + version
	}
}

// SupportedPeriod validates a period
func SupportedPeriod(p string) bool {
	if p == "" {
		// empty means no retention
		return true
	}
	// Create regexp: ^[0-9]*(h|d|w|m)$
	intervals := strings.Join(supportedPeriods, "|")
	re := regexp.MustCompile("^[0-9]*(" + intervals + ")$")
	return re.MatchString(p)
}

// SupportedPeriods returns supported periods
func SupportedPeriods() []string {
	var periods []string
	copy(periods, supportedPeriods)
	return periods
}

// SupportedAggregate validates an aggregate
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
