// Package registry implements Registry API
package registry

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	//"time"

	"linksmart.eu/services/historical-datastore/common"
)

// Registry describes a registry of registered Data Sources
type Registry struct {
	// URL is the URL of the Registry API
	URL string `json:"url"`
	// Entries is an array of Data Sources
	Entries []DataSource `json:"entries"`
	// Page is the current page in Entries pagination
	Page int `json:"page"`
	// PerPage is the results per page in Entries pagination
	PerPage int `json:"per_page"`
	// Total is the total #of pages in Entries pagination
	Total int `json:"total"`
}

// DataSource describes a single data source such as a sensor (LinkSmart Resource)
type DataSource struct {
	// ID is a unique ID of the data source
	ID string `json:"id"`
	// URL is the URL of the Data Source in the Registry API
	URL string `json:"url"`
	// Data is the URL to the data of this Data Source Data API
	Data string `json:"data"`
	// Resource is the URL identifying the corresponding
	// LinkSmart Resource (e.g., @id in the Resource Catalog)
	Resource string `json:"resource"`
	// Meta is a hash-map with optional meta-information
	Meta map[string]interface{} `json:"meta"`
	// Retention is the retention duration for data
	Retention string `json:"retention"`
	// Aggregation is an array of configured aggregations
	Aggregation []AggregatedDataSource `json:"aggregation"`
	// Type is the values type used in payload
	Type string `json:"type"`
	// Format is the MIME type of the payload
	Format string `json:"format"`
}

func (ds *DataSource) ParsedResource() *url.URL {
	parsedResource, _ := url.Parse(ds.Resource)
	return parsedResource
}

// AggregatedDataSource describes a data aggregatoin for a Data Source
type AggregatedDataSource struct {
	ID string `json:"id"`
	// Interval is the aggregation interval
	Interval string `json:"interval"`
	// Data is the URL to the data in the Aggregate API
	Data string `json:"data"`
	// Aggregates is an array of aggregates calculated on each interval
	// Valid values: mean, stddev, sum, min, max, median
	Aggregates []string `json:"aggregates"`
	// Retention is the retention duration
	Retention string `json:"retention"`
}

// Storage is an interface of a Registry storage backend
type Storage interface {
	// CRUD
	add(ds DataSource) (DataSource, error)
	update(id string, ds DataSource) (DataSource, error)
	get(id string) (DataSource, error)
	delete(id string) error

	// Utility functions
	getMany(page, perPage int) ([]DataSource, int, error)
	getCount() (int, error)

	// Path filtering
	pathFilterOne(path, op, value string) (DataSource, error)
	pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error)
}

// Client is an interface of a Registry client
type Client interface {
	// CRUD
	Add(d DataSource) (DataSource, error)
	Update(id string, d DataSource) (DataSource, error)
	Get(id string) (DataSource, error)
	Delete(id string) error

	// Returns a slice of DataSources given:
	// page - page in the collection
	// perPage - number of entries per page
	GetDataSources(page, perPage int) ([]DataSource, int, error)

	// Returns a single DataSource given: path, operation, value
	FindDataSource(path, op, value string) (*DataSource, error)

	// Returns a slice of DataSources given: path, operation, value, page, perPage
	FindDataSources(path, op, value string, page, perPage int) ([]DataSource, int, error)
}

// Validation /////////////////////////////////////////////////////////////////////

const (
	CREATE uint8 = iota
	UPDATE
)

// Validate the DataSource items:
// id: r-o
// url: r-o
// data: r-o
// resource: mandatory, fixed, valid
// meta: n/a
// retention: valid
// aggregation: tba
// type: mandatory, fixed, valid
// format: mandatory
func validateDataSource(ds *DataSource, context uint8) error {
	var _errors []string
	var readOnlyKeys, mandatoryKeys, invalidKeys []string

	// VALIDATE ID `json:"id"` /////////////////////////////////////////////////////
	// system-generated (read-only)
	if ds.ID != "" {
		readOnlyKeys = append(readOnlyKeys, "id")
	}

	// VALIDATE URL `json:"url"` //////////////////////////////////////////////////////
	// system-generated (read-only)
	if ds.URL != "" {
		readOnlyKeys = append(readOnlyKeys, "url")
	}

	// VALIDATE DATA `json:"data"` /////////////////////////////////////////////////////
	// system-generated (read-only)
	if ds.Data != "" {
		readOnlyKeys = append(readOnlyKeys, "data")
	}

	// VALIDATE RESOURCE `json:"resource"` /////////////////////////////////////////////////
	// mandatory for creation
	if ds.Resource == "" && context == CREATE {
		mandatoryKeys = append(mandatoryKeys, "resource")
	}
	// fixed (read-only once created)
	if ds.Resource != "" && context == UPDATE {
		readOnlyKeys = append(readOnlyKeys, "resource")
	}
	// valid
	if ds.Resource != "" {
		_, err := url.Parse(ds.Resource)
		if err != nil {
			invalidKeys = append(invalidKeys, "resource")
		}
	}

	// VALIDATE META `json:"meta"` /////////////////////////////////////////////////////

	// VALIDATE RETENTION `json:"retention"` ////////////////////////////////////////////////
	// valid
	if ds.Retention != "" {
		// Create regexp: ^[0-9]*(h|d|w|m)$
		retPeriods := strings.Join(common.RetentionPeriods(), "|")
		re := regexp.MustCompile("^[0-9]*(" + retPeriods + ")$")
		if !re.MatchString(ds.Retention) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("retention %s", re.String()))
		}
	}

	// VALIDATE `json:"aggregation"` //////////////////////////////////////////////
	// Todo: Validate ds.Aggregation
	// common.SupportedAggregates()
	// only if format=float

	// VALIDATE TYPE `json:"type"` /////////////////////////////////////////////////////
	// mandatory for creation
	if ds.Type == "" && context == CREATE {
		mandatoryKeys = append(mandatoryKeys, "type")
	}
	// fixed (read-only once created)
	if ds.Type != "" && context == UPDATE {
		readOnlyKeys = append(readOnlyKeys, "type")
	}
	// valid
	if ds.Type != "" {
		if !common.SupportedType(ds.Type) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("type<%s>",
				strings.Join(common.SupportedTypes(), ",")))
		}
	}

	// VALIDATE FORMAT `json:"format"` ///////////////////////////////////////////////////
	// mandatory for creation
	if ds.Format == "" && context == CREATE {
		mandatoryKeys = append(mandatoryKeys, "format")
	}

	///////////////////////////////////////////////////////////////////////////////
	if len(readOnlyKeys) > 0 {
		_errors = append(_errors, "Ambitious assignment to read-only attribute(s): "+
			strings.Join(readOnlyKeys, ", "))
	}
	if len(mandatoryKeys) > 0 {
		_errors = append(_errors, "Missing mandatory value(s) of: "+
			strings.Join(mandatoryKeys, ", "))
	}
	if len(invalidKeys) > 0 {
		_errors = append(_errors, "Invalid value(s) for: "+
			strings.Join(invalidKeys, ", "))
	}

	///// return if any errors
	if len(_errors) > 0 {
		return errors.New(strings.Join(_errors, ". "))
	}
	return nil
}
