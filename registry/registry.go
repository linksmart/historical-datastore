// Package registry implements Registry API
package registry

import "net/url"

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
	// Retention is the retention policy for data
	Retention RetentionPolicy `json:"retention"`
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
	// ID is a unique ID of the aggregated data source
	ID string `json:"id"`
	// Data is the URL to the data in the Aggregate API
	Data string `json:"data"`
	// Source is the id of the parent DataSource
	Source string `json:"source"`
	// Interval is the aggregation interval
	Interval string `json:"interval"`
	// Aggregates is an array of aggregates calculated on each interval
	// Valid values: mean, stddev, sum, min, max, median
	Aggregates []string `json:"aggregates"`
	// Retention is the retention policy
	Retention RetentionPolicy `json:"retention"`
}

// RetentionPolicy describes the retention policy
type RetentionPolicy struct {
	// Policy is the period of time the data will be kept around
	// (at least that long) specified as a decimal number with units, e.g., "1h"
	// Valid time units are "h" (hours), "d" (days), "w" (weeks), and "m" (months)
	Policy string `json:"policy"`
	// Duration is the period of time the data will be kept around
	// after the Policy period (how often the old data will be removed)
	Duration string `json:"duration"`
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
	getCount() int

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

// Validate the DataSource for:
// 	Create:
//	- Not provided: id, url, data
//	- Provided: resource, type, format
//	- Valid: type, retention.policy, retention.duration, aggregates
//	Update:
//	- Not provided: id, url, data, resource, type
//	- Provided: format
//	- Valid: retention.policy, retention.duration, aggregates
///////////////////////////////////////////////////////////////////////////////////////
// TODO refactor the validations based on attributes rather than the type of validation
func validateDataSource(ds *DataSource, context uint8) error {
	var _errors []string

	//// System generated (Read-only) ////////////////////////////////////////////
	var readOnlyKeys []string
	if ds.ID != "" {
		readOnlyKeys = append(readOnlyKeys, "id")
	}
	if ds.URL != "" {
		readOnlyKeys = append(readOnlyKeys, "url")
	}
	if ds.Data != "" {
		readOnlyKeys = append(readOnlyKeys, "data")
	}

	///// Fixed (Read-only once created) /////////////////////////////////////////
	if context == UPDATE {
		if ds.Resource != "" {
			readOnlyKeys = append(readOnlyKeys, "resource")
		}
		if ds.Type != "" {
			readOnlyKeys = append(readOnlyKeys, "type")
		}
	}

	if len(readOnlyKeys) > 0 {
		_errors = append(_errors, "Ambitious assignment to read-only key(s): "+strings.Join(readOnlyKeys, ", "))
	}

	///// Mandatory ///////////////////////////////////////////////////////////////
	var mandatoryKeys []string
	if context == CREATE {
		if ds.Resource == "" {
			mandatoryKeys = append(mandatoryKeys, "resource")
		}
		if ds.Type == "" {
			mandatoryKeys = append(mandatoryKeys, "type")
		}
	}
	if ds.Format == "" {
		mandatoryKeys = append(mandatoryKeys, "format")
	}

	if len(mandatoryKeys) > 0 {
		_errors = append(_errors, "Missing mandatory value(s) of: "+strings.Join(mandatoryKeys, ", "))
	}

	//// Invalid //////////////////////////////////////////////////////////////////
	var invalidKeys []string
	if ds.Resource != "" {
		_, err := url.Parse(ds.Resource)
		if err != nil {
			invalidKeys = append(invalidKeys, "resource")
		}
	}
	if ds.Retention.Policy != "" {
		if !validRetention(ds.Retention.Policy) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("retention.policy<[0-9]*(%s)>", strings.Join(common.RetentionPeriods(), "|")))
		}
	}
	if ds.Retention.Duration != "" {
		if !validRetention(ds.Retention.Duration) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("retention.duration<[0-9]*(%s)>", strings.Join(common.RetentionPeriods(), "|")))
		}
	}
	if ds.Type != "" {
		if !stringInSlice(ds.Type, common.SupportedTypes()) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("type<%s>", strings.Join(common.SupportedTypes(), ",")))
		}
	}
	// Todo: Validate ds.Aggregation
	// common.SupportedAggregates()
	// only if format=float

	if len(invalidKeys) > 0 {
		_errors = append(_errors, "Invalid value(s) for: "+strings.Join(invalidKeys, ", "))
	}

	///// return if any errors ////////////////////////////////////////////////////
	if len(_errors) > 0 {
		return errors.New(strings.Join(_errors, ". "))
	}

	return nil
}

func validRetention(duration string) bool {
	// Create regexp: h|d|w|m
	retPeriods := strings.Join(common.RetentionPeriods(), "|")
	// Create regexp: ^[0-9]*(h|d|w|m)$
	re := regexp.MustCompile("^[0-9]*(" + retPeriods + ")$")

	return re.MatchString(duration)

//	_, err := time.ParseDuration(duration)
//	if err == nil{
//		for _,suffix := range common.RetentionPeriods(){
//			return strings.HasSuffix(duration, suffix)	
//		}
//	}
//	if err != nil{
//		fmt.Println(err.Error())
//	}
//	return false
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
