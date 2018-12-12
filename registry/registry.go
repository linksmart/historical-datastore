// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package registry implements Registry API
package registry

import (
	"code.linksmart.eu/hds/historical-datastore/common"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/url"
	"sort"
	"strings"
	"time"
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
	keepSensitiveInfo bool
	// ID is a unique ID of the data source
	ID string `json:"id"`
	// URL is the URL of the Data Source in the Registry API
	URL string `json:"url"`
	// Data is the URL to the data of this Data Source Data API
	Data string `json:"data"`
	// Resource URI (i.e., name in SenML)
	Resource string `json:"resource"`
	// Meta is a hash-map with optional meta-information
	Meta map[string]interface{} `json:"meta"`
	// Data connector
	Connector Connector `json:"connector"`
	// Retention is the retention duration for data
	Retention string `json:"retention"`
	// Aggregation is an array of configured aggregations
	Aggregation []Aggregation `json:"aggregation"`
	// Type is the values type used in payload
	Type string `json:"type"`
}

// MarshalJSON masks sensitive information when using the default marshaller
func (ds DataSource) MarshalJSON() ([]byte, error) {

	if !ds.keepSensitiveInfo {
		// mask MQTT credentials and key paths
		if ds.Connector.MQTT != nil {
			if ds.Connector.MQTT.Username != "" {
				ds.Connector.MQTT.Username = "*****"
			}
			if ds.Connector.MQTT.Password != "" {
				ds.Connector.MQTT.Password = "*****"
			}
			if ds.Connector.MQTT.CaFile != "" {
				ds.Connector.MQTT.CaFile = "*****"
			}
			if ds.Connector.MQTT.CertFile != "" {
				ds.Connector.MQTT.CertFile = "*****"
			}
			if ds.Connector.MQTT.KeyFile != "" {
				ds.Connector.MQTT.KeyFile = "*****"
			}
		}
	}

	type Alias DataSource
	return json.Marshal((*Alias)(&ds))
}

// MarshalSensitiveJSON serializes the datasource including the sensitive information
func (ds DataSource) MarshalSensitiveJSON() ([]byte, error) {
	ds.keepSensitiveInfo = true
	return json.Marshal(&ds)
}

// Connector describes additional connectors to the Data API
type Connector struct {
	MQTT *MQTTConf `json:"mqtt,omitempty"`
}

// MQTT describes a MQTT Connector
type MQTTConf struct {
	URL      string `json:"url"`
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	CaFile   string `json:"caFile,omitempty"`
	CertFile string `json:"certFile,omitempty"`
	KeyFile  string `json:"keyFile,omitempty"`
}

func (ds *DataSource) ParsedResource() *url.URL {
	parsedResource, _ := url.Parse(ds.Resource)
	return parsedResource
}

// Aggregation describes a data aggregatoin for a Data Source
type Aggregation struct {
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

// Generate ID and Data attributes for a given aggregation
// ID is the checksum of aggregation interval and all its aggregates
func (a *Aggregation) Make(dsID string) {
	sort.Strings(a.Aggregates)
	a.ID = fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(a.Interval+strings.Join(a.Aggregates, ""))))
	a.Data = fmt.Sprintf("%s/%s/%s", common.AggrAPILoc, a.ID, dsID)
}

// Storage is an interface of a Registry storage backend
type Storage interface {
	// CRUD
	Add(ds DataSource) (DataSource, error)
	Update(id string, ds DataSource) (DataSource, error)
	Get(id string) (DataSource, error)
	Delete(id string) error
	// Utility functions
	GetMany(page, perPage int) ([]DataSource, int, error)
	FilterOne(path, op, value string) (*DataSource, error)
	Filter(path, op, value string, page, perPage int) ([]DataSource, int, error)
	// needed internally
	getTotal() (int, error)
	getLastModifiedTime() (time.Time, error)
}
