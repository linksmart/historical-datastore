// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package registry implements TimeSeriesList API
package registry

import (
	"encoding/json"
)

type SourceType string

const (
	Mqtt   = "MQTT"
	Series = "Series"
)

// A TimeSeries describes a stored stream of data
type TimeSeries struct {
	// Name is the BrokerURL of the Registry API
	Name string `json:"name"`

	//Source of the time series
	Source Source `json:"source,omitempty"`

	//Function to be performed on the time series
	Function string `json:"function,omitempty"`

	//Type of the data (eg: string, float, bool, data)
	Type ValueType `json:"dataType"`

	//Unit of the data
	Unit string `json:"unit,omitempty"`

	// Meta is a hash-map with optional meta-information
	Meta map[string]interface{} `json:"meta"`

	// Retention
	Retention struct {
		//minimum requirement for the retention
		Min string `json:"min,omitempty"`
		//maximum requirement for the retention. This is useful for enforcing the data privacy
		Max string `json:"max,omitempty"`
	} `json:"retain,omitempty"`
	// DynamicChild TODO
	keepSensitiveInfo bool
}

// Source describes a single time series such as a sensor (LinkSmart Resource)
type Source struct {
	//type of the source
	//This can be either MQTT or a series element itself
	SrcType SourceType `json:"type,omitempty"`
	*MQTTSource
	*SeriesSource
}

type MQTTSource struct {
	//complete BrokerURL including protocols
	BrokerURL string `json:"url"`
	//Topic to subscribe for the datasource
	Topic string `json:"topic"`
	//QoS of subscription
	QoS      byte   `json:"qos,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	CaFile   string `json:"caFile,omitempty"`
	CertFile string `json:"certFile,omitempty"`
	KeyFile  string `json:"keyFile,omitempty"`
	Insecure bool   `json:"insecure,omitempty"`
	//Avoid marshalling sensitive informations

}

type SeriesSource struct {
	//name of the time series
	URL string `json:name`
}

func (ts TimeSeries) copy() TimeSeries {
	newTS := ts
	newTS.Source = ts.Source
	//copy(newTS.Sources, ts.Sources)
	return newTS
}

// MarshalJSON masks sensitive information when using the default marshaller
func (ts TimeSeries) MarshalJSON() ([]byte, error) {
	if !ts.keepSensitiveInfo {
		if ts.Source.SrcType == Mqtt {
			// mask MQTT credentials and key paths
			if ts.Source.Username != "" {
				ts.Source.Username = "*****"
			}
			if ts.Source.Password != "" {
				ts.Source.Password = "*****"
			}
			if ts.Source.CaFile != "" {
				ts.Source.CaFile = "*****"
			}
			if ts.Source.CertFile != "" {
				ts.Source.CertFile = "*****"
			}
			if ts.Source.KeyFile != "" {
				ts.Source.KeyFile = "*****"
			}
			if ts.Source.Insecure {
				ts.Source.Insecure = false
			}
		}

	}
	type Alias TimeSeries
	return json.Marshal((*Alias)(&ts))
}

// MarshalSensitiveJSON serializes the datasource including the sensitive information
func (ts TimeSeries) MarshalSensitiveJSON() ([]byte, error) {
	ts.keepSensitiveInfo = true
	return json.Marshal(&ts)
}
