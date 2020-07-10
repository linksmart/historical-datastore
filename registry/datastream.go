// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package registry implements DataStreamList API
package registry

import (
	"encoding/json"
)

type SourceType string

const (
	Mqtt       = "MQTT"
	Stream = "Stream"
)

// A Datastream describes a stored stream of data
type DataStream struct {
	// Name is the BrokerURL of the DataStreamList API
	Name string `json:"name"`

	//Source of the Data streams
	Source Source `json:"source,omitempty"`

	//Function to be performed on the Data streams
	Function string `json:"function,omitempty"`

	//Type of the data (eg: string, float, bool, data)
	Type StreamType `json:"dataType"`

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

// Source describes a single Data stream such as a sensor (LinkSmart Resource)
type Source struct {
	//type of the source
	//This can be either MQTT or a series element itself
	SrcType SourceType `json:"type,omitempty"`
	*MQTTSource
	*StreamSource
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

type StreamSource struct {
	//name of the stream
	URL string `json:name`
}

func (ds DataStream) copy() DataStream {
	newDS := ds
	newDS.Source = ds.Source
	//copy(newDS.Sources, ds.Sources)
	return newDS
}

// MarshalJSON masks sensitive information when using the default marshaller
func (ds DataStream) MarshalJSON() ([]byte, error) {
	if !ds.keepSensitiveInfo {
		if ds.Source.SrcType == Mqtt {
			// mask MQTT credentials and key paths
			if ds.Source.Username != "" {
				ds.Source.Username = "*****"
			}
			if ds.Source.Password != "" {
				ds.Source.Password = "*****"
			}
			if ds.Source.CaFile != "" {
				ds.Source.CaFile = "*****"
			}
			if ds.Source.CertFile != "" {
				ds.Source.CertFile = "*****"
			}
			if ds.Source.KeyFile != "" {
				ds.Source.KeyFile = "*****"
			}
			if ds.Source.Insecure {
				ds.Source.Insecure = false
			}
		}

	}
	type Alias DataStream
	return json.Marshal((*Alias)(&ds))
}

// MarshalSensitiveJSON serializes the datasource including the sensitive information
func (ds DataStream) MarshalSensitiveJSON() ([]byte, error) {
	ds.keepSensitiveInfo = true
	return json.Marshal(&ds)
}
