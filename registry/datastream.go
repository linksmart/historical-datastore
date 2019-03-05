// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

// Package registry implements DataStreamList API
package registry

import (
	"encoding/json"
)

type SourceType string

const (
	MqttType   = "MQTT"
	SeriesType = "Series"
)

// A Datastream describes a stored stream of data
type DataStream struct {
	// URL is the URL of the DataStreamList API
	Name string `json:"name"`
	// Entries is an array of Data Sources
	Source Source `json:"source"`
	// function to be performed on the data sources
	Function string `json:"function,omitempty"`
	//Type of the data (eg: string, float, bool, data)
	Type string `json:"datatype"`
	// Retain
	Retention struct {
		//minimum requirement for the retention
		Min string `json:"min"`
		//maximum requirement for the retention. This is useful for enforcing the data privacy
		Max string `json:"max",omitempty`
	} `json:"retain,omitempty"`
	// DynamicChild TODO
	keepSensitiveInfo bool
}

// DataSource describes a single data source such as a sensor (LinkSmart Resource)
type Source struct {
	//type of the source
	//This can be either MQTT or a series element itself
	SrcType SourceType `json:"type"`
	*MQTTSource
	*SeriesSource
}

type MQTTSource struct {
	Source
	//complete URL including protocols
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
	//Avoid marshalling sensitive informations

}

type SeriesSource struct {
	//name of the series
	URL string `json:name`
}

func (ds DataStream) copy() DataStream {
	newDS := ds
	newDS.Source = ds.Source
	//copy(newDS.Sources, ds.Sources)
	return newDS
}

// MarshalJSON masks sensitive information when using the default marshaller
func (dataStream DataStream) MarshalJSON() ([]byte, error) {
	//TODO test this for : 1. Original DS doesnt change, 2. the json has everything
	newDataSteam := dataStream
	if newDataSteam.keepSensitiveInfo == true {
		source := newDataSteam.Source
		if source.SrcType == MqttType {
			mqttSource := &source
			// mask MQTT credentials and key paths
			if mqttSource.Username != "" {
				mqttSource.Username = "*****"
			}
			if mqttSource.Password != "" {
				mqttSource.Password = "*****"
			}
			if mqttSource.CaFile != "" {
				mqttSource.CaFile = "*****"
			}
			if mqttSource.CertFile != "" {
				mqttSource.CertFile = "*****"
			}
			if mqttSource.KeyFile != "" {
				mqttSource.KeyFile = "*****"
			}

		}

	}
	type Alias DataStream
	return json.Marshal((*Alias)(&newDataSteam))
}

// MarshalSensitiveJSON serializes the datasource including the sensitive information
func (ds DataStream) MarshalSensitiveJSON() ([]byte, error) {
	ds.keepSensitiveInfo = true
	return json.Marshal(&ds)
}
