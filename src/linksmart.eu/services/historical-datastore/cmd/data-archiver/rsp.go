// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

package main

import (
	"errors"
	"fmt"

	"mime"

	"linksmart.eu/lc/core/catalog/resource"
	"linksmart.eu/services/historical-datastore/data"
	"strings"
)

var notMQTTPublisher = errors.New("Not MQTT Publisher")

// ResourcesProvider interface is implemented by data sources "backends" such as RC or MR
type ResourcesProvider interface {
	GetAll() ([]resource.Resource, error)
}

type ResourceMQTTEndpoint struct {
	Resource   resource.Resource
	Topic      string
	DataType   string // string, bool, float
	DataFormat string // MIME type
}

// returns map[broker][]ResourceMQTTEndpoint
func parseMQTTResourceEndpoints(resources []resource.Resource) map[string][]ResourceMQTTEndpoint {
	endpoints := make(map[string][]ResourceMQTTEndpoint)
	for _, r := range resources {
		for _, proto := range r.Protocols {
			broker, topic, contentType, err := isMQTTPublisher(&proto)
			if err != nil {
				if err != notMQTTPublisher {
					fmt.Println("WARN: error parsing MQTT protocol: ", err.Error())
				}
				continue
			}
			mediaType, params, err := mime.ParseMediaType(contentType)
			if err != nil {
				fmt.Println("WARN: error parsing media type: ", err.Error())
				continue
			}
			_, ok := data.SupportedContentTypes[mediaType]
			if !ok {
				fmt.Println("WARN: unsupported Content-Type ", mediaType)
				continue
			}

			dataType := "float"
			// SenML content-type can override this with "datatype" parameter
			if strings.HasPrefix(mediaType, "application/senml") {
				dt, ok := params["datatype"]
				if ok {
					dataType = convertDatatype(dt)
				}
			}
			//fmt.Println(r.Id, mediaType, dataType)

			_, ok = endpoints[broker]
			if !ok {
				endpoints[broker] = []ResourceMQTTEndpoint{}
			}
			endpoint := ResourceMQTTEndpoint{
				Resource:   r,
				Topic:      topic,
				DataType:   dataType,
				DataFormat: mediaType,
			}
			endpoints[broker] = append(endpoints[broker], endpoint)
		}
	}
	return endpoints
}

// if the given protocol is of a MQTT publisher, returns
// 		broker, pub_topic, mime_type, <nil>
// else returns an error
func isMQTTPublisher(proto *resource.Protocol) (string, string, string, error) {
	var (
		broker, pubTopic, mimeType string
		pubMethod                  bool
	)
	if proto.Type != "MQTT" {
		return broker, pubTopic, mimeType, notMQTTPublisher
	}

	for _, m := range proto.Methods {
		if m == "PUB" {
			pubMethod = true
			break
		}
	}
	if !pubMethod {
		return broker, pubTopic, mimeType, notMQTTPublisher
	}

	pubTopic, ok := proto.Endpoint["pub_topic"].(string)
	if !ok {
		return broker, pubTopic, mimeType, fmt.Errorf("MQTT PUB method is defined, but not pub_topic")
	}

	broker, ok = proto.Endpoint["url"].(string)
	if !ok {
		return broker, pubTopic, mimeType, fmt.Errorf("MQTT PUB method is defined, but not broker URL")
	}

	if len(proto.ContentTypes) < 1 {
		return broker, pubTopic, mimeType, fmt.Errorf("No Content-Types are defined")
	}
	// NOTE: will always take the first content-type defined.
	// Assuming that the resources uses only one content-type with MQTT (no content-negotiation possible)
	mimeType = proto.ContentTypes[0]

	return broker, pubTopic, mimeType, nil
}

func convertDatatype(dt string) string {
	switch dt {
	case "boolean":
		return "bool"
	}
	return dt
}