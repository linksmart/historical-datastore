// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"fmt"

	"log"

	_ "linksmart.eu/lc/sec/auth/cas/obtainer"
	"linksmart.eu/lc/sec/auth/obtainer"

	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
	"strings"
)

type DataPacket struct {
	SourceID   string
	Data       []byte
	DataType   string
	DataFormat string
}

type HDSPublisher struct {
	config            *HDSConfig
	resourcesProvider ResourcesProvider
	registryClient    *registry.RemoteClient
	dataClient        *data.RemoteClient
	mqttConfigs       map[string]MQTTConfig
	chExit            chan bool
	chData            chan DataPacket

	mqttSubscribers map[string]*MQTTSubscriber // map[url]MQTTSubscriber
}

func NewHDSPublisher(config *HDSConfig, rsp ResourcesProvider, mqttConfigs map[string]MQTTConfig) (*HDSPublisher, error) {
	var (
		authClient *obtainer.Client
		err        error
	)
	// configure HDS clients
	if config.Auth != nil && config.Auth.Enabled {
		authClient, err = obtainer.NewClient(config.Auth.Provider, config.Auth.ProviderURL,
			config.Auth.Username, config.Auth.Password, config.Auth.ServiceID)
		if err != nil {
			return nil, fmt.Errorf("Error creating HDS auth client: %v", err.Error())
		}
	}
	registryClient, err := registry.NewRemoteClient(config.Endpoint+"/registry", authClient)
	if err != nil {
		return nil, fmt.Errorf("Error creating HDS registry client: %v", err.Error())
	}
	dataClient, err := data.NewRemoteClient(config.Endpoint+"/data", authClient)
	if err != nil {
		return nil, fmt.Errorf("Error creating HDS data client: %v", err.Error())
	}

	// the config should have been validated already
	return &HDSPublisher{
		config:            config,
		resourcesProvider: rsp,
		mqttConfigs:       mqttConfigs,
		registryClient:    registryClient,
		dataClient:        dataClient,
		chExit:            make(chan bool),
		mqttSubscribers:   make(map[string]*MQTTSubscriber),
		chData:            make(chan DataPacket),
	}, nil
}

// Start launches the publisher go routine
func (p *HDSPublisher) Start() error {
	// Get all resources from the resources provider
	resources, err := p.resourcesProvider.GetAll()
	if err != nil {
		return fmt.Errorf("Error retrieving data sources from the configured provider: %v", err.Error())
	}

	// retrieve MQTT endpoints for configured resources
	mqttResEndpoints := parseMQTTResourceEndpoints(resources)

	// configure MQTT subscribers
	for broker := range mqttResEndpoints {
		// check if we have a config for each broker
		_, ok := p.mqttConfigs[broker]
		if !ok {
			return fmt.Errorf("Don't have configuration for MQTT broker %v", broker)
		}
	}

	log.Println("INFO will perist data for the following MQTT data sources: ", mqttResEndpoints)

	// check that there is a DataSource for each resource, and if not - create a new one
	mqttDSEndpoints := make(map[string][]MQTTEndpoint)
	for broker, endpoints := range mqttResEndpoints {
		for _, mqre := range endpoints {
			var dsID string
			// if resource.ID is empty - ignore
			if mqre.Resource.Id == "" {
				log.Println("WARN: ignoring resource with an empty ID: %v", mqre.Resource)
			}
			// Find by the suffix of the resource
			// trim the http:// prefix of the ID
			suffix := strings.TrimPrefix(mqre.Resource.Id, "http://")
			ds, err := p.registryClient.FilterOne("resource", "suffix", suffix)
			if err != nil {
				// create a new one
				if err == registry.ErrNotFound {
					d := registry.DataSource{
						Resource:    mqre.Resource.Id,
						Meta:        mqre.Resource.Meta,
						Type:        mqre.DataType,
						Format:      mqre.DataFormat,
						Aggregation: p.config.Aggregations,
					}
					dsID, err = p.registryClient.Add(&d)
					if err != nil {
						return fmt.Errorf("Error adding Data Source to Historical Datastoure: %v", err.Error())
					}
				} else {
					return fmt.Errorf("Error retrieving Data Source form Historical Datastoure: %v", err.Error())
				}
			} else {
				// update existing data source (metadata and aggregations only)
				ds.Meta = mqre.Resource.Meta
				ds.Aggregation = p.config.Aggregations

				err = p.registryClient.Update(ds.ID, ds)
				if err != nil {
					return fmt.Errorf("Error updating Data Source %v: %v", ds.ID, err.Error())
				}

				dsID = ds.ID
			}

			// prepare MQTT endpoints
			_, ok := mqttDSEndpoints[broker]
			if !ok {
				mqttDSEndpoints[broker] = []MQTTEndpoint{}
			}
			endpoint := MQTTEndpoint{
				SourceID:   dsID,
				Topic:      mqre.Topic,
				DataFormat: mqre.DataFormat,
				DataType:   mqre.DataType,
			}
			mqttDSEndpoints[broker] = append(mqttDSEndpoints[broker], endpoint)
		}
	}

	// configure MQTT subscribers
	for broker, endpoints := range mqttDSEndpoints {
		// check if we have a config for each broker
		_, ok := p.mqttConfigs[broker]
		if !ok {
			return fmt.Errorf("Don't have configuration for MQTT broker %v", broker)
		}

		p.mqttSubscribers[broker] = NewMQTTSubscriber(p.mqttConfigs[broker], endpoints, p.chData)
	}

	// start the data reader
	go p.dataReader()

	// start all subscribers
	for _, s := range p.mqttSubscribers {
		s.Start()
	}
	return nil
}

func (p *HDSPublisher) dataReader() {
	for {
		select {
		case dp := <-p.chData:
			// fmt.Println("Got new data: ", p)
			err := p.dataClient.Submit(dp.Data, dp.DataFormat, dp.SourceID)
			if err != nil {
				log.Printf("ERR: Error submitting data to HDS: %v\n", err.Error())
			}
		// return immediately on shutdown signal
		case <-p.chExit:
			return
		}
	}
}

func (p *HDSPublisher) Shutdown() {
	log.Println("Shutting down subscribers...")
	// stop all subscribers
	for _, s := range p.mqttSubscribers {
		s.Stop()
	}

	log.Println("Shutting down data reader...")
	// signal the dataReader to shutdown
	p.chExit <- true
}
