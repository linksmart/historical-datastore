// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	senml "github.com/krylovsk/gosenml"
	"github.com/pborman/uuid"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

type MQTTConnector struct {
	registryClient registry.Client
	storage        Storage
	managers       map[string]*Manager
	cache          map[string]*registry.DataSource // resource->ds
}

type Manager struct {
	totalSubscribers int
	client           MQTT.Client
	topics           map[string]int
}

func (m *Manager) incr() {
	m.totalSubscribers++
}

func NewMQTTConnector(registryClient registry.Client, storage Storage) (chan<- common.Notification, error) {

	c := &MQTTConnector{
		registryClient: registryClient,
		storage:        storage,
		managers:       make(map[string]*Manager),
		cache:          make(map[string]*registry.DataSource),
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListenerMQTT(c, ntChan)

	perPage := 100
	for page := 1; ; page++ {
		datasources, total, err := c.registryClient.GetDataSources(page, perPage)
		if err != nil {
			return nil, err
		}

		for _, ds := range datasources {
			if ds.Connector.MQTT != nil {
				err := c.Register(ds.Connector.MQTT)
				if err != nil {
					return nil, logger.Errorf("Error registering MQTT subscription: %v", err)
				}
			}
		}

		if page*perPage >= total {
			break
		}
	}

	return ntChan, nil
}

func (c *MQTTConnector) Register(mqttReg *registry.MQTT) error {

	if _, exists := c.managers[mqttReg.URL]; !exists { // No client for this broker
		opts := MQTT.NewClientOptions() // uses defaults
		opts.AddBroker(mqttReg.URL)
		opts.SetClientID(fmt.Sprintf("HDS-%v", uuid.NewRandom()))

		client := MQTT.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return logger.Errorf("Error connecting to broker: %v", token.Error())
		}
		logger.Printf("MQTT: Connected to %s", mqttReg.URL)
		c.managers[mqttReg.URL] = &Manager{
			client: client,
			topics: make(map[string]int),
		}

		// Subscribe
		if token := c.managers[mqttReg.URL].client.Subscribe(mqttReg.Topic, mqttReg.QoS, c.MessageHandler); token.Wait() && token.Error() != nil {
			return logger.Errorf("Error subscribing: %v", token.Error())
		}
		logger.Printf("MQTT: Subscribed to `%s` @ %s", mqttReg.Topic, mqttReg.URL)
		c.managers[mqttReg.URL].totalSubscribers = 1
		c.managers[mqttReg.URL].topics[mqttReg.Topic] = 1
	} else { // There is a client for this broker
		if _, exists := c.managers[mqttReg.URL].topics[mqttReg.Topic]; !exists { // No subscription for this topics
			// Subscribe
			if token := c.managers[mqttReg.URL].client.Subscribe(mqttReg.Topic, mqttReg.QoS, c.MessageHandler); token.Wait() && token.Error() != nil {
				return logger.Errorf("Error subscribing: %v", token.Error())
			}
			logger.Printf("MQTT: Subscribed to `%s` @ %s", mqttReg.Topic, mqttReg.URL)
			c.managers[mqttReg.URL].topics[mqttReg.Topic] = 1
		} else { // There is a subscription for this topic
			c.managers[mqttReg.URL].topics[mqttReg.Topic]++
		}
		c.managers[mqttReg.URL].totalSubscribers++
	}

	return nil
}

func (c *MQTTConnector) Unregister(mqttReg *registry.MQTT) error {
	c.managers[mqttReg.URL].topics[mqttReg.Topic]--
	c.managers[mqttReg.URL].totalSubscribers--

	if c.managers[mqttReg.URL].topics[mqttReg.Topic] == 0 {
		// Unsubscribe
		if token := c.managers[mqttReg.URL].client.Unsubscribe(mqttReg.Topic); token.Wait() && token.Error() != nil {
			return logger.Errorf("Error unsubscribing: %v", token.Error())
		}
		delete(c.managers[mqttReg.URL].topics, mqttReg.Topic)
		logger.Printf("MQTT: Unsubscribed from `%s` @ %s", mqttReg.Topic, mqttReg.URL)
	}
	if c.managers[mqttReg.URL].totalSubscribers == 0 {
		// Disconnect
		c.managers[mqttReg.URL].client.Disconnect(250)
		delete(c.managers, mqttReg.URL)
		logger.Printf("MQTT: Disconnected from %s", mqttReg.URL)
	}

	return nil
}

func (c *MQTTConnector) MessageHandler(client MQTT.Client, msg MQTT.Message) {
	logger.Debugf("MQTT: %s %s", msg.Topic(), msg.Payload())

	data := make(map[string][]DataPoint)
	sources := make(map[string]registry.DataSource)
	var senmlMessage senml.Message

	err := json.Unmarshal(msg.Payload(), &senmlMessage)
	if err != nil {
		logger.Printf("MQTT: Error parsing json: %s : %v", msg.Payload(), err)
		return
	}

	err = senmlMessage.Validate()
	if err != nil {
		logger.Printf("MQTT: Invalid SenML: %s : %v", msg.Payload(), err)
		return
	}

	// Fill the data map with provided data points
	entries := senmlMessage.Expand().Entries
	for _, e := range entries {
		if e.Name == "" {
			logger.Printf("MQTT: Error: Resource name not specified: %s", msg.Payload())
			return
		}
		// Find the data source for this entry
		ds, exists := c.cache[e.Name]
		if !exists {
			fmt.Println(e.Name)
			ds, err = c.registryClient.FindDataSource("resource", "equals", e.Name)
			if err != nil {
				logger.Printf("MQTT: Error finding data source: %v", e.Name)
				return
			}
			if ds == nil {
				logger.Printf("MQTT: Error: Unable to find resource in registry: %v", e.Name)
				return
			}
			c.cache[e.Name] = ds
		}

		// Check if type of value matches the data source type in registry
		typeError := false
		switch ds.Type {
		case common.FLOAT:
			if e.BooleanValue != nil || e.StringValue != nil && *e.StringValue != "" {
				typeError = true
			}
		case common.STRING:
			if e.Value != nil || e.BooleanValue != nil {
				typeError = true
			}
		case common.BOOL:
			if e.Value != nil || e.StringValue != nil && *e.StringValue != "" {
				typeError = true
			}
		}
		if typeError {
			logger.Printf("MQTT: Error: Entry for data point %v has a type that is incompatible with source registration. Source %v has type %v.", e.Name, ds.ID, ds.Type)
			return
		}

		_, ok := data[ds.ID]
		if !ok {
			data[ds.ID] = []DataPoint{}
			sources[ds.ID] = *ds
		}
		p := NewDataPoint()
		data[ds.ID] = append(data[ds.ID], p.FromEntry(e))
	}

	// Add data to the storage
	err = c.storage.Submit(data, sources)
	if err != nil {
		logger.Printf("MQTT: Error writing data to the database: %v", err)
		return
	}
}

// NOTIFICATION HANDLERS

// Handles the creation of a new data source
func (c *MQTTConnector) NtfCreated(ds registry.DataSource, callback chan error) {

	if ds.Connector.MQTT != nil {
		err := c.Register(ds.Connector.MQTT)
		if err != nil {
			callback <- logger.Errorf("Error registering MQTT subscription: %s", err)
			return
		}
	}

	callback <- nil
}

// Handles updates of a data source
func (c *MQTTConnector) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {

	if oldDS.Connector.MQTT != newDS.Connector.MQTT {
		// Remove old subscription
		if oldDS.Connector.MQTT != nil {
			err := c.Unregister(oldDS.Connector.MQTT)
			if err != nil {
				callback <- logger.Errorf("Error removing MQTT subscription: %s", err)
				return
			}
		}
		// Add new subscription
		if newDS.Connector.MQTT != nil {
			err := c.Register(newDS.Connector.MQTT)
			if err != nil {
				callback <- logger.Errorf("Error adding MQTT subscription: %s", err)
				return
			}
		}
	}
	callback <- nil
}

// Handles deletion of a data source
func (c *MQTTConnector) NtfDeleted(oldDS registry.DataSource, callback chan error) {
	// Remove old subscription
	if oldDS.Connector.MQTT != nil {
		delete(c.cache, oldDS.Resource)
		err := c.Unregister(oldDS.Connector.MQTT)
		if err != nil {
			callback <- logger.Errorf("Error removing MQTT subscription: %s", err)
			return
		}
	}
	callback <- nil
}

// Handles an incoming notification
func NtfListenerMQTT(c *MQTTConnector, ntChan <-chan common.Notification) {
	for ntf := range ntChan {
		switch ntf.Type {
		case common.CREATE:
			ds, ok := ntf.Payload.(registry.DataSource)
			if !ok {
				logger.Println("ntListener() create: Bad notification!", ds)
				continue
			}
			c.NtfCreated(ds, ntf.Callback)
		case common.UPDATE:
			dss, ok := ntf.Payload.([]registry.DataSource)
			if !ok || len(dss) < 2 {
				logger.Println("ntListener() update: Bad notification!", dss)
				continue
			}
			c.NtfUpdated(dss[0], dss[1], ntf.Callback)
		case common.DELETE:
			ds, ok := ntf.Payload.(registry.DataSource)
			if !ok {
				logger.Println("ntListener() delete: Bad notification!", ds)
				continue
			}
			c.NtfDeleted(ds, ntf.Callback)
		default:
			// other notifications
		}
	}
}
