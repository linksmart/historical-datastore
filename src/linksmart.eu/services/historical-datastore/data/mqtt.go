// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"sync"

	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	senml "github.com/krylovsk/gosenml"
	"github.com/pborman/uuid"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

const (
	mqttRetryInterval = 10 // seconds
)

type MQTTConnector struct {
	registryClient registry.Client
	storage        Storage
	managers       map[string]*Manager
	// cache of resource->ds
	cache map[string]*registry.DataSource
	// failed mqtt registrations
	failedReg *failedReg
}

type failedReg struct {
	m map[string]*registry.MQTTConf
	sync.Mutex
}

func (r *failedReg) add(id string, mqttConf *registry.MQTTConf) {
	r.Lock()
	r.m[id] = mqttConf
	r.Unlock()
}
func (r *failedReg) remove(id string) {
	r.Lock()
	delete(r.m, id)
	r.Unlock()
}

type Manager struct {
	client paho.Client
	// total subscribers using this client
	totalSubscribers int
	// total subscriptions for each topic in this manager
	topics map[string]int
}

func NewMQTTConnector(registryClient registry.Client, storage Storage) (chan<- common.Notification, error) {
	c := &MQTTConnector{
		registryClient: registryClient,
		storage:        storage,
		managers:       make(map[string]*Manager),
		cache:          make(map[string]*registry.DataSource),
		failedReg:      &failedReg{m: make(map[string]*registry.MQTTConf)},
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListenerMQTT(c, ntChan)

	perPage := 100
	for page := 1; ; page++ {
		datasources, total, err := c.registryClient.GetDataSources(page, perPage)
		if err != nil {
			logger.Panicf("MQTT: Error getting data sources: %v", err)
		}

		for _, ds := range datasources {
			if ds.Connector.MQTT != nil {
				err := c.register(ds.Connector.MQTT)
				if err != nil {
					logger.Printf("MQTT: Error registering subscription: %v. Retrying in %ds", err, mqttRetryInterval)
					c.failedReg.add(ds.ID, ds.Connector.MQTT)
				}
			}
		}

		if page*perPage >= total {
			break
		}
	}

	go c.retryRegistrations()

	return ntChan, nil
}

func (c *MQTTConnector) retryRegistrations() {
	for {
		time.Sleep(mqttRetryInterval * time.Second)
		c.failedReg.Lock()
		for id, mqttConf := range c.failedReg.m {
			err := c.register(mqttConf)
			if err != nil {
				logger.Printf("MQTT: Error registering subscription: %v. Retrying in %ds", err, mqttRetryInterval)
				continue
			}
			delete(c.failedReg.m, id)
		}
		c.failedReg.Unlock()
	}
}

func (c *MQTTConnector) register(mqttConf *registry.MQTTConf) error {

	if _, exists := c.managers[mqttConf.URL]; !exists { // No client for this broker
		opts := paho.NewClientOptions() // uses defaults
		opts.AddBroker(mqttConf.URL)
		opts.SetClientID(fmt.Sprintf("HDS-%v", uuid.NewRandom()))

		client := paho.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return logger.Errorf("MQTT: Error connecting to broker: %v", token.Error())
		}
		logger.Printf("MQTT: Connected to %s", mqttConf.URL)
		c.managers[mqttConf.URL] = &Manager{
			client: client,
			topics: make(map[string]int),
		}

		// Subscribe
		if token := c.managers[mqttConf.URL].client.Subscribe(mqttConf.Topic, mqttConf.QoS, c.messageHandler); token.Wait() && token.Error() != nil {
			return logger.Errorf("MQTT: Error subscribing: %v", token.Error())
		}
		logger.Printf("MQTT: Subscribed to `%s` @%s", mqttConf.Topic, mqttConf.URL)
		c.managers[mqttConf.URL].totalSubscribers = 1
		c.managers[mqttConf.URL].topics[mqttConf.Topic] = 1
	} else { // There is a client for this broker
		if _, exists := c.managers[mqttConf.URL].topics[mqttConf.Topic]; !exists { // No subscription for this topics
			// Subscribe
			if token := c.managers[mqttConf.URL].client.Subscribe(mqttConf.Topic, mqttConf.QoS, c.messageHandler); token.Wait() && token.Error() != nil {
				return logger.Errorf("MQTT: Error subscribing: %v", token.Error())
			}
			logger.Printf("MQTT: Subscribed to `%s` @%s", mqttConf.Topic, mqttConf.URL)
			c.managers[mqttConf.URL].topics[mqttConf.Topic] = 1
		} else { // There is a subscription for this topic
			logger.Debugf("MQTT: Already subscribed to `%s` @%s", mqttConf.Topic, mqttConf.URL)
			c.managers[mqttConf.URL].topics[mqttConf.Topic]++
		}
		c.managers[mqttConf.URL].totalSubscribers++
	}

	return nil
}

func (c *MQTTConnector) unregister(mqttConf *registry.MQTTConf) error {
	c.managers[mqttConf.URL].topics[mqttConf.Topic]--
	c.managers[mqttConf.URL].totalSubscribers--

	if c.managers[mqttConf.URL].topics[mqttConf.Topic] == 0 {
		// Unsubscribe
		if token := c.managers[mqttConf.URL].client.Unsubscribe(mqttConf.Topic); token.Wait() && token.Error() != nil {
			return logger.Errorf("MQTT: Error unsubscribing: %v", token.Error())
		}
		delete(c.managers[mqttConf.URL].topics, mqttConf.Topic)
		logger.Printf("MQTT: Unsubscribed from `%s` @ %s", mqttConf.Topic, mqttConf.URL)
	}
	if c.managers[mqttConf.URL].totalSubscribers == 0 {
		// Disconnect
		c.managers[mqttConf.URL].client.Disconnect(250)
		delete(c.managers, mqttConf.URL)
		logger.Printf("MQTT: Disconnected from %s", mqttConf.URL)
	}

	return nil
}

// Handles incoming MQTT messages
func (c *MQTTConnector) messageHandler(client paho.Client, msg paho.Message) {
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
			// TODO
			// Make sure the message is coming from the broker bound to this datasource
			if ds.Connector.MQTT == nil {
				logger.Printf("MQTT: Ignoring unbound message for data source: %v", e.Name)
				return
			}
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
		err := c.register(ds.Connector.MQTT)
		if err != nil {
			callback <- logger.Errorf("MQTT: Error adding subscription: %v", err)
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
			err := c.unregister(oldDS.Connector.MQTT)
			if err != nil {
				callback <- logger.Errorf("MQTT: Error removing subscription: %v", err)
				return
			}
		}
		c.failedReg.remove(oldDS.ID)
		// Add new subscription
		if newDS.Connector.MQTT != nil {
			err := c.register(newDS.Connector.MQTT)
			if err != nil {
				callback <- logger.Errorf("MQTT: Error adding subscription: %v", err)
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
		err := c.unregister(oldDS.Connector.MQTT)
		if err != nil {
			callback <- logger.Errorf("MQTT: Error removing subscription: %v", err)
			return
		}
		c.failedReg.remove(oldDS.ID)
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
