// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"fmt"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/pborman/uuid"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

type MQTTConnector struct {
	registryClient registry.Client
	storage        Storage
	managers       map[string]*Manager
}

type Manager struct {
	totalSubscribers int
	client           MQTT.Client
	topics           map[string]int
}

func (m *Manager) incr() {
	m.totalSubscribers++
}

func NewMQTTConnector(registryClient registry.Client, storage Storage) chan<- common.Notification {

	c := &MQTTConnector{
		registryClient: registryClient,
		storage:        storage,
		managers:       make(map[string]*Manager),
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListenerMQTT(c, ntChan)

	return ntChan
}

func (c *MQTTConnector) Register(mqttReg *registry.MQTT) error {

	if _, exists := c.managers[mqttReg.URL]; !exists { // No client for this broker
		opts := MQTT.NewClientOptions() // uses defaults
		opts.AddBroker(mqttReg.URL)
		opts.SetClientID(fmt.Sprintf("HDS-%v", uuid.NewRandom()))

		client := MQTT.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		logger.Printf("Connected to %s", mqttReg.URL)
		c.managers[mqttReg.URL] = &Manager{
			client: client,
			topics: make(map[string]int),
		}

		// Subscribe
		if token := c.managers[mqttReg.URL].client.Subscribe(mqttReg.Topic, mqttReg.QoS, c.MessageHandler); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
			return token.Error()
		}
		logger.Printf("MQTT: Subscribed to `%s` @ %s", mqttReg.Topic, mqttReg.URL)
		c.managers[mqttReg.URL].totalSubscribers = 1
		c.managers[mqttReg.URL].topics[mqttReg.Topic] = 1
	} else { // There is a client for this broker
		if _, exists := c.managers[mqttReg.URL].topics[mqttReg.Topic]; !exists { // No subscription for this topics
			// Subscribe
			if token := c.managers[mqttReg.URL].client.Subscribe(mqttReg.Topic, mqttReg.QoS, c.MessageHandler); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				return token.Error()
			}
			logger.Printf("MQTT: Subscribed to `%s` @ %s", mqttReg.Topic, mqttReg.URL)
			c.managers[mqttReg.URL].topics[mqttReg.Topic] = 1
		} else { // There is a subscription for this topic
			c.managers[mqttReg.URL].topics[mqttReg.Topic]++
		}
		c.managers[mqttReg.URL].totalSubscribers++
	}

	// TODO: REMOVE
	fmt.Println(mqttReg.Topic, mqttReg.URL)
	for k, v := range c.managers {
		fmt.Println(k, v)
	}
	return nil
}

func (c *MQTTConnector) Unregister(mqttReg *registry.MQTT) error {
	c.managers[mqttReg.URL].topics[mqttReg.Topic]--
	c.managers[mqttReg.URL].totalSubscribers--

	if c.managers[mqttReg.URL].topics[mqttReg.Topic] == 0 {
		// Unsubscribe
		if token := c.managers[mqttReg.URL].client.Unsubscribe(mqttReg.Topic); token.Wait() && token.Error() != nil {
			return token.Error()
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

	// TODO: REMOVE
	fmt.Println(mqttReg.Topic, mqttReg.URL)
	for k, v := range c.managers {
		fmt.Println(k, v)
	}
	return nil
}

func (c *MQTTConnector) MessageHandler(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

// HANDLING NOTIFICATIONS

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
