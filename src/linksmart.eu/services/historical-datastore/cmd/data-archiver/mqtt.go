// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

type MQTTEndpoint struct {
	SourceID   string
	Topic      string
	DataType   string
	DataFormat string
}

type MQTTSubscriber struct {
	config        *MQTTConfig
	clientID      string
	client        *MQTT.Client
	endpoints     []MQTTEndpoint
	topicEndpoint map[string]MQTTEndpoint
	dataCh        chan DataPacket
}

func NewMQTTSubscriber(config MQTTConfig, endpoints []MQTTEndpoint, dataCh chan DataPacket) *MQTTSubscriber {
	// create a map of topics to endpoints
	topicEndpoint := make(map[string]MQTTEndpoint)
	for _, e := range endpoints {
		topicEndpoint[e.Topic] = e
	}

	return &MQTTSubscriber{
		config:        &config,
		clientID:      fmt.Sprintf("data-archiver-%v", time.Now().UnixNano()),
		endpoints:     endpoints,
		topicEndpoint: topicEndpoint,
		dataCh:        dataCh,
	}
}

func (s *MQTTSubscriber) Start() {
	// configure the mqtt client
	s.configureMqttConnection()

	// start the connection routine
	log.Printf("MQTT Subscriber will connect to the broker %v\n", s.config.URL)
	go s.connect(0)
}

func (s *MQTTSubscriber) Stop() {
	log.Println("MQTTSubscriber.stop()")
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(500)
	}
}

// processes incoming messages from the broker
func (s *MQTTSubscriber) messageHandler(client *MQTT.Client, msg MQTT.Message) {
	// log.Printf("MQTTSubscriber message received: topic: %v payload: %v\n", msg.Topic(), string(msg.Payload()))
	endpoint, ok := s.topicEndpoint[msg.Topic()]
	if !ok {
		log.Println("MQTTSubscriber the receivied message doesn't belong to any registered endpoint. Will ignore.")
		return
	}

	dp := DataPacket{
		SourceID:   endpoint.SourceID,
		Data:       msg.Payload(),
		DataType:   endpoint.DataType,
		DataFormat: endpoint.DataFormat,
	}
	// This will block until the receiver side reads the message (use buffered channel?)
	s.dataCh <- dp
}

func (s *MQTTSubscriber) onConnected(client *MQTT.Client) {
	// subscribe to all topics
	topicFilters := make(map[string]byte)
	for _, e := range s.endpoints {
		log.Printf("MQTTPulbisher.onConnected() will subscribe to topic %s for data source %s", e.Topic, e.SourceID)
		topicFilters[e.Topic] = byte(1)
	}
	client.SubscribeMultiple(topicFilters, s.messageHandler)
}

func (s *MQTTSubscriber) onConnectionLost(client *MQTT.Client, reason error) {
	log.Println("MQTTPulbisher.onConnectionLost() lost connection to the broker: ", reason.Error())

	// Initialize a new client and reconnect
	s.configureMqttConnection()
	go s.connect(0)
}

func (s *MQTTSubscriber) connect(backOff int) {
	if s.client == nil {
		log.Printf("MQTTSubscriber.connect() client is not configured")
		return
	}
	for {
		log.Printf("MQTTSubscriber.connect() connecting to the broker %v, backOff: %v sec\n", s.config.URL, backOff)
		time.Sleep(time.Duration(backOff) * time.Second)
		if s.client.IsConnected() {
			break
		}
		token := s.client.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		log.Printf("MQTTSubscriber.connect() failed to connect: %v\n", token.Error().Error())
		if backOff == 0 {
			backOff = 10
		} else if backOff <= 600 {
			backOff *= 2
		}
	}

	log.Printf("MQTTSubscriber.connect() connected to the broker %v", s.config.URL)
	return
}

func (s *MQTTSubscriber) configureMqttConnection() {
	connOpts := MQTT.NewClientOptions().
		AddBroker(s.config.URL).
		SetClientID(s.clientID).
		SetCleanSession(true).
		SetConnectionLostHandler(s.onConnectionLost).
		SetOnConnectHandler(s.onConnected).
		SetAutoReconnect(false) // we take care of re-connect ourselves

	// Username/password authentication
	if s.config.Username != "" && s.config.Password != "" {
		connOpts.SetUsername(s.config.Username)
		connOpts.SetPassword(s.config.Password)
	}

	// SSL/TLS
	if strings.HasPrefix(s.config.URL, "ssl") {
		tlsConfig := &tls.Config{}
		// Custom CA to auth broker with a self-signed certificate
		if s.config.CaFile != "" {
			caFile, err := ioutil.ReadFile(s.config.CaFile)
			if err != nil {
				log.Printf("MQTTSubscriber.configureMqttConnection() ERROR: failed to read CA file %s:%s\n", s.config.CaFile, err.Error())
			} else {
				tlsConfig.RootCAs = x509.NewCertPool()
				ok := tlsConfig.RootCAs.AppendCertsFromPEM(caFile)
				if !ok {
					log.Printf("MQTTSubscriber.configureMqttConnection() ERROR: failed to parse CA certificate %s\n", s.config.CaFile)
				}
			}
		}
		// Certificate-based client authentication
		if s.config.CertFile != "" && s.config.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(s.config.CertFile, s.config.KeyFile)
			if err != nil {
				log.Printf("MQTTSubscriber.configureMqttConnection() ERROR: failed to load client TLS credentials: %s\n",
					err.Error())
			} else {
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}

		connOpts.SetTLSConfig(tlsConfig)
	}

	s.client = MQTT.NewClient(connOpts)
}
