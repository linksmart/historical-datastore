// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package common

import (
	"code.linksmart.eu/com/go-sec/authz"
	"errors"
	"net/url"
)

type Config struct {
	// Service ID
	ServiceID string `json:"serviceID"`
	// HDS API addr
	HTTP HTTPConf `json:"http"`
	// Web GUI
	Web WebConfig `json:"web"`
	// Registry API Config
	Reg RegConf `json:"registry"`
	// Data API Config
	Data DataConf `json:"data"`
	// Aggregation API Config
	Aggr AggrConf `json:"aggregation"`
	// LinkSmart Service Catalog registration config
	ServiceCatalog *ServiceCatalogConf `json:"serviceCatalog"`
	// Auth config
	Auth ValidatorConf `json:"auth"`
}

// HTTP config
type HTTPConf struct {
	PublicEndpoint string `json:"publicEndpoint"`
	BindAddr       string `json:"bindAddr"`
	BindPort       uint16 `json:"bindPort"`
}

// Web GUI Config
type WebConfig struct {
	BindAddr  string `json:"bindAddr"`
	BindPort  uint16 `json:"bindPort"`
	StaticDir string `json:"staticDir"`
}

// Registry config
type RegConf struct {
	Backend          RegBackendConf `json:"backend"`
	RetentionPeriods []string       `json:"retentionPeriods"`
}

func (c RegConf) ConfiguredRetention(period string) bool {
	if period == "" {
		// empty means no retention
		return true
	}
	return stringInSlice(period, c.RetentionPeriods)
}

// Registry backend config
type RegBackendConf struct {
	Type string `json:"type"`
	DSN  string `json:"dsn"`
}

// Data config
type DataConf struct {
	Backend DataBackendConf `json:"backend"`
	// RetentionPeriods is deprecated, will be removed from v0.6.0. Use registry.retentionPeriods instead.
	RetentionPeriods []string `json:"retentionPeriods"`
	AutoRegistration bool     `json:"autoRegistration"`
}

// Data backend config
type DataBackendConf struct {
	Type string `json:"type"`
	DSN  string `json:"dsn"`
}

// Aggregation config
type AggrConf struct{}

// LinkSmart Service Catalog registration config
type ServiceCatalogConf struct {
	Discover bool          `json:"discover"`
	Endpoint string        `json:"endpoint"`
	TTL      uint          `json:"ttl"`
	Auth     *ObtainerConf `json:"auth"`
}

// Ticket Validator Config
type ValidatorConf struct {
	// Auth switch
	Enabled bool `json:"enabled"`
	// Authentication provider name
	Provider string `json:"provider"`
	// Authentication provider BrokerURL
	ProviderURL string `json:"providerURL"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// Basic Authentication switch
	BasicEnabled bool `json:"basicEnabled"`
	// Authorization config
	Authz *authz.Conf `json:"authorization"`
}

func (c ValidatorConf) Validate() error {

	// Validate Provider
	if c.Provider == "" {
		return errors.New("Ticket Validator: Auth provider name (provider) is not specified.")
	}

	// Validate ProviderURL
	if c.ProviderURL == "" {
		return errors.New("Ticket Validator: Auth provider BrokerURL (providerURL) is not specified.")
	}
	_, err := url.Parse(c.ProviderURL)
	if err != nil {
		return errors.New("Ticket Validator: Auth provider BrokerURL (providerURL) is invalid: " + err.Error())
	}

	// Validate ServiceID
	if c.ServiceID == "" {
		return errors.New("Ticket Validator: Auth Service ID (serviceID) is not specified.")
	}

	// Validate Authorization
	if c.Authz != nil {
		if err := c.Authz.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Ticket Obtainer Client Config
type ObtainerConf struct {
	// Authentication provider name
	Provider string `json:"provider"`
	// Authentication provider BrokerURL
	ProviderURL string `json:"providerURL"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// User credentials
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c ObtainerConf) Validate() error {

	// Validate Provider
	if c.Provider == "" {
		return errors.New("Ticket Obtainer: Auth provider name (provider) is not specified.")
	}

	// Validate ProviderURL
	if c.ProviderURL == "" {
		return errors.New("Ticket Obtainer: Auth provider BrokerURL (ProviderURL) is not specified.")
	}
	_, err := url.Parse(c.ProviderURL)
	if err != nil {
		return errors.New("Ticket Obtainer: Auth provider BrokerURL (ProviderURL) is invalid: " + err.Error())
	}

	// Validate Username
	if c.Username == "" {
		return errors.New("Ticket Obtainer: Auth Username (username) is not specified.")
	}

	// Validate ServiceID
	if c.ServiceID == "" {
		return errors.New("Ticket Obtainer: Auth Service ID (serviceID) is not specified.")
	}

	return nil
}
