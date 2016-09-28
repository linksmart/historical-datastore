// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"

	"linksmart.eu/lc/sec/authz"
)

// Supported Registry backend types
var supportedRegBackends = map[string]bool{
	"memory":  true,
	"leveldb": true,
}

// Supported Data backend types
var supportedDataBackends = map[string]bool{
	"influxdb": true,
}

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
	// Service Catalogs Registration Config
	ServiceCatalogs []ServiceCatalogConf `json:"serviceCatalogs"`
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
	Backend RegBackendConf `json:"backend"`
}

// Registry backend config
type RegBackendConf struct {
	Type string `json:"type"`
	DSN  string `json:"dsn"`
}

// Data config
type DataConf struct {
	Backend DataBackendConf `json:"backend"`
}

// Data backend config
type DataBackendConf struct {
	Type string `json:"type"`
	DSN  string `json:"dsn"`
}

// Aggregation config
type AggrConf struct{}

// Service Catalogs Registration Config
type ServiceCatalogConf struct {
	Discover bool          `json:"discover"`
	Endpoint string        `json:"endpoint"`
	TTL      uint          `json:"ttl"`
	Auth     *ObtainerConf `json:"auth"`
}

// Load API configuration from config file
func loadConfig(confPath *string) (*Config, error) {
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		return nil, err
	}

	var conf Config
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return nil, err
	}

	// VALIDATE HTTP
	if conf.HTTP.BindAddr == "" || conf.HTTP.BindPort == 0 || conf.HTTP.PublicEndpoint == "" {
		return nil, fmt.Errorf("HTTP bindAddr, publicEndpoint, and bindPort have to be defined")
	}
	_, err = url.Parse(conf.HTTP.PublicEndpoint)
	if err != nil {
		return nil, fmt.Errorf("HTTP PublicEndpoint should be a valid URL")
	}

	// VALIDATE Web Config
	if conf.Web.BindAddr == "" || conf.Web.BindPort == 0 {
		return nil, fmt.Errorf("Web bindAddr and bindPort have to be defined")
	}

	// VALIDATE REGISTRY API CONFIG
	// Check if backend is supported
	if !supportedRegBackends[conf.Reg.Backend.Type] {
		return nil, errors.New("Registry backend type is not supported!")
	}
	// Check DSN
	_, err = url.Parse(conf.Reg.Backend.DSN)
	if err != nil {
		return nil, err
	}

	// VALIDATE DATA API CONFIG
	// Check if backend is supported
	if !supportedDataBackends[conf.Data.Backend.Type] {
		return nil, errors.New("Data backend type is not supported!")
	}
	// Check DSN
	_, err = url.Parse(conf.Data.Backend.DSN)
	if err != nil {
		return nil, err
	}

	// VALIDATE AGGREGATION API CONFIG
	//
	//

	// VALIDATE SERVICE CATALOG CONFIG
	for _, cat := range conf.ServiceCatalogs {
		if cat.Endpoint == "" && cat.Discover == false {
			return nil, errors.New("All ServiceCatalog entries must have either endpoint or a discovery flag defined")
		}
		if cat.TTL <= 0 {
			return nil, errors.New("All ServiceCatalog entries must have TTL >= 0")
		}
		if cat.Auth != nil {
			// Validate ticket obtainer config
			err = cat.Auth.Validate()
			if err != nil {
				return nil, err
			}
		}
	}

	if conf.Auth.Enabled {
		// Validate ticket validator config
		err = conf.Auth.Validate()
		if err != nil {
			return nil, err
		}
	}

	return &conf, nil
}

// Ticket Validator Config
type ValidatorConf struct {
	// Auth switch
	Enabled bool `json:"enabled"`
	// Authentication provider name
	Provider string `json:"provider"`
	// Authentication provider URL
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
		return errors.New("Ticket Validator: Auth provider URL (providerURL) is not specified.")
	}
	_, err := url.Parse(c.ProviderURL)
	if err != nil {
		return errors.New("Ticket Validator: Auth provider URL (providerURL) is invalid: " + err.Error())
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
	// Authentication provider URL
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
		return errors.New("Ticket Obtainer: Auth provider URL (ProviderURL) is not specified.")
	}
	_, err := url.Parse(c.ProviderURL)
	if err != nil {
		return errors.New("Ticket Obtainer: Auth provider URL (ProviderURL) is invalid: " + err.Error())
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
