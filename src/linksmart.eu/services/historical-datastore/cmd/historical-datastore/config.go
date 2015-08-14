package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
)

// Supported Data backend types
var supportedBackends = map[string]bool{
	"influxdb": true,
}

type Config struct {
	// Service ID
	ServiceID string `json:"serviceID"`
	// HDS API addr
	HTTP HTTPConf `json:"http"`
	// Registry API Config
	Reg RegConf `json:"registry"`
	// Data API Config
	Data DataConf `json:"data"`
	// Aggregation API Config
	Aggr AggrConf `json:"aggregation"`
	// Service Catalogs Registration Config
	ServiceCatalogs []ServiceCatalogConf `json:"serviceCatalogs"`
	// Enable Authorization Checking
	EnableAuth bool `json:"enableAuth"`
}

// HTTP config
type HTTPConf struct {
	PublicAddr string `json:"publicAddr"`
	BindAddr   string `json:"bindAddr"`
	BindPort   uint16 `json:"bindPort"`
}

// Registry config
type RegConf struct{}

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
	Discover bool   `json:"discover"`
	Endpoint string `json:"endpoint"`
	TTL      uint   `json:"ttl"`
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
	if conf.HTTP.PublicAddr == "" || conf.HTTP.BindAddr == "" || conf.HTTP.BindPort == 0 {
		return nil, fmt.Errorf("HTTP publicAddr, bindAddr, and bindPort have to be defined")
	}

	// VALIDATE REGISTRY API CONFIG
	//
	//

	// VALIDATE DATA API CONFIG
	// Check if backend is supported
	if !supportedBackends[conf.Data.Backend.Type] {
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
	}

	// VALIDATE AUTHENTICATION SERVER CONFIG
	//	if conf.AuthServer.Enabled == true {
	//		if conf.AuthServer.ServerAddr == "" {
	//			return nil, errors.New("Authentication server address (ServerAddr) is not specified.")
	//		}
	//		conf.AuthServer.ServerAddr = strings.TrimSuffix(conf.AuthServer.ServerAddr, "/")
	//		_, err = url.Parse(conf.AuthServer.ServerAddr)
	//		if err != nil {
	//			return nil, errors.New("Invalid authentication server address (ServerAddr): " + err.Error())
	//		}
	//	}

	return &conf, nil
}
