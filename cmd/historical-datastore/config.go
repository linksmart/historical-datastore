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
	// HDS API addr
	HTTP HTTPConf `json:"http"`
	// Registry API Config
	Reg RegConf `json:"registry"`
	// Data API Config
	Data DataConf `json:"data"`
	// Aggregation API Config
	Aggr AggrConf `json:"aggregation"`
}

// HTTP config
type HTTPConf struct {
	BindAddr string `json:"bindAddr"`
	BindPort uint16 `json:"bindPort"`
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
	if conf.HTTP.BindAddr == "" || conf.HTTP.BindPort == 0 {
		return nil, fmt.Errorf("HTTP bindAddr and bindPort have to be defined")
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

	return &conf, nil
}
