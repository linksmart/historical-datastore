package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"linksmart.eu/services/historical-datastore/data"
)

type Config struct {
	// HDS API addr
	Http       HttpConfig               `json:"http"`
	InfluxConf data.InfluxStorageConfig `json:"influxdb"`
}

// Http config
type HttpConfig struct {
	BindAddr string `json:"bindAddr"`
	BindPort uint16 `json:"bindPort"`
}

func (h *HttpConfig) Validate() error {
	if h.BindAddr == "" || h.BindPort == 0 {
		return fmt.Errorf("HTTP bindAddr and bindPort have to be defined")
	}
	return nil
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

	// Validate influx config
	conf.InfluxConf.IsValid()
	if err != nil {
		return nil, err
	}

	// Validate
	err = conf.Http.Validate()
	if err != nil {
		return nil, err
	}

	return &conf, nil
}
