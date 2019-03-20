// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
)

// loads service configuration from a file at the given path
func loadConfig(confPath *string) (*common.Config, error) {
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		return nil, err
	}

	var conf common.Config
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
		return nil, fmt.Errorf("HTTP PublicEndpoint should be a valid BrokerURL")
	}

	// VALIDATE Web Config
	if conf.Web.BindAddr == "" || conf.Web.BindPort == 0 {
		return nil, fmt.Errorf("Web bindAddr and bindPort have to be defined")
	}

	// VALIDATE REGISTRY API CONFIG
	// Check if backend is supported
	if !registry.SupportedBackends(conf.Reg.Backend.Type) {
		return nil, fmt.Errorf("DataStreamList backend type is not supported: %s", conf.Reg.Backend.Type)
	}
	// Check DSN
	_, err = url.Parse(conf.Reg.Backend.DSN)
	if err != nil {
		return nil, err
	}
	// Check retention periods
	for _, rp := range conf.Reg.RetentionPeriods {
		if !common.SupportedPeriod(rp) {
			return nil, fmt.Errorf("DataStreamList retentionPeriod is not valid: %s. Supported period suffixes are: %s",
				rp, strings.Join(common.SupportedPeriods(), ", "))
		}
	}

	// VALIDATE DATA API CONFIG
	// Check if backend is supported
	if !data.SupportedBackends(conf.Data.Backend.Type) {
		return nil, fmt.Errorf("Data backend type is not supported: %s", conf.Data.Backend.Type)
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
	if conf.ServiceCatalog != nil {
		if conf.ServiceCatalog.Endpoint == "" && conf.ServiceCatalog.Discover == false {
			return nil, errors.New("All ServiceCatalog entries must have either endpoint or a discovery flag defined")
		}
		if conf.ServiceCatalog.TTL <= 0 {
			return nil, errors.New("All ServiceCatalog entries must have TTL >= 0")
		}
		if conf.ServiceCatalog.Auth != nil {
			// Validate ticket obtainer config
			err = conf.ServiceCatalog.Auth.Validate()
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
