// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/kelseyhightower/envconfig"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
)

// loads service configuration from a file at the given path
func loadConfig(confPath *string, ignoreEnv bool) (*common.Config, error) {
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		return nil, err
	}

	var conf common.Config
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return nil, err
	}

	// Override loaded values with environment variables
	if !ignoreEnv {
		err = envconfig.Process("hds", &conf)
		if err != nil {
			return nil, err
		}
	}
	// VALIDATE HTTP
	if conf.HTTP.BindAddr == "" || conf.HTTP.BindPort == 0 || conf.HTTP.PublicEndpoint == "" {
		return nil, fmt.Errorf("HTTP bindAddr, publicEndpoint, and bindPort have to be defined")
	}
	_, err = url.Parse(conf.HTTP.PublicEndpoint)
	if err != nil {
		return nil, fmt.Errorf("HTTP PublicEndpoint should be a valid BrokerURL")
	}

	// VALIDATE REGISTRY API CONFIG
	// Check if backend is supported
	if !registry.SupportedBackends(conf.Registry.Backend.Type) {
		return nil, fmt.Errorf("backend type is not supported: %s", conf.Registry.Backend.Type)
	}
	// Check DSN
	_, err = url.Parse(conf.Registry.Backend.DSN)
	if err != nil {
		return nil, err
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


	// VALIDATE SERVICE CATALOG CONFIG
	if conf.ServiceCatalog.Enabled {
		if conf.ServiceCatalog.Endpoint == "" && conf.ServiceCatalog.Discover == false {
			return nil, errors.New("All ServiceCatalog entries must have either endpoint or a discovery flag defined")
		}
		if conf.ServiceCatalog.TTL <= 0 {
			return nil, errors.New("All ServiceCatalog entries must have TTL >= 0")
		}
		if conf.ServiceCatalog.Auth.Enabled {
			// Validate ticket obtainer config
			err = conf.ServiceCatalog.Auth.Validate()
			if err != nil {
				return nil, fmt.Errorf("invalid Service Catalog auth: %w", err)
			}
		}
	}

	if conf.Auth.Enabled {
		// Validate ticket validator config
		supportedProviders := map[string]bool{
			common.Keycloak: true,
		}
		if !supportedProviders[conf.Auth.Provider] {
			return nil, fmt.Errorf("auth provider %s is not supported", conf.Auth.Provider)
		}
		err = conf.Auth.Validate()
		if err != nil {
			return nil, fmt.Errorf("invalid auth: %w", err)
		}
	}

	return &conf, nil
}
