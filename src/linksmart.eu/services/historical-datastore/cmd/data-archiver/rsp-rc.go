// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

package main

import (
	"fmt"

	"linksmart.eu/lc/core/catalog/resource"
	"linksmart.eu/lc/sec/auth/obtainer"
)

type RCResourcesProvider struct {
	config   *RCConfig
	rcClient resource.CatalogClient
}

func NewRCResourcesProvider(config *RCConfig) (*RCResourcesProvider, error) {
	var (
		authClient *obtainer.Client
		err        error
	)
	// configure RC client
	if config.Auth != nil && config.Auth.Enabled {
		authClient, err = obtainer.NewClient(config.Auth.Provider, config.Auth.ProviderURL,
			config.Auth.Username, config.Auth.Password, config.Auth.ServiceID)
		if err != nil {
			return nil, fmt.Errorf("ERR: Error creating RC auth client: %v", err.Error())
		}
	}

	// the config should have been validated already
	return &RCResourcesProvider{
		config:   config,
		rcClient: resource.NewRemoteCatalogClient(config.Endpoint, authClient),
	}, nil
}

func (sp *RCResourcesProvider) GetAll() ([]resource.Resource, error) {
	var resources []resource.Resource

	// retrieve all resources from the RC
	for page := 1; ; page++ {
		resPage, total, err := sp.rcClient.ListResources(page, resource.MaxPerPage)
		if err != nil {
			return nil, fmt.Errorf("Error retrieving resources from RC: %v", err.Error())
		}

		for _, r := range resPage {
			res := resource.Resource{
				Id:        fmt.Sprintf("%v/resources/%v", sp.config.Endpoint, r.Id),
				Name:      r.Name,
				Meta:      r.Meta,
				Protocols: r.Protocols,
			}
			resources = append(resources, res)
		}

		if page*resource.MaxPerPage >= total {
			break
		}
	}

	return resources, nil
}
