package main

import (
	"log"

	"linksmart.eu/lc/core/catalog/resource"
	"linksmart.eu/lc/sec/auth/obtainer"
)

type RCResourcesProvider struct {
	config   *RCConfig
	rcClient resource.CatalogClient
}

func NewRCResourcesProvider(config *RCConfig) *RCResourcesProvider {
	var (
		authClient *obtainer.Client
		err        error
	)
	// configure RC client
	if config.Auth != nil && config.Auth.Enabled {
		authClient, err = obtainer.NewClient(config.Auth.Provider, config.Auth.ProviderURL,
			config.Auth.Username, config.Auth.Password, config.Auth.ServiceID)
		if err != nil {
			log.Printf("ERR: Error creating RC auth client: %v", err.Error())
			return nil
		}
	}

	// the config should have been validated already
	return &RCResourcesProvider{
		config:   config,
		rcClient: resource.NewRemoteCatalogClient(config.Endpoint, authClient),
	}
}

// TODO: fix this once we migrate the vendored code to RC API 1.0.0
func (sp *RCResourcesProvider) GetAll() ([]resource.Resource, error) {
	//var resources []resource.Resource
	//
	//// retrieve all resources from the RC
	//for page := 1; ; page++ {
	//	resPage, err := sp.rcClient.GetMany(page, resource.MaxPerPage)
	//	if err != nil {
	//		return nil, fmt.Errorf("Error retrieving resources from RC: %v", err.Error())
	//	}
	//
	//	for _, r := range resPage {
	//
	//		resources = append(resources, r)
	//	}
	//
	//	if page*resource.MaxPerPage >= resPage.Total {
	//		break
	//	}
	//}

	return []resource.Resource{}, nil
}
