// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"fmt"

	_ "code.linksmart.eu/com/go-sec/auth/keycloak/obtainer"
	"code.linksmart.eu/com/go-sec/auth/obtainer"
	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/sc/service-catalog/catalog"
	"code.linksmart.eu/sc/service-catalog/client"
)

func registerInServiceCatalog(conf *common.Config) (func() error, error) {

	cat := conf.ServiceCatalog

	service := catalog.Service{
		ID:          conf.ServiceID,
		Name:        "_linksmart-hds._tcp",
		Description: "LinkSmart Historical Datastore",
		APIs:        map[string]string{"REST API": conf.HTTP.PublicEndpoint},
		Docs: []catalog.Doc{{
			Description: "Documentation",
			APIs:        []string{"REST API"},
			URL:         "https://docs.linksmart.eu/display/HDS",
			Type:        "text/html",
		}},
		Meta: map[string]interface{}{
			"codename":     "HDS",
			"apiVersion":   common.APIVersion,
			"apiEndpoints": []string{common.RegistryAPILoc, common.DataAPILoc},
		},
		TTL: cat.TTL,
	}

	var ticket *obtainer.Client
	var err error
	if cat.Auth != nil {
		// Setup ticket client
		ticket, err = obtainer.NewClient(cat.Auth.Provider, cat.Auth.ProviderURL, cat.Auth.Username, cat.Auth.Password, cat.Auth.ServiceID)
		if err != nil {
			return nil, fmt.Errorf("error creating auth client: %s", err)
		}
	}

	stopRegistrator, _, err := client.RegisterServiceAndKeepalive(cat.Endpoint, service, ticket)
	if err != nil {
		return nil, fmt.Errorf("error registering service: %s", err)
	}

	return stopRegistrator, nil
}
