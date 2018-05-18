// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"fmt"

	"github.com/pborman/uuid"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/sc/service-catalog/catalog"
	"code.linksmart.eu/sc/service-catalog/client"

	_ "code.linksmart.eu/com/go-sec/auth/keycloak/obtainer"
	"code.linksmart.eu/com/go-sec/auth/obtainer"
)

func registerInServiceCatalog(conf *common.Config) func() {
	var unregisterFuncs []func() error

	unregisterAll := func() {
		for _, unregister := range unregisterFuncs {
			err := unregister()
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		}
	}

	serviceID := conf.ServiceID
	if conf.ServiceID == "" {
		serviceID = uuid.New()
	}

	for _, cat := range conf.ServiceCatalogs {
		service := catalog.Service{
			ID:          serviceID,
			Name:        "_linksmart-hds._tcp",
			Description: "LinkSmart® Historical Datastore",
			APIs:        map[string]string{"REST API": conf.HTTP.PublicEndpoint},
			Docs: []catalog.Doc{{
				Description: "Documentation",
				APIs:        []string{"REST API"},
				URL:         "https://docs.linksmart.eu/display/HDS",
				Type:        "text/html",
			}},
			Meta: map[string]interface{}{
				"ls_codename":   "HDS",
				"api_version":   common.APIVersion,
				"api_endpoints": []string{common.RegistryAPILoc, common.DataAPILoc, common.AggrAPILoc},
			},
			TTL: cat.TTL,
		}

		var ticket *obtainer.Client
		var err error
		if cat.Auth != nil {
			// Setup ticket client
			ticket, err = obtainer.NewClient(cat.Auth.Provider, cat.Auth.ProviderURL, cat.Auth.Username, cat.Auth.Password, cat.Auth.ServiceID)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		}

		stopRegistrator, _, err := client.RegisterServiceAndKeepalive(cat.Endpoint, service, ticket)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		unregisterFuncs = append(unregisterFuncs, stopRegistrator)
	}

	return unregisterAll
}
