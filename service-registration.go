// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	sc "linksmart.eu/lc/core/catalog/service"
	"code.linksmart.eu/hds/historical-datastore/common"

	_ "linksmart.eu/lc/sec/auth/cas/obtainer"
	"linksmart.eu/lc/sec/auth/obtainer"
)

const (
	registrationTemplate = `
	{
	  "meta": {
		"serviceType": "",
	    "version": "",
		"apis": []
	  },
	  "protocols": [
	    {
	      "endpoint": {
	        "url": ""
	      },
	      "methods": [],
	      "content-types": []
	    }
	  ],
	  "representation" : {}
	}
	`
)

func registerInServiceCatalog(conf *Config, wg *sync.WaitGroup) []chan bool {
	regChannels := make([]chan bool, 0, len(conf.ServiceCatalogs))

	for _, cat := range conf.ServiceCatalogs {
		s := sc.ServiceConfig{}

		err := json.Unmarshal([]byte(registrationTemplate), &s)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		s.Type = "Service"
		s.Name = "HistoricalDatastoreAPI"
		s.Description = "Historical Datastore API"
		publicURL, _ := url.Parse(conf.HTTP.PublicEndpoint)
		s.Host = strings.Split(publicURL.Host, ":")[0]
		s.Ttl = int(cat.TTL)

		// meta
		s.Meta["serviceType"] = "linksmart-hds"
		s.Meta["version"] = common.APIVersion
		s.Meta["apis"] = []string{common.RegistryAPILoc, common.DataAPILoc, common.AggrAPILoc}

		// protocols
		// port from the bind port, address from the public address
		s.Protocols[0].Endpoint["url"] = fmt.Sprintf("%s%s", conf.HTTP.PublicEndpoint, common.RegistryAPILoc)
		s.Protocols[0].Type = "REST"
		s.Protocols[0].Methods = []string{"GET", "POST", "PUT", "DELETE"}
		s.Protocols[0].ContentTypes = []string{common.DefaultMIMEType}

		// representation
		// s.Representation[common.DefaultMIMEType] = map[string]interface{}{}

		sigCh := make(chan bool)
		service, err := s.GetService()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		if cat.Auth == nil {
			go sc.RegisterServiceWithKeepalive(cat.Endpoint, cat.Discover, *service, sigCh, wg, nil)
		} else {
			// Setup ticket client
			ticket, err := obtainer.NewClient(cat.Auth.Provider, cat.Auth.ProviderURL, cat.Auth.Username, cat.Auth.Password, cat.Auth.ServiceID)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			// Register with a ticket obtainer client
			go sc.RegisterServiceWithKeepalive(cat.Endpoint, cat.Discover, *service, sigCh, wg, ticket)
		}

		regChannels = append(regChannels, sigCh)
		wg.Add(1)
	}

	return regChannels
}
