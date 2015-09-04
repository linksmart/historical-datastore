package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"linksmart.eu/auth/cas/obtainer"
	auth "linksmart.eu/auth/obtainer"
	sc "linksmart.eu/lc/core/catalog/service"
	"linksmart.eu/services/historical-datastore/common"
)

const (
	registrationTemplate = `
	{
	  "meta": {
		"serviceType": "",
	    "version": "",
		"components": []
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
		s.Host = conf.HTTP.PublicAddr
		s.Ttl = int(cat.TTL)

		// meta
		s.Meta["serviceType"] = "linksmart-hds"
		s.Meta["version"] = common.APIVersion
		s.Meta["components"] = []string{"registryAPI", "dataAPI", "aggrAPI"}

		// protocols
		// port from the bind port, address from the public address
		s.Protocols[0].Endpoint["url"] = fmt.Sprintf("http://%v:%v%v", conf.HTTP.PublicAddr, conf.HTTP.BindPort, common.RegistryAPILoc)
		s.Protocols[0].Type = "REST"
		s.Protocols[0].Methods = []string{"GET", "POST", "PUT", "DELETE"}
		s.Protocols[0].ContentTypes = []string{common.DefaultMIMEType}

		// representation
		s.Representation[common.DefaultMIMEType] = map[string]interface{}{}

		sigCh := make(chan bool)
		service, err := s.GetService()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		if cat.Auth == nil {
			go sc.RegisterServiceWithKeepalive(cat.Endpoint, cat.Discover, *service, sigCh, wg, nil)
		} else {
			// Setup auth client with a CAS obtainer
			go sc.RegisterServiceWithKeepalive(cat.Endpoint, cat.Discover, *service, sigCh, wg,
				auth.NewClient(
					obtainer.New(cat.Auth.ServerAddr),
					cat.Auth.Username, cat.Auth.Password, cat.Auth.ServiceID),
			)
		}

		regChannels = append(regChannels, sigCh)
		wg.Add(1)
	}

	return regChannels
}
