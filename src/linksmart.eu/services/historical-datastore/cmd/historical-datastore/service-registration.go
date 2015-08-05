package main

import (
	"encoding/json"
	"fmt"
	"sync"

	sc "linksmart.eu/lc/core/catalog/service"
	"linksmart.eu/services/historical-datastore/common"
)

const (
	registrationTemplate = `
	{
	  "meta": {
	    "apiVersion": "",
		"apiComponents": []
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

		s.Type = ""
		s.Name = "HistoricalDatastoreAPI"
		s.Description = ""
		s.Host = conf.HTTP.PublicAddr
		s.Ttl = int(cat.TTL)

		// meta
		s.Meta["apiVersion"] = common.APIVersion
		s.Meta["apiComponents"] = []string{"registryAPI", "dataAPI", "aggrAPI"}

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
		go sc.RegisterServiceWithKeepalive(cat.Endpoint, cat.Discover, *service, sigCh, wg)
		regChannels = append(regChannels, sigCh)
		wg.Add(1)
	}

	return regChannels
}
