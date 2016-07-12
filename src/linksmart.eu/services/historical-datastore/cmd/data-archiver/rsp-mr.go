// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"linksmart.eu/lc/core/catalog/resource"
	mrc "linksmart.eu/redmine/linksmart-opensource/linksmart-services/model-repository/go-client.git/src/linksmart.eu/services/mr/client"
)

type MRResourcesProvider struct {
	conf     *MRConfig
	client   *mrc.ModelRepositoryClient
	fileMode bool
}

func NewMRResourcesProvider(conf *MRConfig) (*MRResourcesProvider, error) {
	mrp := MRResourcesProvider{
		conf: conf,
	}

	// this should have been checked at validation
	endpoint, _ := url.Parse(conf.Endpoint)
	if endpoint.Scheme == "file" {
		mrp.fileMode = true
		return &mrp, nil
	} else {
		mrp.client = mrc.NewModelRepositoryClient(conf.Endpoint)
	}
	return &mrp, nil
}

func (sp *MRResourcesProvider) GetAll() ([]resource.Resource, error) {
	var model *mrc.Model
	var err error

	endpoint, _ := url.Parse(sp.conf.Endpoint)
	if sp.fileMode == true {
		r, err := ioutil.ReadFile(endpoint.Path)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(r, model)
		if err != nil {
			return nil, err
		}
	} else {
		model, err = sp.client.GetJSONModelByName(sp.conf.ModelName)
		if err != nil {
			return nil, err
		}
	}
	resources, err := sp.parseModel(model)
	if err != nil {
		return nil, err
	}
	return resources, nil
}

func (sp *MRResourcesProvider) parseModel(model *mrc.Model) ([]resource.Resource, error) {
	devices, err := model.ParseDevices()
	if err != nil {
		return nil, err
	}

	var resources []resource.Resource
	for _, d := range devices {
		for _, dr := range d.Resources {
			var protocol resource.Protocol
			switch dr.ExtProtocol.(type) {
			case *mrc.MQTTProtocol:
				protocol = resource.Protocol{
					Type:         dr.ExtProtocol.(*mrc.MQTTProtocol).Protocol.Type,
					Endpoint:     dr.ExtProtocol.(*mrc.MQTTProtocol).Protocol.Endpoint,
					Methods:      dr.ExtProtocol.(*mrc.MQTTProtocol).Protocol.Methods,
					ContentTypes: dr.ExtProtocol.(*mrc.MQTTProtocol).Protocol.ContentTypes,
				}
			case *mrc.RESTProtocol:
				protocol = resource.Protocol{
					Type:         dr.ExtProtocol.(*mrc.RESTProtocol).Protocol.Type,
					Endpoint:     dr.ExtProtocol.(*mrc.RESTProtocol).Protocol.Endpoint,
					Methods:      dr.ExtProtocol.(*mrc.RESTProtocol).Protocol.Methods,
					ContentTypes: dr.ExtProtocol.(*mrc.RESTProtocol).Protocol.ContentTypes,
				}
			case *mrc.Protocol:
				protocol = resource.Protocol{
					Type:         dr.ExtProtocol.(*mrc.Protocol).Type,
					Endpoint:     dr.ExtProtocol.(*mrc.Protocol).Endpoint,
					Methods:      dr.ExtProtocol.(*mrc.Protocol).Methods,
					ContentTypes: dr.ExtProtocol.(*mrc.Protocol).ContentTypes,
				}
			default:
				fmt.Printf("WARN: unknown protocol type %T on resource %v, will ignore\n", dr.ExtProtocol, dr.ID)
			}

			r := resource.Resource{
				Id:        fmt.Sprintf("%v/%v", sp.conf.DefaultHost, dr.ID),
				Name:      dr.Name,
				Meta:      dr.Meta,
				Protocols: []resource.Protocol{protocol},
			}
			resources = append(resources, r)
		}
	}

	return resources, nil
}
