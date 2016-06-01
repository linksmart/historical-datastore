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
	}
	// TODO: intialize MR client,etc
	return &mrp, nil
}

func (sp *MRResourcesProvider) GetAll() ([]resource.Resource, error) {
	endpoint, _ := url.Parse(sp.conf.Endpoint)
	if sp.fileMode == true {
		return sp.getAllFromFile(endpoint.Path)
	}
	// TODO: otherwise retrieve model from the repository
	return []resource.Resource{}, nil
}

func (sp *MRResourcesProvider) getAllFromFile(filePath string) ([]resource.Resource, error) {
	f, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var model mrc.Model
	err = json.Unmarshal(f, &model)
	if err != nil {
		return nil, err
	}

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
