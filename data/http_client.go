// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/linksmart/go-sec/auth/obtainer"
	"github.com/linksmart/historical-datastore/registry"
	"github.com/linksmart/service-catalog/v2/utils"
)

type RemoteClient struct {
	serverEndpoint *url.URL
	ticket         *obtainer.Client
}

func NewRemoteClient(serverEndpoint string, ticket *obtainer.Client) (*RemoteClient, error) {
	// Check if serverEndpoint is a correct BrokerURL
	endpointUrl, err := url.Parse(serverEndpoint)
	if err != nil {
		return nil, err
	}

	return &RemoteClient{
		serverEndpoint: endpointUrl,
		ticket:         ticket,
	}, nil
}

// Submit data for ingestion, where:
// data - is a byte array with actual data
// contentType - mime-type of the data (will be set in the header)
// id... - ID (or array of IDs) of time series for which the data is being submitted
func (c *RemoteClient) Submit(data []byte, contentType string, id ...string) error {
	res, err := utils.HTTPRequest("POST",
		c.serverEndpoint.String()+"/"+strings.Join(id, ","),
		map[string][]string{"Content-Type": []string{contentType}},
		bytes.NewReader(data),
		c.ticket,
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return registry.ErrNotFound
	} else if res.StatusCode != http.StatusAccepted {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%v: %v", res.StatusCode, string(body))
	}

	return nil
}

func (c *RemoteClient) Query(q Query, id ...string) (*RecordSet, error) {
	path := fmt.Sprintf("%v/%v",
		c.serverEndpoint,
		GetUrlFromQuery(q, id...))
	res, err := utils.HTTPRequest("GET",
		path,
		nil,
		nil,
		c.ticket,
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Unable to read body of response: %v", err.Error())
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%v: %v", res.StatusCode, string(body))
	}

	var rs RecordSet
	err = json.Unmarshal(body, &rs)
	if err != nil {
		return nil, err
	}
	return &rs, nil

}
