// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"code.linksmart.eu/com/go-sec/auth/obtainer"
	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/sc/service-catalog/utils"
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

func (c *RemoteClient) GetMany(page int, perPage int) (*DataStreamList, error) {
	res, err := utils.HTTPRequest("GET",
		fmt.Sprintf("%v?%v=%v&%v=%v", c.serverEndpoint, common.ParamPage, page, common.ParamPerPage, perPage),
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

	if res.StatusCode == http.StatusOK {
		var reg DataStreamList
		err = json.Unmarshal(body, &reg)
		if err != nil {
			return nil, err
		}
		return &reg, nil
	}

	return nil, fmt.Errorf("%v: %v", res.StatusCode, string(body))
}

func (c *RemoteClient) Add(d *DataStream) (string, error) {
	b, _ := json.Marshal(d)
	res, err := utils.HTTPRequest("POST",
		c.serverEndpoint.String()+"/",
		map[string][]string{"Content-Type": []string{"application/ld+json"}},
		bytes.NewReader(b),
		c.ticket,
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusCreated {
		// retrieve ID from the header
		loc := res.Header.Get("Location")

		return loc, nil
	}

	// Get body of error
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("Unable to read body of error: %v", err.Error())
	}

	return "", fmt.Errorf("%v: %v", res.StatusCode, string(body))
}

func (c *RemoteClient) Get(id string) (*DataStream, error) {
	res, err := utils.HTTPRequest("GET",
		fmt.Sprintf("%v/%v", c.serverEndpoint, id),
		nil,
		nil,
		c.ticket,
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%v", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var ds DataStream
	err = json.Unmarshal(body, &ds)
	if err != nil {
		return nil, err
	}

	return &ds, nil
}

func (c *RemoteClient) Update(id string, d *DataStream) error {
	b, _ := json.Marshal(d)
	res, err := utils.HTTPRequest("PUT",
		fmt.Sprintf("%v/%v", c.serverEndpoint, id),
		nil,
		bytes.NewReader(b),
		c.ticket,
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if res.StatusCode == http.StatusNotFound {
		return ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%v: %v", res.StatusCode, string(body))
	}
	return nil
}

func (c *RemoteClient) Delete(id string) error {
	res, err := utils.HTTPRequest("DELETE",
		fmt.Sprintf("%v/%v", c.serverEndpoint, id),
		nil,
		bytes.NewReader([]byte{}),
		c.ticket,
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if res.StatusCode == http.StatusNotFound {
		return ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%v: %v", res.StatusCode, string(body))
	}

	return nil
}

func (c *RemoteClient) FilterOne(path, op, value string) (*DataStream, error) {
	res, err := utils.HTTPRequest("GET",
		fmt.Sprintf("%v/%v/%v/%v/%v", c.serverEndpoint, FTypeOne, path, op, value),
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
		return nil, err
	}

	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%v: %v", res.StatusCode, string(body))
	}

	var ds DataStream
	err = json.Unmarshal(body, &ds)
	if err != nil {
		return nil, err
	}

	return &ds, nil
}

func (c *RemoteClient) Filter(path, op, value string) ([]DataStream, error) {
	res, err := utils.HTTPRequest("GET",
		fmt.Sprintf("%v/%v/%v/%v/%v", c.serverEndpoint, FTypeMany, path, op, value),
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
		return nil, err
	}

	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%v: %v", res.StatusCode, string(body))
	}

	var reg DataStreamList
	err = json.Unmarshal(body, &reg)
	if err != nil {
		return nil, err
	}

	return reg.Streams, nil
}
