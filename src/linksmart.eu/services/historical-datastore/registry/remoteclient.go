package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/lc/sec/auth/obtainer"
	"linksmart.eu/services/historical-datastore/common"
)

type RemoteClient struct {
	serverEndpoint *url.URL
	ticket         *obtainer.Client
}

func NewRemoteClient(serverEndpoint string, ticket *obtainer.Client) *RemoteClient {
	// Check if serverEndpoint is a correct URL
	endpointUrl, err := url.Parse(serverEndpoint)
	if err != nil {
		return &RemoteClient{}
	}

	return &RemoteClient{
		serverEndpoint: endpointUrl,
		ticket:         ticket,
	}
}

func (c *RemoteClient) Index(page int, perPage int) (*Registry, error) {
	res, err := catalog.HTTPRequest("GET",
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
		var reg Registry
		err = json.Unmarshal(body, &reg)
		if err != nil {
			return nil, err
		}
		return &reg, nil
	}

	return nil, fmt.Errorf("%v: %v", res.StatusCode, string(body))
}

func (c *RemoteClient) Add(d *DataSource) error {
	b, _ := json.Marshal(d)
	res, err := catalog.HTTPRequest("POST",
		c.serverEndpoint.String()+"/",
		map[string][]string{"Content-Type": []string{"application/ld+json"}},
		bytes.NewReader(b),
		c.ticket,
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusCreated {
		return nil
	}

	// Get body of error
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Unable to read body of error: %v", err.Error())
	}

	return fmt.Errorf("%v: %v", res.StatusCode, string(body))
}

func (c *RemoteClient) Get(id string) (*DataSource, error) {
	res, err := catalog.HTTPRequest("GET",
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

	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		return nil, err
	}

	return &ds, nil
}

func (c *RemoteClient) Update(id string, d *DataSource) error {
	b, _ := json.Marshal(d)
	res, err := catalog.HTTPRequest("PUT",
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
	res, err := catalog.HTTPRequest("DELETE",
		fmt.Sprintf("%v/%v", c.serverEndpoint, id),
		nil,
		bytes.NewReader([]byte{}),
		c.ticket,
	)
	if err != nil {
		return err
	}

	if res.StatusCode == http.StatusNotFound {
		return ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%v", res.StatusCode)
	}

	return nil
}

func (c *RemoteClient) FilterOne(path, op, value string) (*DataSource, error) {
	res, err := catalog.HTTPRequest("GET",
		fmt.Sprintf("%v/%v/%v/%v/%v", c.serverEndpoint, FTypeOne, path, op, value),
		nil,
		nil,
		c.ticket,
	)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%v", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		return nil, err
	}

	return &ds, nil
}

// func (c *RemoteCatalogClient) GetResource(id string) (*Resource, error) {
// 	res, err := catalog.HTTPRequest("GET",
// 		fmt.Sprintf("%v/%v", c.serverEndpoint, id),
// 		nil,
// 		nil,
// 		c.ticket,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if res.StatusCode == http.StatusNotFound {
// 		return nil, ErrorNotFound
// 	} else if res.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("%v", res.StatusCode)
// 	}
// 	return resourceFromResponse(res, c.serverEndpoint.Path)
// }

// func (c *RemoteCatalogClient) FindDevice(path, op, value string) (*Device, error) {
// 	res, err := catalog.HTTPRequest("GET",
// 		fmt.Sprintf("%v/%v/%v/%v/%v", c.serverEndpoint, FTypeDevice, path, op, value),
// 		nil,
// 		nil,
// 		c.ticket,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if res.StatusCode == http.StatusNotFound {
// 		return nil, ErrorNotFound
// 	} else if res.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("%v", res.StatusCode)
// 	}
// 	return deviceFromResponse(res, c.serverEndpoint.Path)
// }

// func (c *RemoteCatalogClient) FindDevices(path, op, value string, page, perPage int) ([]Device, int, error) {
// 	res, err := catalog.HTTPRequest("GET",
// 		fmt.Sprintf("%v/%v/%v/%v/%v?%v=%v&%v=%v",
// 			c.serverEndpoint, FTypeDevices, path, op, value, GetParamPage, page, GetParamPerPage, perPage),
// 		nil,
// 		nil,
// 		c.ticket,
// 	)
// 	if err != nil {
// 		return nil, 0, err
// 	}

// 	return devicesFromResponse(res, c.serverEndpoint.Path)
// }

// func (c *RemoteCatalogClient) FindResource(path, op, value string) (*Resource, error) {
// 	res, err := catalog.HTTPRequest("GET",
// 		fmt.Sprintf("%v/%v/%v/%v/%v", c.serverEndpoint, FTypeResource, path, op, value),
// 		nil,
// 		nil,
// 		c.ticket,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if res.StatusCode == http.StatusNotFound {
// 		return nil, ErrorNotFound
// 	} else if res.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("%v", res.StatusCode)
// 	}
// 	return resourceFromResponse(res, c.serverEndpoint.Path)
// }

// func (c *RemoteCatalogClient) FindResources(path, op, value string, page, perPage int) ([]Device, int, error) {
// 	res, err := catalog.HTTPRequest("GET",
// 		fmt.Sprintf("%v/%v/%v/%v/%v?%v=%v&%v=%v",
// 			c.serverEndpoint, FTypeResources, path, op, value, GetParamPage, page, GetParamPerPage, perPage),
// 		nil,
// 		nil,
// 		c.ticket,
// 	)
// 	if err != nil {
// 		return nil, 0, err
// 	}

// 	return devicesFromResponse(res, c.serverEndpoint.Path)
// }
