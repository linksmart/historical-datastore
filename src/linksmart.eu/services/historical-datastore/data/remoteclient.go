package data

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	senml "github.com/krylovsk/gosenml"

	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/lc/sec/auth/obtainer"
	"linksmart.eu/services/historical-datastore/registry"
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

func (c *RemoteClient) Submit(senmlMsg *senml.Message, id ...string) error {
	encoder := senml.NewJSONEncoder()
	b, err := encoder.EncodeMessage(senmlMsg)
	if err != nil {
		return err
	}

	res, err := catalog.HTTPRequest("POST",
		c.serverEndpoint.String()+"/"+strings.Join(id, ","),
		map[string][]string{"Content-Type": []string{"application/senml+json"}},
		bytes.NewReader(b),
		c.ticket,
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return registry.ErrorNotFound
	} else if res.StatusCode != http.StatusAccepted {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%v: %v", res.Status, string(body))
	}

	return nil
}

// func (c *RemoteClient) Query(id ...string) (*registry.DataSource, error) {
// 	res, err := catalog.HTTPRequest("GET",
// 		fmt.Sprintf("%v/%v", c.serverEndpoint, id),
// 		nil,
// 		nil,
// 		c.ticket,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer res.Body.Close()

// 	if res.StatusCode == http.StatusNotFound {
// 		return nil, ErrorNotFound
// 	} else if res.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("%v", res.StatusCode)
// 	}

// 	body, err := ioutil.ReadAll(res.Body)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var ds registry.DataSource
// 	err = json.Unmarshal(body, &ds)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &ds, nil
// }
