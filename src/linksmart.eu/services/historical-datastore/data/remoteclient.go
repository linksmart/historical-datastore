package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	senml "github.com/krylovsk/gosenml"

	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/lc/sec/auth/obtainer"
	"linksmart.eu/services/historical-datastore/common"
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
		return registry.ErrNotFound
	} else if res.StatusCode != http.StatusAccepted {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%v: %v", res.Status, string(body))
	}

	return nil
}

func (c *RemoteClient) Query(q Query, page, perPage int, id ...string) (*RecordSet, error) {
	var query string
	if q.Sort != "" {
		query += fmt.Sprintf("&%v=%v", common.ParamSort, q.Sort)
	}
	if q.Limit != 0 {
		query += fmt.Sprintf("&%v=%v", common.ParamLimit, q.Limit)
	}
	if !q.Start.IsZero() {
		query += fmt.Sprintf("&%v=%v", common.ParamStart, q.Start.Format("2006-01-02T15:04:05Z"))
	}
	if !q.End.IsZero() {
		query += fmt.Sprintf("&%v=%v", common.ParamEnd, q.End.Format("2006-01-02T15:04:05Z"))
	}

	res, err := catalog.HTTPRequest("GET",
		fmt.Sprintf("%v/%v?%v=%v&%v=%v%v",
			c.serverEndpoint,
			strings.Join(id, common.IDSeparator),
			common.ParamPage, page,
			common.ParamPerPage, perPage,
			query,
		),
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
		return nil, fmt.Errorf("%v: %v", res.Status, string(body))
	}

	var rs RecordSet
	err = json.Unmarshal(body, &rs)
	if err != nil {
		return nil, err
	}
	return &rs, nil

}
