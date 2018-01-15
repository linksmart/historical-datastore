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

	"code.linksmart.eu/com/go-sec/auth/obtainer"
	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"code.linksmart.eu/sc/service-catalog/utils"
)

type RemoteClient struct {
	serverEndpoint *url.URL
	ticket         *obtainer.Client
}

func NewRemoteClient(serverEndpoint string, ticket *obtainer.Client) (*RemoteClient, error) {
	// Check if serverEndpoint is a correct URL
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
// id... - ID (or array of IDs) of data sources for which the data is being submitted
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

	res, err := utils.HTTPRequest("GET",
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
