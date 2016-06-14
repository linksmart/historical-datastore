package client

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type ModelRepositoryClient struct {
	endpoint string
}

func NewModelRepositoryClient(endpoint string) *ModelRepositoryClient {
	return &ModelRepositoryClient{endpoint}
}

func (c *ModelRepositoryClient) GetJSONModelByName(name string) (*Model, error) {
	var m Model
	res, err := http.Get(fmt.Sprintf("%s/mr/latest/json/%s", c.endpoint, name))
	if err != nil {
		return nil, fmt.Errorf("Error contacting Model Repository: %v", err.Error())
	}

	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&m)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Model Repository response: %v", err.Error())
	}

	return &m, nil
}

// TODO: implement adding model
func (c *ModelRepositoryClient) Add(m Model) (string, error) {
	return "", nil
}

// TODO: implement updating model
func (c *ModelRepositoryClient) Update(name string, m Model) error {
	return nil
}

// TODO: implement deleting model
func (c *ModelRepositoryClient) Delete(name string) error {
	return nil
}

// TODO: implement retriving XMI model
func (c *ModelRepositoryClient) GetXMIModelByName(name string) (string, error) {
	return "", nil
}
