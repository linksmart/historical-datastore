package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"

	"linksmart.eu/services/historical-datastore/registry"
)

type Config struct {
	RC   *RCConfig             `json:"rc"` // if not defined - will ignore and use MR
	MR   *MRConfig             `json:"mr"` // if not defined - will ignore and use RC
	HDS  *HDSConfig            `json:"hds"`
	MQTT map[string]MQTTConfig `json:"mqtt"` // map[brokerURL]config
}

func (c *Config) Validate() error {
	// either RC or MR have to be defined
	if c.RC == nil && c.MR == nil {
		return fmt.Errorf("Invalid configuration: neither Resource Catalog nor Model Repository are configured")
	}

	// but not both
	if c.RC != nil && c.MR != nil {
		return fmt.Errorf("Invalid configuration: BOTH Resource Catalog nor Model Repository are configured")
	}

	if c.RC != nil {
		err := c.RC.Validate()
		if err != nil {
			return err
		}
	}

	if c.MR != nil {
		err := c.MR.Validate()
		if err != nil {
			return err
		}
	}

	err := c.HDS.Validate()
	if err != nil {
		return err
	}

	for _, mc := range c.MQTT {
		err = mc.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

// Reousrce Catalog config
type RCConfig struct {
	Endpoint string        `json:"endpoint"`
	Auth     *ObtainerConf `json:"auth"`
	// Discover bool   `json:"discover"`
}

func (c *RCConfig) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("Resource Catalog API Endpoint is not defined.")
	}
	_, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("Resource Catalog API Endpoint is not a valid URL: %v", err.Error())
	}

	if c.Auth != nil {
		err = c.Auth.Validate()
		if err != nil {
			return fmt.Errorf("Resource Catalog API has invalid auth config: %v", err.Error())
		}
	}
	return nil
}

// Model Repository config
type MRConfig struct {
	DefaultHost string `json:"defaultHost"` // default host to be used for "resource" url in HDS
	Endpoint    string `json:"endpoint"`    // file://path/to/dir/file.json is suported
	ModelName   string `json:"modelName"`   // model name in the MR service
	// Discover bool   `json:"discover"`
}

func (c *MRConfig) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("Model Repository API endpoint is not defined")
	}

	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("Model Repository API Endpoint is not a valid URL: %v", err.Error())
	}

	if u.Scheme != "file" && c.ModelName == "" {
		return fmt.Errorf("Model name in the Model Repository config is not deifned (and not an fs endpoint)")
	}
	return nil
}

// Historical Datastore config
type HDSConfig struct {
	Endpoint     string                 `json:"endpoint"`
	Aggregations []registry.Aggregation `json:"aggregations"`
	Auth         *ObtainerConf          `json:"auth"`
	// Discover       bool                   `json:"discover"`
}

func (c *HDSConfig) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("Historical Datastore API endpoint is not defined")
	}
	_, err := url.Parse(c.Endpoint)
	if err != nil {
		return fmt.Errorf("Historical Datastore API endpoint is not a valid URL: %v", err.Error())
	}
	if c.Auth != nil {
		err = c.Auth.Validate()
		if err != nil {
			return fmt.Errorf("Historical Datastore API has invalid auth config: %v", err.Error())
		}
	}
	return nil
}

// MQTT Broker Config
type MQTTConfig struct {
	// Discover bool   `json:"discover"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	CaFile   string `json:"caFile"`
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

func (c *MQTTConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("MQTT broker URL must be provided")
	}
	_, err := url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("MQTT broker URL must be a valid URL")
	}

	if c.Username != "" && c.Password == "" {
		return fmt.Errorf("MQTT username provided, but password is empty")
	}
	return nil
}

func loadConfig(confPath string) (*Config, error) {
	file, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, err
	}

	var conf Config
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}

// Ticket Obtainer Client Config
type ObtainerConf struct {
	Enabled bool `json:"enabled"`
	// Authentication provider name
	Provider string `json:"provider"`
	// Authentication provider URL
	ProviderURL string `json:"providerURL"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// User credentials
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c ObtainerConf) Validate() error {
	if c.Enabled {
		// Validate Provider
		if c.Provider == "" {
			return errors.New("Ticket Obtainer: Auth provider name (provider) is not specified.")
		}

		// Validate ProviderURL
		if c.ProviderURL == "" {
			return errors.New("Ticket Obtainer: Auth provider URL (ProviderURL) is not specified.")
		}
		_, err := url.Parse(c.ProviderURL)
		if err != nil {
			return errors.New("Ticket Obtainer: Auth provider URL (ProviderURL) is invalid: " + err.Error())
		}

		// Validate Username
		if c.Username == "" {
			return errors.New("Ticket Obtainer: Auth Username (username) is not specified.")
		}

		// Validate ServiceID
		if c.ServiceID == "" {
			return errors.New("Ticket Obtainer: Auth Service ID (serviceID) is not specified.")
		}

	}

	return nil
}
