package validator

import (
	"errors"
	"net/url"
	"strings"
)

// Validator Config
type Conf struct {
	// Auth server address
	ServerAddr string `json:"serverAddr"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// Authentication
	Authentication bool `json:"authentication"`
	// Authorization rules, if any
	AuthorizationRules []Rule `json:"authorization"`
}

// Validator Config Rule
type Rule struct {
	Resources []string `json:"resources"`
	Methods   []string `json:"methods"`
	Users     []string `json:"users"`
	Groups    []string `json:"groups"`
}

func ValidateConf(conf *Conf) error {

	// Validate ServerAddr
	conf.ServerAddr = strings.TrimSuffix(conf.ServerAddr, "/")
	_, err := url.Parse(conf.ServerAddr)
	if err != nil {
		return errors.New("Invalid server address (ServerAddr): " + err.Error())
	}

	return nil
}
