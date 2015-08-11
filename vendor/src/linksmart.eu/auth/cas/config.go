package cas

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/url"
	"strings"
)

type Config struct {
	// CAS Server URL (http://hostname:port/cas)
	CasServer string `json:"casServerURL"`

	Username string `json:"username"`
	Password string `json:"password"`
}

// Load API configuration from config file
func (ca *CasAuth) loadConfig(confPath *string) error {
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		return err
	}

	var conf Config
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return err
	}

	// Validate casServer
	if conf.CasServer == "" {
		return errors.New("CAS Server URL (casServer) is not specified.")
	}
	conf.CasServer = strings.TrimSuffix(conf.CasServer, "/")
	_, err = url.Parse(conf.CasServer)
	if err != nil {
		return errors.New("CAS Server URL (casServer): " + err.Error())
	}

	// Validate username / password
	if conf.Username == "" || conf.Password == "" {
		return errors.New("username and password must be specified.")
	}

	ca.conf = conf
	return nil
}
