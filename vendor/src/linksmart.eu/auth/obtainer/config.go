package obtainer

import (
	"errors"
	"net/url"
	"strings"
)

// Obtainer Config
type Conf struct {
	ServerAddr string `json:"serverAddr"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	ServiceID  string `json:"serviceID"`
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
