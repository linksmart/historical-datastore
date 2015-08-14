package auth

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Methods to login, obtain Service Token, and logout
type TicketObtainer interface {
	// Given valid username and password,
	// 	Login must return a Ticket Granting Ticket (TGT).
	Login(username, password string) (string, error)
	// Given valid TGT and serviceID,
	//	RequestServiceToken must return a Service Token.
	RequestServiceToken(TGT, serviceID string) (string, error)
	// Given a valid TGT,
	// 	Logout must expire it.
	Logout(TGT string) error
}

// Methods to validate Service Token
type TicketValidator interface {
	// Given a valid serviceToken for the specified serviceID,
	//	ValidateServiceToken must return true with a set of user attributes.
	ValidateServiceToken(serviceToken string) (bool, map[string]string, error)
	// A HTTP handler wraping ValidateServiceToken
	//	which resonds based on the X_auth_token entity header
	ValidateServiceTokenHandler(next http.Handler) http.Handler
}

// TicketValidator: Config
type TicketValidatorConf struct {
	// Auth server address
	ServerAddr string `json:"serverAddr"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// Authorization policy
	Policy string `json:"policy"`
	// Authorization rules
	Rules []Rule `json:"rules"`
}

// TicketValidator: Config Rule
type Rule struct {
	Resources []string `json:"resources"`
	Methods   []string `json:"methods"`
	Users     []string `json:"users"`
	Groups    []string `json:"groups"`
}

// TicketValidator:
// Loads config file into TicketValidatorConf
func LoadTicketValidatorConf(path *string) (*TicketValidatorConf, error) {
	file, err := ioutil.ReadFile(*path)
	if err != nil {
		return nil, err
	}

	var conf TicketValidatorConf
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return nil, err
	}
	// Validate ServerAddr
	conf.ServerAddr = strings.TrimSuffix(conf.ServerAddr, "/")
	_, err = url.Parse(conf.ServerAddr)
	if err != nil {
		return nil, errors.New("Invalid server address (ServerAddr): " + err.Error())
	}

	return &conf, nil
}

// TicketValidator:
// Check whether a user/group is authorized to access resource using a specific method
// The decision is made based on the configured rules and policy
func (c *TicketValidatorConf) IsAuthorized(resource, method, user, group string) bool {
	policyAllow := true
	if c.Policy != "allow" {
		policyAllow = false
	}

	// Create a tree of paths
	// e.g. parses /path1/path2/path3 to [/path1/path2/path3 /path1/path2 /path1]
	// e.g. parses / to [/]
	resource_split := strings.Split(resource, "/")
	resource_split = resource_split[1:len(resource_split)] // truncate the first slash
	var resource_tree []string
	// construct tree from longest to shortest (/path1) path
	for i := len(resource_split); i >= 1; i-- {
		resource_tree = append(resource_tree, "/"+strings.Join(resource_split[0:i], "/"))
	}
	//fmt.Println(len(resource_split), resource_split)
	//fmt.Println(len(resource_tree), resource_tree)

	// Check whether a is in slice
	inSlice := func(a string, slice []string) bool {
		for _, b := range slice {
			if b == a {
				return true
			}
		}
		return false
	}

	for _, rule := range c.Rules {
		for _, res := range resource_tree {
			// Return true if user or group matches a rule
			if inSlice(res, rule.Resources) && inSlice(method, rule.Methods) &&
				(inSlice(user, rule.Users) || inSlice(group, rule.Groups)) {
				return true == policyAllow // XNOR (Negate return if policy is not allow)
			}
		}
	}
	return false == policyAllow // XNOR (Negate return if policy is not allow)
}
