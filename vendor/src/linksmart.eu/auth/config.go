package auth

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)

type Config struct {
	// Auth server address
	ServerAddr string `json:"serverAddr"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// Authorization policy
	Policy string `json:"policy"`
	// Authorization rules
	Rules []Rule `json:"rules"`
}

type Rule struct {
	Resources []string `json:"resources"`
	Methods   []string `json:"methods"`
	Users     []string `json:"users"`
	Groups    []string `json:"groups"`
}

func (c *Config) IsAuthorized(resource, method, user, group string) bool {
	fmt.Println("IsAuthorized() Check:", resource, method, user, group)

	policyAllow := true
	if c.Policy != "allow" {
		policyAllow = false
	}

	resource_split := strings.Split(resource, "/")
	resource_split = resource_split[1:len(resource_split)]
	fmt.Println(len(resource_split), resource_split)
	var resource_tree []string
	for i := len(resource_split); i >= 1; i-- {
		resource_tree = append(resource_tree, "/"+strings.Join(resource_split[0:i], "/"))
	}
	fmt.Println(len(resource_tree), resource_tree)

	now := time.Now()
	for _, rule := range c.Rules {
		for _, res := range resource_tree {
			// Check user
			if inSlice(res, rule.Resources) && inSlice(method, rule.Methods) && inSlice(user, rule.Users) {
				fmt.Println("Took:", time.Since(now))
				return true == policyAllow // xnor
			}
			// Check group
			if inSlice(res, rule.Resources) && inSlice(method, rule.Methods) && inSlice(group, rule.Groups) {
				fmt.Println("Took:", time.Since(now))
				return true == policyAllow // xnor
			}
		}
	}
	//	for _, rule := range c.Rules {
	//		for _, r := range rule.Resources {
	//			for _, m := range rule.Methods {
	//				for _, u := range rule.Users {
	//					if inSlice(r, resource_tree) && m == method && u == user {
	//						fmt.Println("Took:", time.Since(now))
	//						return true
	//					}
	//				}
	//				for _, g := range rule.Groups {
	//					if inSlice(r, resource_tree) && m == method && g == group {
	//						fmt.Println("Took:", time.Since(now))
	//						return true
	//					}
	//				}
	//			}
	//		}
	//	}

	return false == policyAllow // xnor
}

func inSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func LoadConfigFile(path *string) (*Config, error) {
	file, err := ioutil.ReadFile(*path)
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
