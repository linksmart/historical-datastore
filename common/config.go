// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package common

import (
	json "encoding/json"

	"github.com/linksmart/go-sec/auth/obtainer"
	"github.com/linksmart/go-sec/auth/validator"
)

type Config struct {
	// Service ID
	ServiceID string `json:"serviceID"`
	// HDS API addr
	HTTP HTTPConf `json:"http"`
	// HDS GRPC API addr
	GRPC GRPCConf `json:"grpc"`
	//DNS service discovery
	DnssdEnabled bool `json:"dnssdEnabled"`
	//DNS-SD description
	Description string `json:"description"`
	// Registry API Config
	Registry RegConf `json:"registry"`
	// Data API Config
	Data DataConf `json:"data"`
	// LinkSmart Service Catalog registration config
	ServiceCatalog ServiceCatalogConf `json:"serviceCatalog"`
	// Auth config
	Auth validator.Conf `json:"auth"`
	// PKI config
	PKI PKIConf `json:"pki"`
	// Sync has Synchronization configuration
	Sync SyncConf `json:"sync"`
}

// GRPC config
type GRPCConf struct {
	Enabled  bool   `json:"enabled"`
	BindAddr string `json:"bindAddr"`
	BindPort uint16 `json:"bindPort"`
}

// HTTP config
type HTTPConf struct {
	PublicEndpoint string `json:"publicEndpoint"`
	BindAddr       string `json:"bindAddr"`
	BindPort       uint16 `json:"bindPort"`
}

// Web GUI Config
type WebConfig struct {
	BindAddr  string `json:"bindAddr"`
	BindPort  uint16 `json:"bindPort"`
	StaticDir string `json:"staticDir"`
}

// Registry config
type RegConf struct {
	Backend RegBackendConf `json:"backend"`
}

// Registry backend config
type RegBackendConf struct {
	Type string `json:"type"`
	DSN  string `json:"dsn"`
}

// Data config
type DataConf struct {
	Backend DataBackendConf `json:"backend"`
	// RetentionPeriods is deprecated, will be removed from v0.6.0. Use registry.retentionPeriods instead.
	RetentionPeriods []string `json:"retentionPeriods"`
	AutoRegistration bool     `json:"autoRegistration"`
}

// Data backend config
type DataBackendConf struct {
	Type string `json:"type"`
	DSN  string `json:"dsn"`
}

// LinkSmart Service Catalog registration config
type ServiceCatalogConf struct {
	Enabled  bool          `json:"enabled"`
	Discover bool          `json:"discover"`
	Endpoint string        `json:"endpoint"`
	TTL      uint          `json:"ttl"`
	Auth     obtainer.Conf `json:"auth"`
}

// Certificate authority and Certificate and
type PKIConf struct {
	CaCert     string `json:"caCert"`
	ServerCert string `json:"serverCert"`
	ServerKey  string `json:"serverKey"`
}

type SyncConf struct {
	Enabled      bool   `json:"enabled"`
	Destination  string `json:"destination"`
	SyncInterval string `json:"syncInterval"`
}

func (c Config) String() string {
	c.ServiceCatalog.Auth.Password = "******"
	c.ServiceCatalog.Auth.Username = "******"
	c.ServiceCatalog.Auth.ClientID = "******"
	retByte, _ := json.MarshalIndent(c, "", "   ")
	return string(retByte)
}
