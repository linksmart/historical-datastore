// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package common

import (
	"github.com/linksmart/go-sec/auth/obtainer"
	"github.com/linksmart/go-sec/auth/validator"
)

type Config struct {
	// Service ID
	ServiceID string `json:"serviceID"`
	// HDS API addr
	HTTP HTTPConf `json:"http"`
	// Registry API Config
	Reg RegConf `json:"registry"`
	// Data API Config
	Data DataConf `json:"data"`
	// Aggregation API Config
	Aggr AggrConf `json:"aggregation"`
	// LinkSmart Service Catalog registration config
	ServiceCatalog ServiceCatalogConf `json:"serviceCatalog"`
	// Auth config
	Auth validator.Conf `json:"auth"`
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
	Backend          RegBackendConf `json:"backend"`
	RetentionPeriods []string       `json:"retentionPeriods"`
}

func (c RegConf) ConfiguredRetention(period string) bool {
	if period == "" {
		// empty means no retention
		return true
	}
	return stringInSlice(period, c.RetentionPeriods)
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

// Aggregation config
type AggrConf struct{}

// LinkSmart Service Catalog registration config
type ServiceCatalogConf struct {
	Enabled  bool          `json:"enabled"`
	Discover bool          `json:"discover"`
	Endpoint string        `json:"endpoint"`
	TTL      uint          `json:"ttl"`
	Auth     obtainer.Conf `json:"auth"`
}
