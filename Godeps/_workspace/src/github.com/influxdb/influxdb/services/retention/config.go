package retention

import (
	"time"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/influxdb/influxdb/toml"
)

type Config struct {
	Enabled       bool          `toml:"enabled"`
	CheckInterval toml.Duration `toml:"check-interval"`
}

func NewConfig() Config {
	return Config{Enabled: true, CheckInterval: toml.Duration(10 * time.Minute)}
}
