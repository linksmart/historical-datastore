package udp

import "linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/influxdb/influxdb/toml"

type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`

	Database     string        `toml:"database"`
	BatchSize    int           `toml:"batch-size"`
	BatchTimeout toml.Duration `toml:"batch-timeout"`
}
