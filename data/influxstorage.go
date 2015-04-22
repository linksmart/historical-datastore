package data

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	influx "linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/influxdb/influxdb/client"
	"linksmart.eu/services/historical-datastore/registry"
)

// influxStorage implements a simple data storage back-end with SQLite
type influxStorage struct {
	client *influx.Client
	config *InfluxStorageConfig
}

// InfluxStorageConfig describes the influxdb storage configuration
type InfluxStorageConfig struct {
	URL      url.URL
	Database string
	Username string
	Password string
}

func seriesFromURL(u url.URL) string {
	h := strings.Replace(u.Host, ".", "_", -1)
	p := strings.Replace(u.Path, "/", "_", -1)
	return fmt.Sprintf("hds_data_%s%s", h, p)
}

func (c *InfluxStorageConfig) isValid() error {
	if c.URL.Host == "" {
		return fmt.Errorf("host:port in the URL must be not empty")
	}
	if c.Database == "" {
		return fmt.Errorf("db must be not empty")
	}
	return nil
}

// NewInfluxStorage returns a new Storage given a configuration
func NewInfluxStorage(cfg *InfluxStorageConfig) (Storage, error) {
	err := cfg.isValid()
	if err != nil {
		return nil, fmt.Errorf("Invalid config provided: %v", err.Error())
	}

	c, err := influx.NewClient(influx.Config{
		URL:      cfg.URL,
		Username: cfg.Username,
		Password: cfg.Password,
	})

	if err != nil {
		return nil, fmt.Errorf("Error initializing influxdb client: %v", err.Error())
	}

	return &influxStorage{
		client: c,
		config: cfg,
	}, nil
}

// Adds multiple data points for multiple data sources
// data is a map where keys are data source ids
func (s *influxStorage) submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error {
	for id, dps := range data {
		points := []influx.Point{}

		for _, dp := range dps {
			var (
				timestamp time.Time
				tags      map[string]string
				fields    map[string]interface{}
			)
			// tags
			tags = make(map[string]string)
			tags["dsName"] = dp.Name // must be the same as sources[id].Resource
			tags["dsID"] = sources[id].ID
			if dp.Units != "" {
				tags["units"] = dp.Units
			}

			// fields
			fields = make(map[string]interface{})
			if dp.Value != nil {
				fields["value"] = *dp.Value
			}
			if dp.StringValue != nil {
				fields["stringValue"] = *dp.StringValue
			}
			if dp.BooleanValue != nil {
				fields["booleanValue"] = *dp.BooleanValue
			}

			// timestamp
			if dp.Time == 0 {
				timestamp = time.Now()
			} else {
				timestamp = time.Unix(dp.Time, 0)
			}

			pt := influx.Point{
				Name:      seriesFromURL(sources[id].Resource),
				Tags:      tags,
				Fields:    fields,
				Timestamp: timestamp,
				Precision: "s",
			}
			points = append(points, pt)
		}
		bps := influx.BatchPoints{
			Points:          points,
			Database:        s.config.Database,
			RetentionPolicy: sources[id].Retention.Policy,
		}
		_, err := s.client.Write(bps)
		if err != nil {
			return err
		}
	}
	return nil
}

// Retrieves last data point of every data source
func (s *influxStorage) getLast(ds ...registry.DataSource) (int, DataSet, error) {
	// TODO
	return 0, DataSet{}, nil
}

// Queries data for specified data sources
func (s *influxStorage) query(q query, ds ...registry.DataSource) (int, DataSet, error) {
	// TODO
	return 0, DataSet{}, nil
}
