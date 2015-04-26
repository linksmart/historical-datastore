package data

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"log"

	influx "linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/influxdb/influxdb/client"
	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/influxdb/influxdb/influxql"
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

// Returns the influxdb measurement for a given data source
func msrmtBySource(ds registry.DataSource) string {
	h := strings.Replace(ds.Resource.Host, ".", "_", -1)
	p := strings.Replace(ds.Resource.Path, "/", "_", -1)
	return fmt.Sprintf("hds_data_%s%s", h, p)
}

func pointsFromRow(r influxql.Row) ([]DataPoint, error) {
	var name, units string
	var ok bool
	points := []DataPoint{}

	// (shared) name
	if name, ok = r.Tags["dsName"]; !ok {
		return nil, fmt.Errorf("Empty data source name tag")
	}
	// (shared) units
	units, _ = r.Tags["units"]

	// individual points (values)
	for _, e := range r.Values {
		p := NewDataPoint()
		p.Name = name
		p.Units = units

		// values
		for i, v := range e {
			// point with nil column
			if v == nil {
				continue
			}
			switch r.Columns[i] {
			case "value":
				val, _ := v.(json.Number).Float64()
				p.Value = &val
			case "booleanValue":
				val := v.(bool)
				p.BooleanValue = &val
			case "srtringValue":
				val := v.(string)
				p.StringValue = &val
			case "time":
				val := v.(string)
				t, err := time.Parse(time.RFC3339, val)
				if err != nil {
					continue
				}
				p.Time = t.Unix()
			}
		}
		points = append(points, p)
	}
	return points, nil
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

// Returns n last points for a given DataSource
func (s *influxStorage) getLastPoints(ds registry.DataSource, n int) ([]DataPoint, error) {
	msrmt := msrmtBySource(ds)
	q := influx.Query{
		Command:  fmt.Sprintf("SELECT * FROM %s WHERE dsID='%s' GROUP BY * LIMIT %d", msrmt, ds.ID, n),
		Database: s.config.Database,
	}

	res, err := s.client.Query(q)
	if err != nil {
		return []DataPoint{}, err
	}
	if res.Error() != nil {
		return []DataPoint{}, res.Error()
	}

	if len(res.Results) < 1 || len(res.Results[0].Series) < 1 {
		return []DataPoint{}, nil // no error but also no data
	}

	// There can be a case where there is more than 1 series matching the query
	// e.g., if someone messed with the data outside of the HDS API
	points, err := pointsFromRow(res.Results[0].Series[0])
	if err != nil {
		return []DataPoint{}, err
	}

	return points, nil
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
				Name:      msrmtBySource(sources[id]),
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
func (s *influxStorage) getLast(sources ...registry.DataSource) (DataSet, error) {
	points := []DataPoint{}
	for _, ds := range sources {
		pds, err := s.getLastPoints(ds, 1)
		if err != nil {
			log.Printf("Error retrieving a data point for source %v: %v", ds.Resource.String(), err.Error())
			continue
		}
		if len(pds) < 1 {
			log.Printf("There is no data for source %v", ds.Resource.String())
			continue
		}
		points = append(points, pds[0])
	}
	dataset := NewDataSet()
	dataset.Entries = points
	return dataset, nil
}

// Queries data for specified data sources
func (s *influxStorage) query(q query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error) {
	points := []DataPoint{}
	total := 0
	perEach := perPage / len(sources)

	// NOTE: clarify relation between limit and perPage
	for _, ds := range sources {
		msrmt := msrmtBySource(ds)
		q := influx.Query{
			Command: fmt.Sprintf("SELECT * FROM %s WHERE dsID='%s' AND time > '%s' AND time < '%s' GROUP BY * LIMIT %d",
				msrmt, ds.ID, q.start.Format(time.RFC3339), q.end.Format(time.RFC3339), perEach),
			Database: s.config.Database,
		}

		res, err := s.client.Query(q)
		if err != nil {
			log.Printf("Error retrieving a data point for source %v: %v", ds.Resource.String(), err.Error())
			continue
		}
		if res.Error() != nil || len(res.Results) < 1 || len(res.Results[0].Series) < 1 {
			log.Printf("There is no data for source %v", ds.Resource.String())
			continue
		}

		// There can be a case where there is more than 1 series matching the query
		// e.g., if someone messed with the data outside of the HDS API
		pds, err := pointsFromRow(res.Results[0].Series[0])
		if err != nil {
			log.Printf("Error parsing points for source %v: %v", ds.Resource.String(), err.Error())
			continue
		}
		points = append(points, pds...)
	}
	dataset := NewDataSet()
	dataset.Entries = points
	return dataset, total, nil
}
