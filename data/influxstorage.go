// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/influxdata/influxdb/models"
	influx "github.com/influxdb/influxdb/client/v2"
)

// InfluxStorage implements a simple data storage back-end with SQLite
type InfluxStorage struct {
	client influx.Client
	config *InfluxStorageConfig
}

// NewInfluxStorage returns a new Storage given a configuration
func NewInfluxStorage(DSN string) (*InfluxStorage, chan<- common.Notification, error) {
	cfg, err := initInfluxConf(DSN)
	if err != nil {
		return nil, nil, logger.Errorf("Influx config error: %s", err)
	}
	cfg.Replication = 1

	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     cfg.DSN,
		Username: cfg.Username,
		Password: cfg.Password,
	})
	if err != nil {
		return nil, nil, logger.Errorf("Error initializing influxdb client: %s", err)
	}

	s := &InfluxStorage{
		client: c,
		config: cfg,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListener(s, ntChan)

	return s, ntChan, nil
}

// Formatted measurement name for a given data source
func (s *InfluxStorage) Msrmt(ds registry.DataSource) string {
	return fmt.Sprintf("data_%s", ds.ID)
}

// Formatted retention policy name for a given data source
func (s *InfluxStorage) Retention(ds registry.DataSource) string {
	return fmt.Sprintf("policy_%s", ds.ID)
}

// Fully qualified measurement name
func (s *InfluxStorage) FQMsrmt(ds registry.DataSource) string {
	return fmt.Sprintf("%s.\"%s\".\"%s\"", s.config.Database, s.Retention(ds), s.Msrmt(ds))
}

// The field-name for HDS data types
func (s *InfluxStorage) FieldForType(t string) string {
	switch t {
	case common.FLOAT:
		return "value"
	case common.STRING:
		return "stringValue"
	case common.BOOL:
		return "booleanValue"
	}
	return ""
}

// Database name
func (s *InfluxStorage) Database() string {
	return s.config.Database
}

// Influx Replication
func (s *InfluxStorage) Replication() int {
	return s.config.Replication
}

// Adds multiple data points for multiple data sources
// data is a map where keys are data source ids
func (s *InfluxStorage) Submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error {
	for id, dps := range data {

		bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
			Database:        s.config.Database,
			Precision:       "ms",
			RetentionPolicy: s.Retention(sources[id]),
		})
		if err != nil {
			return logger.Errorf("%s", err)
		}
		for _, dp := range dps {
			var (
				timestamp time.Time
				tags      map[string]string
				fields    map[string]interface{}
			)
			// tags
			tags = make(map[string]string)
			tags["name"] = dp.Name // must be the same as sources[id].Resource
			//tags["id"] = sources[id].ID
			if dp.Units != "" {
				tags["units"] = dp.Units
			}

			// fields
			fields = make(map[string]interface{})
			// The "value", "stringValue", and "booleanValue" fields MUST NOT appear together.
			if dp.Value != nil {
				fields["value"] = *dp.Value
			} else if dp.StringValue != nil && *dp.StringValue != "" {
				fields["stringValue"] = *dp.StringValue
			} else if dp.BooleanValue != nil {
				fields["booleanValue"] = *dp.BooleanValue
			}

			// timestamp
			if dp.Time == 0 {
				timestamp = time.Now()
			} else {
				timestamp = time.Unix(dp.Time, 0)
			}
			pt, err := influx.NewPoint(
				s.Msrmt(sources[id]),
				tags,
				fields,
				timestamp,
			)
			if err != nil {
				return logger.Errorf("Error creating data point for source %v: %s", sources[id].ID, err)
			}
			bp.AddPoint(pt)
		}
		err = s.client.Write(bp)
		if err != nil {
			return logger.Errorf("%s", err)
		}
	}
	return nil
}

func pointsFromRow(r models.Row) ([]DataPoint, error) {
	points := []DataPoint{}

	for _, e := range r.Values {
		p := NewDataPoint()

		// fields and tags
		for i, v := range e {
			// point with nil column
			if v == nil {
				continue
			}
			switch r.Columns[i] {
			case "time":
				if val, ok := v.(string); ok {
					t, err := time.Parse(time.RFC3339, val)
					if err != nil {
						return nil, logger.Errorf("Invalid time format: %v", val)
					}
					p.Time = t.Unix()
				} else {
					return nil, logger.Errorf("Interface conversion error. time not string?")
				}
			case "name":
				if val, ok := v.(string); ok {
					p.Name = val
				} else {
					return nil, logger.Errorf("Interface conversion error. name not string?")
				}
			case "value":
				if val, err := v.(json.Number).Float64(); err == nil {
					p.Value = &val
				} else {
					return nil, logger.Errorf("%s", err)
				}
			case "booleanValue":
				if val, ok := v.(bool); ok {
					p.BooleanValue = &val
				} else {
					return nil, logger.Errorf("Interface conversion error. booleanValue not bool?")
				}
			case "stringValue":
				if val, ok := v.(string); ok {
					p.StringValue = &val
				} else {
					return nil, logger.Errorf("Interface conversion error. stringValue not string?")
				}
			case "units":
				if val, ok := v.(string); ok {
					p.Units = val
				} else {
					return nil, logger.Errorf("Interface conversion error. units not string?")
				}
			} // endswitch
		}
		points = append(points, p)
	}

	return points, nil
}

// Queries data for specified data sources
func (s *InfluxStorage) Query(q Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error) {
	points := []DataPoint{}
	total := 0

	// Set minimum time to 1970-01-01T00:00:00Z
	if q.Start.Before(time.Unix(0, 0)) {
		q.Start = time.Unix(0, 0)
		if q.End.Before(time.Unix(0, 1)) {
			return NewDataSet(), 0, logger.Errorf("%s argument must be greater than 1970-01-01T00:00:00Z", common.ParamEnd)
		}
	}

	// If q.End is not set, make the query open-ended
	var timeCond string
	if q.Start.Before(q.End) {
		timeCond = fmt.Sprintf("time > '%s' AND time < '%s'", q.Start.Format(time.RFC3339), q.End.Format(time.RFC3339))
	} else {
		timeCond = fmt.Sprintf("time > '%s'", q.Start.Format(time.RFC3339))
	}

	perItems, offsets := common.PerItemPagination(q.Limit, page, perPage, len(sources))

	// Initialize sort order
	sort := "DESC"
	if q.Sort == common.ASC {
		sort = "ASC"
	}

	for i, ds := range sources {
		// Count total
		count, err := s.CountSprintf("SELECT COUNT(%s) FROM %s WHERE %s",
			s.FieldForType(ds.Type), s.FQMsrmt(ds), timeCond)
		if err != nil {
			return NewDataSet(), 0, logger.Errorf("Error counting records for source %v: %s", ds.Resource, err)
		}
		if count < 1 {
			//logger.Printf("There is no data for source %v", ds.Resource)
			continue
		}
		total += int(count)

		res, err := s.QuerySprintf("SELECT * FROM %s WHERE %s ORDER BY time %s LIMIT %d OFFSET %d",
			s.FQMsrmt(ds), timeCond, sort, perItems[i], offsets[i])
		if err != nil {
			return NewDataSet(), 0, logger.Errorf("Error retrieving a data point for source %v: %s", ds.Resource, err)
		}

		if len(res[0].Series) > 1 {
			return NewDataSet(), 0, logger.Errorf("Unrecognized/Corrupted database schema.")
		}

		pds, err := pointsFromRow(res[0].Series[0])
		if err != nil {
			return NewDataSet(), 0, logger.Errorf("Error parsing points for source %v: %s", ds.Resource, err)
		}

		if perItems[i] != 0 { // influx ignores `limit 0`
			points = append(points, pds...)
		}
	}
	dataset := NewDataSet()
	dataset.Entries = points

	// q.Limit overrides total
	if q.Limit > 0 && q.Limit < total {
		total = q.Limit
	}

	return dataset, total, nil
}

// Handles the creation of a new data source
func (s *InfluxStorage) NtfCreated(ds registry.DataSource, callback chan error) {

	duration := "INF"
	if ds.Retention != "" {
		duration = ds.Retention
	}
	_, err := s.QuerySprintf("CREATE RETENTION POLICY \"%s\" ON %s DURATION %v REPLICATION %d",
		s.Retention(ds), s.config.Database, duration, s.config.Replication)
	if err != nil {
		callback <- logger.Errorf("Error creating retention policy: %s", err)
		return
	}
	logger.Println("InfluxStorage: created retention policy for", ds.ID)

	callback <- nil
}

// Handles updates of a data source
func (s *InfluxStorage) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {

	if oldDS.Retention != newDS.Retention {
		duration := "INF"
		if newDS.Retention != "" {
			duration = newDS.Retention
		}
		// Setting SHARD DURATION 0s tells influx to use the default duration
		// https://docs.influxdata.com/influxdb/v1.2/query_language/database_management/#retention-policy-management
		_, err := s.QuerySprintf("ALTER RETENTION POLICY \"%s\" ON %s DURATION %v SHARD DURATION 0s", s.Retention(oldDS), s.config.Database, duration)
		if err != nil {
			callback <- logger.Errorf("Error modifying the retention policy for source: %s", err)
			return
		}
		logger.Println("InfluxStorage: altered retention policy for", oldDS.ID)
	}
	callback <- nil
}

// Handles deletion of a data source
func (s *InfluxStorage) NtfDeleted(ds registry.DataSource, callback chan error) {

	_, err := s.QuerySprintf("DROP MEASUREMENT \"%s\"", s.Msrmt(ds))
	if err != nil {
		if strings.Contains(err.Error(), "measurement not found") {
			// Not an error, No data to delete.
			goto DROP_RETENTION
		}
		callback <- logger.Errorf("Error removing the historical data: %s", err)
		return
	}
	logger.Println("InfluxStorage: dropped measurements for", ds.ID)

DROP_RETENTION:
	_, err = s.QuerySprintf("DROP RETENTION POLICY \"%s\" ON %s", s.Retention(ds), s.config.Database)
	if err != nil {
		callback <- logger.Errorf("Error removing the retention policy for source: %s", err)
		return
	}
	logger.Println("InfluxStorage: dropped retention policy for", ds.ID)

	callback <- nil
}

// Query influxdb
func (s *InfluxStorage) QuerySprintf(format string, a ...interface{}) (res []influx.Result, err error) {
	logger.Debugln("Influx:", fmt.Sprintf(format, a...))
	q := influx.Query{
		Command:  fmt.Sprintf(format, a...),
		Database: s.config.Database,
	}
	response, err := s.client.Query(q)
	if err != nil {
		return res, logger.Errorf("Request error: %v", err)
	}
	if response.Error() != nil {
		return res, logger.Errorf("Statement error: %v", response.Error())
	}

	return response.Results, nil
}

func (s *InfluxStorage) CountSprintf(format string, a ...interface{}) (int64, error) {
	res, err := s.QuerySprintf(format, a...)
	if err != nil {
		return 0, logger.Errorf("%s", err)
	}

	if len(res) < 1 {
		return 0, logger.Errorf("Unable to get count from database: response empty")
	}
	if len(res[0].Series) < 1 {
		// No data
		return 0, nil
	}
	if len(res[0].Series[0].Values) < 1 ||
		len(res[0].Series[0].Values[0]) < 2 {
		return 0, logger.Errorf("Unable to get count from database: bad response")
	}
	count, err := res[0].Series[0].Values[0][1].(json.Number).Int64()
	if err != nil {
		return 0, logger.Errorf("Unable to parse count from database response.")
	}
	return count, nil
}

// InfluxStorageConfig configuration

type InfluxStorageConfig struct {
	DSN         string
	Database    string
	Username    string
	Password    string
	Replication int
}

func initInfluxConf(DSN string) (*InfluxStorageConfig, error) {
	// Parse config's DSN string
	PDSN, err := url.Parse(DSN)
	if err != nil {
		return nil, logger.Errorf("%s", err)
	}
	// Validate
	if PDSN.Host == "" {
		return nil, logger.Errorf("Influxdb config: host:port in the URL must be not empty")
	}
	if PDSN.Path == "" {
		return nil, logger.Errorf("Influxdb config: db must be not empty")
	}

	var c InfluxStorageConfig
	c.DSN = fmt.Sprintf("%v://%v", PDSN.Scheme, PDSN.Host)
	c.Database = strings.Trim(PDSN.Path, "/")
	// Optional username and password
	if PDSN.User != nil {
		c.Username = PDSN.User.Username()
		c.Password, _ = PDSN.User.Password()
	}

	return &c, nil
}
