package data

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	influx "github.com/influxdb/influxdb/client/v2"
	"github.com/influxdb/influxdb/models"

	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
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
		return nil, nil, fmt.Errorf("Influx config error: %v", err.Error())
	}
	cfg.Replication = 1

	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     cfg.DSN,
		Username: cfg.Username,
		Password: cfg.Password,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("Error initializing influxdb client: %v", err.Error())
	}

	s := &InfluxStorage{
		client: c,
		config: cfg,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go ntListener(s, ntChan)

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
			return err
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
				return fmt.Errorf("Error creating data point for source %v: %v", sources[id].ID, err.Error())
			}
			bp.AddPoint(pt)
		}
		err = s.client.Write(bp)
		if err != nil {
			return err
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
						return nil, fmt.Errorf("Invalid time format:", val)
					}
					p.Time = t.Unix()
				} else {
					return nil, errors.New("Interface conversion error. time not string?")
				}
			case "name":
				if val, ok := v.(string); ok {
					p.Name = val
				} else {
					return nil, errors.New("Interface conversion error. name not string?")
				}
			case "value":
				if val, err := v.(json.Number).Float64(); err == nil {
					p.Value = &val
				} else {
					return nil, err
				}
			case "booleanValue":
				if val, ok := v.(bool); ok {
					p.BooleanValue = &val
				} else {
					return nil, errors.New("Interface conversion error. booleanValue not bool?")
				}
			case "stringValue":
				if val, ok := v.(string); ok {
					p.StringValue = &val
				} else {
					return nil, errors.New("Interface conversion error. stringValue not string?")
				}
			case "units":
				if val, ok := v.(string); ok {
					p.Units = val
				} else {
					return nil, errors.New("Interface conversion error. units not string?")
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
	//perEach := perPage / len(sources)

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
		res, err := s.QuerySprintf("SELECT * FROM %s WHERE %s ORDER BY time %s LIMIT %d OFFSET %d",
			s.FQMsrmt(ds), timeCond, sort, perItems[i], offsets[i])
		if err != nil {
			return NewDataSet(), 0, fmt.Errorf("Error retrieving a data point for source %v: %v", ds.Resource, err.Error())
		}
		if len(res) < 1 || len(res[0].Series) < 1 {
			log.Printf("There is no data for source %v", ds.Resource)
			continue
		}

		if len(res[0].Series) > 1 {
			return NewDataSet(), 0, fmt.Errorf("Unrecognized/Corrupted database schema.")
		}

		pds, err := pointsFromRow(res[0].Series[0])
		if err != nil {
			return NewDataSet(), 0, fmt.Errorf("Error parsing points for source %v: %v", ds.Resource, err.Error())
		}

		// Count total
		res, err = s.QuerySprintf("SELECT COUNT(value)+COUNT(stringValue)+COUNT(booleanValue) FROM %s WHERE %s",
			s.FQMsrmt(ds), timeCond)
		if err != nil {
			return NewDataSet(), 0, fmt.Errorf("Error counting records for source %v: %v", ds.Resource, err.Error())
		}
		if len(res) < 1 ||
			len(res[0].Series) < 1 ||
			len(res[0].Series[0].Values) < 1 ||
			len(res[0].Series[0].Values[0]) < 2 {
			return NewDataSet(), 0, fmt.Errorf("Unable to count records for source %v", ds.Resource)
		}
		count, err := res[0].Series[0].Values[0][1].(json.Number).Int64()
		if err != nil {
			log.Println(err.Error())
		}
		total += int(count)

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
func (s *InfluxStorage) ntfCreated(ds registry.DataSource, callback chan error) {

	duration := "INF"
	if ds.Retention != "" {
		duration = ds.Retention
	}
	_, err := s.QuerySprintf("CREATE RETENTION POLICY \"%s\" ON %s DURATION %v REPLICATION %d",
		s.Retention(ds), s.config.Database, duration, s.config.Replication)
	if err != nil {
		callback <- fmt.Errorf("Error creating retention policy: %v", err.Error())
		return
	}
	log.Println("InfluxStorage: created retention policy for", ds.ID)

	callback <- nil
}

// Handles updates of a data source
func (s *InfluxStorage) ntfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {

	if oldDS.Retention != newDS.Retention {
		duration := "INF"
		if newDS.Retention != "" {
			duration = newDS.Retention
		}
		_, err := s.QuerySprintf("ALTER RETENTION POLICY \"%s\" ON %s DURATION %v", s.Retention(oldDS), s.config.Database, duration)
		if err != nil {
			callback <- fmt.Errorf("Error modifying the retention policy for source: %v", err.Error())
			return
		}
		log.Println("InfluxStorage: altered retention policy for", oldDS.ID)
	}
	callback <- nil
}

// Handles deletion of a data source
func (s *InfluxStorage) ntfDeleted(ds registry.DataSource, callback chan error) {

	_, err := s.QuerySprintf("DROP MEASUREMENT \"%s\"", s.Msrmt(ds))
	if err != nil {
		if strings.Contains(err.Error(), "measurement not found") {
			// Not an error, No data to delete.
			goto DROP_RETENTION
		}
		callback <- fmt.Errorf("Error removing the historical data: %v", err.Error())
		return
	}
	log.Println("InfluxStorage: dropped measurements for", ds.ID)

DROP_RETENTION:
	_, err = s.QuerySprintf("DROP RETENTION POLICY \"%s\" ON %s", s.Retention(ds), s.config.Database)
	if err != nil {
		callback <- fmt.Errorf("Error removing the retention policy for source: %v", err.Error())
		return
	}
	log.Println("InfluxStorage: dropped retention policy for", ds.ID)

	callback <- nil
}

// Query influxdb
func (s *InfluxStorage) QuerySprintf(format string, a ...interface{}) (res []influx.Result, err error) {
	log.Println("Influx:", fmt.Sprintf(format, a...))
	q := influx.Query{
		Command:  fmt.Sprintf(format, a...),
		Database: s.config.Database,
	}
	if response, err := s.client.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
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
		return nil, err
	}
	// Validate
	if PDSN.Host == "" {
		return nil, fmt.Errorf("Influxdb config: host:port in the URL must be not empty")
	}
	if PDSN.Path == "" {
		return nil, fmt.Errorf("Influxdb config: db must be not empty")
	}

	password, _ := PDSN.User.Password()
	c := &InfluxStorageConfig{
		DSN:      fmt.Sprintf("%v://%v", PDSN.Scheme, PDSN.Host),
		Database: strings.Trim(PDSN.Path, "/"),
		Username: PDSN.User.Username(),
		Password: password,
	}

	return c, nil
}
