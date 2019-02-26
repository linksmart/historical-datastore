// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/farshidtz/senml"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

const influxPingTimeout = 30 * time.Second

// InfluxStorage implements a InfluxDB storage client for HDS Data API
type InfluxStorage struct {
	client           influx.Client
	config           influxStorageConfig
	retentionPeriods []string
	prepare          sync.WaitGroup
}

// NewInfluxStorage returns a new InfluxStorage
func NewInfluxStorage(conf common.DataConf, retentionPeriods []string) (*InfluxStorage, error) {
	return nil, fmt.Errorf("Not implemented!!")
}

// Submit adds multiple data points for multiple data sources
// data is a map where keys are data source ids
func (s *InfluxStorage) Submit(data map[string]senml.Pack, sources map[string]*registry.DataStream) error {

	return fmt.Errorf("Not implemented!!")
}

func (s *InfluxStorage) Query(q Query, sources ...*registry.DataStream) (senml.Pack, int, *time.Time, error) {
	return nil, 0, nil, fmt.Errorf("Not implemented!!")
}

// CreateHandler handles the creation of a new data source
func (s *InfluxStorage) CreateHandler(ds registry.DataStream) error {
	s.prepare.Wait()
	return nil
}

// UpdateHandler handles updates of a data source
func (s *InfluxStorage) UpdateHandler(oldDS registry.DataStream, newDS registry.DataStream) error {
	s.prepare.Wait()

	return fmt.Errorf("not implemented!!")
}

// DeleteHandler handles deletion of a data source
func (s *InfluxStorage) DeleteHandler(ds registry.DataStream) error {
	s.prepare.Wait()

	return fmt.Errorf("not implemented!!")
}

// UTILITY FUNCTIONS

// QuerySprintf constructs a query for influxdb
func (s *InfluxStorage) QuerySprintf(format string, a ...interface{}) (res []influx.Result, err error) {
	//log.Println("Influx:", fmt.Sprintf(format, a...))
	q := influx.Query{
		Command:  fmt.Sprintf(format, a...),
		Database: s.config.database,
	}
	response, err := s.client.Query(q)
	if err != nil {
		return res, fmt.Errorf("Request error: %v", err)
	}
	if response.Error() != nil {
		return res, fmt.Errorf("Statement error: %v", response.Error())
	}

	return response.Results, nil
}

// CountSprintf constructs a counting query for influxdb
func (s *InfluxStorage) CountSprintf(format string, a ...interface{}) (int64, error) {
	res, err := s.QuerySprintf(format, a...)
	if err != nil {
		return 0, err
	}

	if len(res) < 1 {
		return 0, fmt.Errorf("Unable to get count from database: response empty")
	}
	if len(res[0].Series) < 1 {
		// No data
		return 0, nil
	}
	if len(res[0].Series[0].Values) < 1 ||
		len(res[0].Series[0].Values[0]) < 2 {
		return 0, fmt.Errorf("Unable to get count from database: bad response")
	}
	count, err := res[0].Series[0].Values[0][1].(json.Number).Int64()
	if err != nil {
		return 0, fmt.Errorf("Unable to parse count from database response.")
	}
	return count, nil
}

//
func (s *InfluxStorage) ChangeRetentionPolicy(measurement, countField, oldRP, newRP string) error {
	/*count, err := s.CountSprintf("SELECT COUNT(%s) FROM %s GROUP BY *",
		countField, s.MeasurementNameFQ(oldRP, measurement))
	if err != nil {
		return fmt.Errorf("Error counting historical data: %s", err)
	}
	if count == 0 {
		// no data to move
		return nil
	}

	retention, err := s.ParseDuration(newRP)
	if err != nil {
		return fmt.Errorf("Error parsing retention period: %s: %s", newRP, err)
	}
	retention -= time.Minute // reduce 1m to avoid overshooting the new RP

	// formatting functions
	measurementNameTemp := func(uuid string) string {
		return fmt.Sprintf("temp_%s", uuid)
	}
	measurementNameTempFQ := func(uuid string) string {
		return fmt.Sprintf("%s.\"%s\".\"temp_%s\"", s.config.database, s.RetentionPolicyName(""), uuid)
	}

	tempUUID := uuid.NewV4().String()

	// Changing retention policy in four steps:
	// 1) keep required data in temp measurement
	_, err = s.QuerySprintf("SELECT * INTO %s FROM %s WHERE time > '%s'",
		measurementNameTempFQ(tempUUID), s.MeasurementNameFQ(oldRP, measurement), time.Now().UTC().Add(-retention).Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("Error moving the historical data to new retention policy: %s", err)
	}
	// 2) delete the data from old measurement (on all RPs)
	_, err = s.QuerySprintf("DELETE FROM \"%s\"", measurement)
	if err != nil {
		return fmt.Errorf("Error removing the historical data: %s", err)
	}
	// 3) move data from temp into new RP
	_, err = s.QuerySprintf("SELECT * INTO %s FROM %s",
		s.MeasurementNameFQ(newRP, measurement), measurementNameTempFQ(tempUUID))
	if err != nil {
		return fmt.Errorf("Error moving the historical data to new retention policy: %s", err)
	}
	// 4) drop temp
	_, err = s.QuerySprintf("DROP MEASUREMENT \"%s\"", measurementNameTemp(tempUUID))
	if err != nil {
		if strings.Contains(err.Error(), "measurement not found") {
			// Not an error, No data to delete.
			return nil
		}
		return fmt.Errorf("Error removing the historical data: %s", err)
	}*/
	return fmt.Errorf("Not implemented!!")
}

func (s *InfluxStorage) ParseDuration(durationStr string) (time.Duration, error) {
	if durationStr == "" {
		return time.Since(time.Unix(0, 0)), nil
	}
	return influxql.ParseDuration(durationStr)
}

type influxStorageConfig struct {
	dsn         string
	database    string
	username    string
	password    string
	replication int
}

// initInfluxConf initializes the influxdb configuration
func initInfluxConf(DSN string) (*influxStorageConfig, error) {
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

	var c influxStorageConfig
	c.dsn = fmt.Sprintf("%v://%v", PDSN.Scheme, PDSN.Host)
	c.database = strings.Trim(PDSN.Path, "/")
	// Optional username and password
	if PDSN.User != nil {
		c.username = PDSN.User.Username()
		c.password, _ = PDSN.User.Password()
	}

	return &c, nil
}

// prepareStorage prepares the backend for storage
func (s *InfluxStorage) prepareStorage() {
	// wait for influxdb
	for interval := 5; ; interval *= 2 {
		if interval >= 60 {
			interval = 60
		}
		_, version, err := s.client.Ping(influxPingTimeout)
		if err != nil {
			log.Printf("InfluxStorage: Unable to reach influxdb backend: %s", err)
			time.Sleep(time.Duration(interval) * time.Second)
			continue
		}
		log.Printf("InfluxStorage: Connected to InfluxDB %s", version)
		break
	}

	// create retention policies
	for _, period := range s.retentionPeriods {
		_, err := s.QuerySprintf("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %v REPLICATION %d",
			s.RetentionPolicyName(period), s.config.database, period, s.config.replication)
		if err != nil {
			// TODO check database before this?
			log.Fatalf("Error creating retention policies: %s", err)
		}
		log.Printf("InfluxStorage: Created retention policy for period: %s", period)
	}

	s.prepare.Done()
}

// MeasurementName returns formatted measurement name for a given data source
func (s *InfluxStorage) MeasurementName(id string) string {
	return fmt.Sprintf("data_%s", id)
}

// MeasurementNameFQ returns formatted fully-qualified measurement name
func (s *InfluxStorage) MeasurementNameFQ(retention, measurementName string) string {
	return fmt.Sprintf("\"%s\".\"%s\".\"%s\"", s.config.database, s.RetentionPolicyName(retention), measurementName)
}

// RetentionPolicyName returns formatted retention policy name for a given period
func (s *InfluxStorage) RetentionPolicyName(period string) string {
	if period == "" {
		return "autogen" // default retention policy name
	}
	return fmt.Sprintf("policy_%s", period)
}

// FieldForType returns the field-name for HDS data types
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

// Database returns database name
func (s *InfluxStorage) Database() string {
	return s.config.database
}

// Replication returns Influxdb Replication factor
func (s *InfluxStorage) Replication() int {
	return s.config.replication
}

// pointsFromRow converts Influxdb rows to HDS data points
func serieToRecords(r models.Row) ([]senml.Record, error) {
	var records []senml.Record

	for _, e := range r.Values {
		var record senml.Record

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
						return nil, fmt.Errorf("Invalid time format: %v", val)
					}
					record.Time = float64(t.UnixNano()) / 1000000000
				} else {
					return nil, fmt.Errorf("Interface conversion error. time not string?")
				}
			case "name":
				if val, ok := v.(string); ok {
					record.Name = val
				} else {
					return nil, fmt.Errorf("Interface conversion error. name not string?")
				}
			case "value":
				if val, err := v.(json.Number).Float64(); err == nil {
					record.Value = &val
				} else {
					return nil, err
				}
			case "booleanValue":
				if val, ok := v.(bool); ok {
					record.BoolValue = &val
				} else {
					return nil, fmt.Errorf("Interface conversion error. booleanValue not bool?")
				}
			case "stringValue":
				if val, ok := v.(string); ok {
					record.StringValue = val
				} else {
					return nil, fmt.Errorf("Interface conversion error. stringValue not string?")
				}
			case "units":
				if val, ok := v.(string); ok {
					record.Unit = val
				} else {
					return nil, fmt.Errorf("Interface conversion error. units not string?")
				}
			} // endswitch
		}
		records = append(records, record)
	}

	return records, nil
}
