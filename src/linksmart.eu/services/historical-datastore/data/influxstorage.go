package data

import (
	"encoding/json"
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

// influxStorage implements a simple data storage back-end with SQLite
type influxStorage struct {
	client influx.Client
	config *InfluxStorageConfig
}

// NewInfluxStorage returns a new Storage given a configuration
func NewInfluxStorage(DSN string) (Storage, chan<- common.Notification, error) {
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

	s := &influxStorage{
		client: c,
		config: cfg,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go s.ntListener(ntChan)

	return s, ntChan, nil
}

// Returns the influxdb measurement for a given data source
func (s *influxStorage) msrmtBySource(ds registry.DataSource, fullyQualify bool) string {
	h := strings.Replace(ds.ParsedResource().Host, ".", "_", -1)
	p := strings.Replace(ds.ParsedResource().Path, "/", "_", -1)
	p = strings.Replace(p, ".", "_", -1)

	msrmt := fmt.Sprintf("hds_data_%s%s", h, p)
	if fullyQualify && ds.Retention != "" {
		msrmt = fmt.Sprintf("%s.\"%s\".%s", s.config.Database, ds.Retention, msrmt)
	}

	return msrmt
}

func pointsFromRow(r models.Row) ([]DataPoint, error) {
	var name, units string
	var ok bool
	points := []DataPoint{}

	// resource name
	if name, ok = r.Tags["name"]; !ok {
		return nil, fmt.Errorf("Empty data source name tag")
	}
	// (shared) units
	units, _ = r.Tags["units"]

	// individual points (values)
	for _, e := range r.Values {
		fmt.Println("value", e)
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
			case "stringValue":
				val := v.(string)
				p.StringValue = &val
			case "time":
				val := v.(string)
				t, err := time.Parse(time.RFC3339, val)
				if err != nil {
					fmt.Println("pointsFromRow()", err.Error())
					continue
				}
				p.Time = t.Unix()
			}
		}
		points = append(points, p)
	}
	return points, nil
}

// Returns n last points for a given DataSource
func (s *influxStorage) getLastPoints(ds registry.DataSource, n int) ([]DataPoint, error) {
	q := influx.Query{
		Command:  fmt.Sprintf("SELECT * FROM %s WHERE id='%s' GROUP BY * ORDER BY time DESC LIMIT %d", s.msrmtBySource(ds, true), ds.ID, n),
		Database: s.config.Database,
	}
	fmt.Println(q)

	res, err := s.client.Query(q)
	if err != nil {
		return []DataPoint{}, err
	}
	if res.Error() != nil {
		return []DataPoint{}, res.Error()
	}

	if len(res.Results) < 1 || len(res.Results[0].Series) < 1 {
		log.Panicln("No data records.")
		return []DataPoint{}, nil // no error but also no data
	}

	// There can be a case where there is more than 1 series matching the query
	// e.g., if someone messed with the data outside of the HDS API
	points, err := pointsFromRow(res.Results[0].Series[0])
	if err != nil {
		fmt.Println("pointsFromRow error", err.Error())
		return []DataPoint{}, err
	}

	return points, nil
}

// echo '{"e":[{ "n": "string", "sv":"hello2" }]}' | http post http://localhost:8085/data/33bc308c-bac5-4e78-b97f-4de9b06f3b27 Content-Type:'application/senml+json'

// Adds multiple data points for multiple data sources
// data is a map where keys are data source ids
func (s *influxStorage) Submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error {
	for id, dps := range data {

		bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
			Database:        s.config.Database,
			Precision:       "ms",
			RetentionPolicy: sources[id].Retention,
		})
		if err != nil {
			fmt.Println(err.Error())
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
			tags["id"] = sources[id].ID
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
			pt, err := influx.NewPoint(
				s.msrmtBySource(sources[id], false),
				tags,
				fields,
				timestamp,
			)
			if err != nil {
				fmt.Println(err.Error())
				continue
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

// Retrieves last data point of every data source
func (s *influxStorage) GetLast(sources ...registry.DataSource) (DataSet, error) {
	points := []DataPoint{}
	for _, ds := range sources {

		pds, err := s.getLastPoints(ds, 1)
		if err != nil {
			log.Printf("Error retrieving a data point for source %v: %v", ds.Resource, err.Error())
			continue
		}
		if len(pds) < 1 {
			log.Printf("There is no data for source %v", ds.Resource)
			continue
		}
		points = append(points, pds[0])
	}
	dataset := NewDataSet()
	dataset.Entries = points
	return dataset, nil
}

// Queries data for specified data sources
func (s *influxStorage) Query(q Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error) {
	points := []DataPoint{}
	total := 0
	perEach := perPage / len(sources)

	// If q.End is not set, make the query open-ended
	var timeQry string
	if q.Start.Before(q.End) {
		timeQry = fmt.Sprintf("time > '%s' AND time < '%s'", q.Start.Format(time.RFC3339), q.End.Format(time.RFC3339))
	} else {
		timeQry = fmt.Sprintf("time > '%s'", q.Start.Format(time.RFC3339))
	}

	// NOTE: clarify relation between limit and perPage
	for _, ds := range sources {
		q := influx.Query{
			Command: fmt.Sprintf("SELECT * FROM %s WHERE id='%s' AND %s GROUP BY * LIMIT %d",
				s.msrmtBySource(ds, true), ds.ID, timeQry, perEach),
			Database: s.config.Database,
		}

		res, err := s.client.Query(q)
		if err != nil {
			log.Printf("Error retrieving a data point for source %v: %v", ds.Resource, err.Error())
			continue
		}
		if res.Error() != nil || len(res.Results) < 1 || len(res.Results[0].Series) < 1 {
			log.Printf("There is no data for source %v", ds.Resource)
			continue
		}

		// There can be a case where there is more than 1 series matching the query
		// e.g., if someone messed with the data outside of the HDS API
		pds, err := pointsFromRow(res.Results[0].Series[0])
		if err != nil {
			log.Printf("Error parsing points for source %v: %v", ds.Resource, err.Error())
			continue
		}
		points = append(points, pds...)
	}
	dataset := NewDataSet()
	dataset.Entries = points
	return dataset, total, nil

	return NewDataSet(), 0, nil
}

// Handles the creation of a new data source
func (s *influxStorage) ntfCreated(ds *registry.DataSource) {
	fmt.Println("[nt] created: ", ds.ID)
	if ds.Retention != "" {
		q := influx.Query{
			Command: fmt.Sprintf("CREATE RETENTION POLICY \"%s\" ON %s DURATION %v REPLICATION %d",
				ds.Retention, s.config.Database, ds.Retention, s.config.Replication),
		}
		res, err := s.client.Query(q)
		if err != nil {
			log.Printf("Error creating retention policy: %v", err.Error())
		}
		if res.Error() != nil {
			log.Printf("Error creating retention policy: %v", res.Error())
		}
	}
}

// Handles updates of a data source
func (s *influxStorage) ntfUpdated(ds *registry.DataSource) {
	fmt.Println("[nt] updated: ", ds.ID)
}

// Handles deletion of a data source
func (s *influxStorage) ntfDeleted(ds *registry.DataSource) {
	fmt.Println("[nt] deleted: ", ds.ID)
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
		DSN:      DSN,
		Database: strings.Trim(PDSN.Path, "/"),
		Username: PDSN.User.Username(),
		Password: password,
	}

	return c, nil
}
