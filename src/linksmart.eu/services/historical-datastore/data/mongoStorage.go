package data

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	mgo "gopkg.in/mgo.v2"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"

)

// InfluxStorage implements a simple data storage back-end with SQLite
type MongoStorage struct {
	session mgo.Session
	config *MongoStorageConfig
}

// NewInfluxStorage returns a new Storage given a configuration
func MongoStorage(DSN string) (*MongoStorage, chan<- common.Notification, error) {
	cfg, err := initMongoConf(DSN)
	if err != nil {
		return nil, nil, logger.Errorf("Mongo config error: %s", err)
	}
	cfg.Replication = 1

	csession, err := mgo.DialWithInfo(mgo.DialInfo{
		Addrs:     cfg.DSN,
		Username: cfg.Username,
		Password: cfg.Password,
	})
	if err != nil {
		return nil, nil, logger.Errorf("Error initializing MOngoDB client: %s", err)
	}

	s := &MongoStorage{
		session: csession,
		config: cfg,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListener(s, ntChan)

	return s, ntChan, nil
}

// Formatted measurement name for a given data source
func (s *MongoStorage) Msrmt(ds registry.DataSource) string {
	return fmt.Sprintf("data_%s", ds.ID)
}

// Formatted retention policy name for a given data source
func (s *MongoStorage) Retention(ds registry.DataSource) string {
	return fmt.Sprintf("policy_%s", ds.ID)
}

// Fully qualified measurement name
func (s *MongoStorage) FQMsrmt(ds registry.DataSource) string {
	return fmt.Sprintf("%s.\"%s\".\"%s\"", s.config.Database, s.Retention(ds), s.Msrmt(ds))
}

// The field-name for HDS data types
func (s *MongoStorage) FieldForType(t string) string {
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
func (s *MongoStorage) Database() string {
	return s.config.Database
}

// Influx Replication
func (s *MongoStorage) Replication() int {
	return s.config.Replication
}

// Adds multiple data points for multiple data sources
// data is a map where keys are data source ids
func (s *MongoStorage) Submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error {
	for id,dps := range data{
		session := s.session.Copy()
		defer session.Close()

		collection := session.DB(s.Database()).C(s.Msrmt(sources[id]))

		for _,dp := range dps{
			var timestamp time.Time
			if dp.Time == 0 {
				timestamp = time.Now()
			} else {
				timestamp = time.Unix(dp.Time, 0)
			}
			dp.Time = timestamp
			collection.Insert(dp)
		}


	}

	return nil
}


// Queries data for specified data sources
func (s *MongoStorage) Query(q Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error) {
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

	var queryBson bson.M
	if q.Start.Before(q.End) {
		queryBson = bson.M{
			"Time": bson.M{"$lte":  q.Start.Format(time.RFC3339)},
			"Time":   bson.M{"$gte": q.End.Format(time.RFC3339)},
		}

	} else {
		queryBson = bson.M{
			"Time": bson.M{"$lte":  q.Start.Format(time.RFC3339)},
		}

	}

	perItems, offsets := common.PerItemPagination(q.Limit, page, perPage, len(sources))

	// Initialize sort order
	sort := "-Time"
	if q.Sort == common.ASC {
		sort = "+Time"
	}

	for i, ds := range sources {

		session := s.session.Copy()
		defer session.Close()

		collection  := session.DB(s.Database()).C(s.Msrmt(s.FQMsrmt(ds)))
		var searchResults []DataPoint
		err := collection.Find(queryBson).Sort(sort).Skip(offsets[i]).Limit(perItems[i]).All(&searchResults)

		if err != nil {
			return NewDataSet(), 0, logger.Errorf("Error parsing points for source %v: %s", ds.Resource, err)
		}

		if perItems[i] != 0 {
			points = append(points, searchResults...)
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
func (s *MongoStorage) NtfCreated(ds registry.DataSource, callback chan error) {


	if ds.Retention != "" {
		duration := ds.Retention

		session := s.session.Copy()
		defer session.Close()

		collection  := session.DB(s.Database()).C(s.Msrmt(s.FQMsrmt(ds)))

		TTL := mgo.Index{
			Key:         []string{"Time"},
			Unique:      false,
			DropDups:    false,
			Background:  true,
			ExpireAfter: duration} // one hour
		err := collection.EnsureIndex(TTL)
		if err != nil {
			callback <- logger.Errorf("Error modifying the retention policy for source: %s", err)
			return
		}
		logger.Println("MongoStorage: created retention policy for", ds.ID)
	}
	callback <- nil
}

// Handles updates of a data source
func (s *MongoStorage) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {

	if oldDS.Retention != newDS.Retention {
		duration := newDS.Retention

		session := s.session.Copy()
		defer session.Close()

		collection  := session.DB(s.Database()).C(s.Msrmt(s.FQMsrmt(newDS)))

		TTL := mgo.Index{
			Key:         []string{"Time"},
			Unique:      false,
			DropDups:    false,
			Background:  true,
			ExpireAfter: duration} // one hour
		err := collection.EnsureIndex(TTL)
		if err != nil {
			callback <- logger.Errorf("Error modifying the retention policy for source: %s", err)
			return
		}
		logger.Println("MongoStorage: altered retention policy for", oldDS.ID)
	}
	callback <- nil
}

// Handles deletion of a data source
func (s *MongoStorage) NtfDeleted(ds registry.DataSource, callback chan error) {

	session := s.session.Copy()
	defer session.Close()

	collection  := session.DB(s.Database()).C(s.Msrmt(s.FQMsrmt(ds)))

	_,err := collection.RemoveAll(nil)

	if err != nil {
		callback <- logger.Errorf("Error removing the historical data: %s", err)
		return
	}
	callback <- nil
}


// InfluxStorageConfig configuration
type MongoStorageConfig struct {
	DSN         string
	Database    string
	Username    string
	Password    string
	Replication int
}


func initMongoConf(DSN string) (*MongoStorageConfig, error) {
	// Parse config's DSN string
	PDSN, err := url.Parse(DSN)
	if err != nil {
		return nil, logger.Errorf("%s", err)
	}
	// Validate
	if PDSN.Host == "" {
		return nil, logger.Errorf("Mongodb config: host:port in the URL must be not empty")
	}
	if PDSN.Path == "" {
		return nil, logger.Errorf("Mongodb config: db must be not empty")
	}

	var c MongoStorageConfig
	c.DSN = fmt.Sprintf("%v://%v", PDSN.Scheme, PDSN.Host)
	c.Database = strings.Trim(PDSN.Path, "/")
	// Optional username and password
	if PDSN.User != nil {
		c.Username = PDSN.User.Username()
		c.Password, _ = PDSN.User.Password()
	}

	return &c, nil
}

