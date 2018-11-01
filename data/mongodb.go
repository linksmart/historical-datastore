package data
//
//import (
//	"fmt"
//	"time"
//
//	mgo "gopkg.in/mgo.v2"
//	"gopkg.in/mgo.v2/bson"
//
//	"code.linksmart.eu/hds/historical-datastore/common"
//	"code.linksmart.eu/hds/historical-datastore/registry"
//	"github.com/krylovsk/gosenml"
//)
//
//// Entry is a measurement of Parameter Entry
//type Entry struct {
//	Name         string   `bson:"n,omitempty"`
//	Units        string   `bson:"u,omitempty"`
//	Value        *float64 `bson:"v,omitempty"`
//	StringValue  *string  `bson:"sv,omitempty"`
//	BooleanValue *bool    `bson:"bv,omitempty"`
//	Sum          *float64 `bson:"s,omitempty"`
//	Time         int64    `bson:"t,omitempty"`
//	UpdateTime   int64    `bson:"ut,omitempty"`
//}
//
//// InfluxStorage implements a simple data storage back-end with SQLite
//type MongoStorage struct {
//	session  *mgo.Session
//	dialInfo *mgo.DialInfo
//}
//
//// NewInfluxStorage returns a new Storage given a configuration
//func NewMongoStorage(DSN string) (*MongoStorage, chan<- common.Notification, error) {
//	dialInfo, err := mgo.ParseURL(DSN)
//	if err != nil {
//		return nil, nil, logger.Errorf("Mongo url parse error: %s", err)
//	}
//
//	csession, err := mgo.DialWithInfo(dialInfo)
//	if err != nil {
//		return nil, nil, logger.Errorf("Error initializing MOngoDB client: %s", err)
//	}
//
//	s := &MongoStorage{
//		session:  csession,
//		dialInfo: dialInfo,
//	}
//
//	// Run the notification listener
//	ntChan := make(chan common.Notification)
//	go NtfListener(s, ntChan)
//
//	return s, ntChan, nil
//}
//
//// Formatted measurement name for a given data source
//func (s *MongoStorage) Msrmt(ds *registry.DataSource) string {
//	return fmt.Sprintf("data_%s", ds.ID)
//}
//
//// Formatted retention policy name for a given data source
//func (s *MongoStorage) Retention(ds *registry.DataSource) string {
//	return fmt.Sprintf("policy_%s", ds.ID)
//}
//
//// Fully qualified measurement name
//func (s *MongoStorage) FQMsrmt(ds *registry.DataSource) string {
//	return fmt.Sprintf("%s.\"%s\".\"%s\"", s.dialInfo.Database, s.Retention(ds), s.Msrmt(ds))
//}
//
//// The field-name for HDS data types
//func (s *MongoStorage) FieldForType(t string) string {
//	switch t {
//	case common.FLOAT:
//		return "value"
//	case common.STRING:
//		return "stringValue"
//	case common.BOOL:
//		return "booleanValue"
//	}
//	return ""
//}
//
//// Database name
//func (s *MongoStorage) Database() string {
//	return s.dialInfo.Database
//}
//
//// Adds multiple data points for multiple data sources
//// data is a map where keys are data source ids
//func (s *MongoStorage) Submit(data map[string][]DataPoint, sources map[string]*registry.DataSource) error {
//	for id, dps := range data {
//		session := s.session.Copy()
//		defer session.Close()
//
//		collection := session.DB(s.Database()).C(s.Msrmt(sources[id]))
//
//		for _, senmldp := range dps {
//			dp := Entry{
//				Name:         senmldp.Name,
//				Units:        senmldp.Units,
//				Value:        senmldp.Value,
//				StringValue:  senmldp.StringValue,
//				BooleanValue: senmldp.BooleanValue,
//				Sum:          senmldp.Sum,
//				Time:         senmldp.Time,
//				UpdateTime:   senmldp.UpdateTime,
//			}
//			var timestamp time.Time
//			if dp.Time == 0 {
//				timestamp = time.Now()
//			} else {
//				timestamp = time.Unix(dp.Time, 0)
//			}
//			dp.Time = timestamp.Unix()
//			collection.Insert(dp)
//		}
//
//	}
//
//	return nil
//}
//
//// Queries data for specified data sources
//
//func (s *MongoStorage) Query(q Query, page, perPage int, sources ...*registry.DataSource) (DataSet, int, error) {
//	points := []DataPoint{}
//	total := 0
//	// Set minimum time to 1970-01-01T00:00:00Z
//	if q.Start.Before(time.Unix(0, 0)) {
//		q.Start = time.Unix(0, 0)
//		if q.End.Before(time.Unix(0, 1)) {
//			return NewDataSet(), 0, logger.Errorf("%s argument must be greater than 1970-01-01T00:00:00Z", common.ParamEnd)
//		}
//	}
//
//	// If q.End is not set, make the query open-ended
//
//	var queryBson bson.M
//	queryBson = bson.M{}
//	if q.Start.Before(q.End) {
//		queryBson = bson.M{
//			"t": bson.M{"$lte": q.End.Unix(),
//				"$gte": q.Start.Unix()},
//		}
//
//	} else {
//		queryBson = bson.M{
//			"t": bson.M{"$gte": bson.MongoTimestamp(q.Start.Unix())},
//		}
//
//	}
//
//	perItems, offsets := common.PerItemPagination(q.Limit, page, perPage, len(sources))
//
//	// Initialize sort order
//	sort := "-t"
//	if q.Sort == common.ASC {
//		sort = "+t"
//	}
//
//	for i, ds := range sources {
//
//		session := s.session.Copy()
//		defer session.Close()
//
//		collection := session.DB(s.Database()).C(s.Msrmt(ds))
//		var searchResults []Entry
//		err := collection.Find(queryBson).Sort(sort).Skip(offsets[i]).Limit(perItems[i]).All(&searchResults)
//		//err := collection.Find(queryBson).Sort(sort).All(&searchResults)
//		//offsets[i]).Limit(perItems[i]
//		logger.Println(offsets, perItems, sort)
//		logger.Println("the query is:", queryBson)
//		logger.Println("the result is:", searchResults)
//		if err != nil {
//			return NewDataSet(), 0, logger.Errorf("Error parsing points for source %v: %s", ds.Resource, err)
//		}
//
//		var entries = make([]DataPoint, len(searchResults))
//		for i, val := range searchResults {
//			entries[i] = DataPoint{
//				&gosenml.Entry{
//					val.Name,
//					val.Units,
//					val.Value,
//					val.StringValue,
//					val.BooleanValue,
//					val.Sum,
//					val.Time,
//					val.UpdateTime,
//				},
//			}
//		}
//		if perItems[i] != 0 {
//			points = append(points, entries...)
//		}
//	}
//
//	dataset := NewDataSet()
//	dataset.Entries = points
//
//	// q.Limit overrides total
//	if q.Limit > 0 && q.Limit < total {
//		total = q.Limit
//	}
//
//	return dataset, total, nil
//
//}
//
//// Handles the creation of a new data source
//func (s *MongoStorage) NtfCreated(ds registry.DataSource, callback chan error) {
//
//	if ds.Retention != "" {
//
//		duration, err := time.ParseDuration(ds.Retention)
//		if err != nil {
//			callback <- logger.Errorf("Error parsing the duration: %s", err)
//			return
//		}
//		session := s.session.Copy()
//		defer session.Close()
//
//		collection := session.DB(s.Database()).C(s.Msrmt(&ds))
//
//		TTL := mgo.Index{
//			Key:         []string{"t"},
//			Unique:      false,
//			DropDups:    false,
//			Background:  true,
//			ExpireAfter: duration} // one hour
//		err = collection.EnsureIndex(TTL)
//		if err != nil {
//			callback <- logger.Errorf("Error modifying the retention policy for source: %s", err)
//			return
//		}
//		logger.Println("MongoStorage: created retention policy for", ds.ID)
//	}
//	callback <- nil
//}
//
//// Handles updates of a data source
//func (s *MongoStorage) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {
//
//	if oldDS.Retention != newDS.Retention {
//		duration, err := time.ParseDuration(newDS.Retention)
//		if err != nil {
//			callback <- logger.Errorf("Error parsing the duration: %s", err)
//			return
//		}
//
//		session := s.session.Copy()
//		defer session.Close()
//
//		collection := session.DB(s.Database()).C(s.Msrmt(&newDS))
//
//		TTL := mgo.Index{
//			Key:         []string{"t"},
//			Unique:      false,
//			DropDups:    false,
//			Background:  true,
//			ExpireAfter: duration} // one hour
//		err = collection.EnsureIndex(TTL)
//		if err != nil {
//			callback <- logger.Errorf("Error modifying the retention policy for source: %s", err)
//			return
//		}
//		logger.Println("MongoStorage: altered retention policy for", oldDS.ID)
//	}
//	callback <- nil
//}
//
//// Handles deletion of a data source
//func (s *MongoStorage) NtfDeleted(ds registry.DataSource, callback chan error) {
//
//	session := s.session.Copy()
//	defer session.Close()
//
//	collection := session.DB(s.Database()).C(s.Msrmt(&ds))
//
//	_, err := collection.RemoveAll(nil)
//
//	if err != nil {
//		callback <- logger.Errorf("Error removing the historical data: %s", err)
//		return
//	}
//	callback <- nil
//}
