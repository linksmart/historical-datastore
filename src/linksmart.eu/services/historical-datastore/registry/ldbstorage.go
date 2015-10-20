package registry

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/pborman/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/services/historical-datastore/common"
)

// LevelDB storage
type LevelDBStorage struct {
	db     *leveldb.DB
	ntChan chan common.Notification
	wg     sync.WaitGroup
}

func NewLevelDBStorage(filename string) (Storage, *chan common.Notification, func() error, error) {
	// LevelDB options
	options := &opt.Options{
	// https://godoc.org/github.com/syndtr/goleveldb/leveldb/opt#Options
	}

	// Open the database
	db, err := leveldb.OpenFile(filename, options)
	if err != nil {
		return &LevelDBStorage{}, nil, nil, err
	}

	s := &LevelDBStorage{
		db: db,
	}
	return s, &s.ntChan, s.Close, nil
}

func (s *LevelDBStorage) Close() error {
	// Wait for pending operations
	s.wg.Wait()
	return s.db.Close()
}

func (s *LevelDBStorage) add(ds DataSource) (DataSource, error) {

	// Get a new UUID and convert it to string (UUID type can't be used as map-key)
	newUUID := fmt.Sprint(uuid.NewRandom())

	// Initialize read-only fields
	ds.ID = newUUID
	ds.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, ds.ID)
	ds.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, ds.ID)

	// Convert to json bytes
	dsBytes, err := json.Marshal(&ds)
	if err != nil {
		return DataSource{}, err
	}

	// Add the new DataSource to database
	err = s.db.Put([]byte(ds.ID), dsBytes, nil)
	if err != nil {
		return DataSource{}, err
	}

	// Send a create notification
	s.sendNotification(&ds, common.CREATE)

	return ds, nil
}

func (s *LevelDBStorage) update(id string, ds DataSource) (DataSource, error) {

	oldDS, err := s.get(id) // for comparison
	if err == leveldb.ErrNotFound {
		return DataSource{}, ErrorNotFound
	} else if err != nil {
		return DataSource{}, err
	}
	tempDS := oldDS

	// Modify writable elements
	tempDS.Meta = ds.Meta
	tempDS.Retention = ds.Retention
	tempDS.Aggregation = ds.Aggregation
	tempDS.Format = ds.Format
	//tempDS.Resource
	//tempDS.Type

	// Convert to json bytes
	dsBytes, err := json.Marshal(&tempDS)
	if err != nil {
		return DataSource{}, err
	}

	// Store the modified DS
	err = s.db.Put([]byte(tempDS.ID), dsBytes, nil)
	if err != nil {
		return DataSource{}, err
	}

	// Compare DataAPI-related changes
	if oldDS.Retention != tempDS.Retention { // anything else?
		// Send an update notification
		s.sendNotification(&tempDS, common.UPDATE_DATA)
	}
	// Compare AggrAPI-related changes
	//	if(oldDS.Aggregation != updatedDS.Aggregation){
	//		// Send an update notification
	//		regAPI.sendNotification(&updatedDS, common.UPDATE_AGGR)
	//	}

	return tempDS, nil
}

func (s *LevelDBStorage) delete(id string) error {

	ds, err := s.get(id) // for notification
	if err != nil {
		return err
	}

	err = s.db.Delete([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return ErrorNotFound
	} else if err != nil {
		return err
	}

	// Send a delete notification
	s.sendNotification(&ds, common.DELETE)

	return nil
}

func (s *LevelDBStorage) get(id string) (DataSource, error) {
	// Query from database
	dsBytes, err := s.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return DataSource{}, ErrorNotFound
	} else if err != nil {
		return DataSource{}, err
	}

	var ds DataSource
	err = json.Unmarshal(dsBytes, &ds)
	if err != nil {
		return ds, err
	}

	return ds, nil
}

func (s *LevelDBStorage) getMany(page, perPage int) ([]DataSource, int, error) {

	total, err := s.getCount()
	if err != nil {
		return nil, 0, err
	}

	// Extract keys from database
	keys := make([]string, 0, total)
	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, 0, err
	}
	// LevelDB keys are sorted

	// Get the queried page
	pagedKeys := catalog.GetPageOfSlice(keys, page, perPage, common.MaxPerPage)

	// page/registry is empty
	if len(pagedKeys) == 0 {
		return []DataSource{}, total, nil
	}

	datasources := make([]DataSource, 0, len(pagedKeys))

	// TODO
	// Iterate through a database snapshot
	// pagedKeys should have exclusive upper limit i.e. [...) instead of [...]

	//	// Iterate over a latest snapshot of the database
	//	s.wg.Add(1)
	//	iter = s.db.NewIterator(&util.Range{
	//		Start: []byte(pagedKeys[0]),
	//		Limit: []byte(pagedKeys[len(pagedKeys)-1])},
	//		nil)
	//	for iter.Next() {
	//		dsBytes := iter.Value()
	//		var ds DataSource
	//		err = json.Unmarshal(dsBytes, &ds)
	//		if err != nil {
	//			return nil, 0, err
	//		}
	//		datasources = append(datasources, ds)
	//	}
	//	iter.Release()
	//	s.wg.Done()
	//	err = iter.Error()
	//	if err != nil {
	//		return nil, 0, err
	//	}

	for _, key := range pagedKeys {
		ds, err := s.get(key)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		datasources = append(datasources, ds)
	}

	return datasources, total, nil
}

func (s *LevelDBStorage) getCount() (int, error) {
	counter := 0

	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		counter++
	}
	iter.Release()
	s.wg.Done()
	err := iter.Error()
	if err != nil {
		return 0, err
	}

	return counter, nil
}

// Path filtering
// Filter one registration
func (s *LevelDBStorage) pathFilterOne(path, op, value string) (DataSource, error) {
	pathTknz := strings.Split(path, ".")

	// return the first one found
	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ds DataSource
		err := json.Unmarshal(iter.Value(), &ds)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return DataSource{}, err
		}

		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return DataSource{}, err
		}
		if matched {
			iter.Release()
			s.wg.Done()
			return ds, nil
		}
	}
	iter.Release()
	s.wg.Done()
	err := iter.Error()
	if err != nil {
		return DataSource{}, err
	}

	// No match
	return DataSource{}, nil
}

// Filter multiple registrations
func (s *LevelDBStorage) pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {

	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ds DataSource
		err := json.Unmarshal(iter.Value(), &ds)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []DataSource{}, 0, err
		}

		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []DataSource{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ds.ID)
		}
	}
	iter.Release()
	s.wg.Done()
	err := iter.Error()
	if err != nil {
		return []DataSource{}, 0, err
	}

	keys := catalog.GetPageOfSlice(matchedIDs, page, perPage, common.MaxPerPage)
	if len(keys) == 0 {
		return []DataSource{}, len(matchedIDs), nil
	}

	dss := make([]DataSource, 0, len(keys))

	// TODO
	// Iterate through a database snapshot

	for _, key := range keys {
		ds, err := s.get(key)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		dss = append(dss, ds)
	}

	return dss, len(matchedIDs), nil
}

// Sends a Notification{} to channel
func (s *LevelDBStorage) sendNotification(ds *DataSource, ntType common.NotificationTYPE) {
	if s.ntChan != nil {
		s.ntChan <- common.Notification{DS: *ds, TYPE: ntType}
	}
}
