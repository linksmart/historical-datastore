// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"linksmart.eu/lc/core/catalog"
	"code.linksmart.eu/hds/historical-datastore/common"
)

// LevelDB storage
type LevelDBStorage struct {
	db           *leveldb.DB
	nt           chan common.Notification
	wg           sync.WaitGroup
	lastModified time.Time
	resources    map[string]string
}

func NewLevelDBStorage(dsn string, opts *opt.Options) (Storage, *chan common.Notification, func() error, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return nil, nil, nil, logger.Errorf("%s", err)
	}

	// Open the database
	db, err := leveldb.OpenFile(url.Path, opts)
	if err != nil {
		return nil, nil, nil, logger.Errorf("%s", err)
	}

	s := &LevelDBStorage{
		db:           db,
		lastModified: time.Now(),
		resources:    make(map[string]string),
	}

	// bootstrap
	// Iterate over a latest snapshot of the database
	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ds DataSource
		err = json.Unmarshal(iter.Value(), &ds)
		if err != nil {
			return nil, nil, nil, logger.Errorf("Error parsing registry data: %v", err)
		}
		s.resources[ds.Resource] = ds.ID
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, nil, nil, logger.Errorf("Error loading registry: %v", err)
	}

	return s, &s.nt, s.close, nil
}

func (s *LevelDBStorage) close() error {
	// Wait for pending operations
	s.wg.Wait()
	return s.db.Close()
}

func (s *LevelDBStorage) add(ds DataSource) (DataSource, error) {
	err := validateCreation(ds)
	if err != nil {
		return DataSource{}, logger.Errorf("%s: %s", ErrConflict, err)
	}

	// Get a new UUID and convert it to string (UUID type can't be used as map-key)
	newUUID := fmt.Sprint(uuid.NewRandom())

	// Initialize read-only fields
	ds.ID = newUUID
	ds.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, ds.ID)
	ds.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, ds.ID)

	for i := range ds.Aggregation {
		ds.Aggregation[i].Make(ds.ID)
	}

	// Send a create notification
	err = sendNotification(ds, common.CREATE, s.nt)
	if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	// Convert to json bytes
	dsBytes, err := json.Marshal(&ds)
	if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	if _, exists := s.resources[ds.Resource]; exists {
		return DataSource{}, logger.Errorf("%s: Resource name not unique: %s", ErrConflict, ds.Resource)
	}

	// Add the new DataSource to database
	err = s.db.Put([]byte(ds.ID), dsBytes, nil)
	if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}
	// Add secondary index
	s.resources[ds.Resource] = ds.ID

	s.lastModified = time.Now()
	return ds, nil
}

func (s *LevelDBStorage) update(id string, ds DataSource) (DataSource, error) {

	oldDS, err := s.get(id) // for comparison
	if err == leveldb.ErrNotFound {
		return DataSource{}, logger.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	err = validateUpdate(ds, oldDS)
	if err != nil {
		return DataSource{}, logger.Errorf("%s: %s", ErrConflict, err)
	}

	tempDS := oldDS

	// Modify writable elements
	tempDS.Meta = ds.Meta
	tempDS.Connector = ds.Connector
	tempDS.Retention = ds.Retention
	tempDS.Aggregation = ds.Aggregation
	tempDS.Format = ds.Format
	//tempDS.Resource
	//tempDS.Type

	// Re-generate read-only fields
	for i := range tempDS.Aggregation {
		tempDS.Aggregation[i].Make(ds.ID)
	}

	// Send an update notification
	err = sendNotification([]DataSource{oldDS, tempDS}, common.UPDATE, s.nt)
	if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	// Convert to json bytes
	dsBytes, err := json.Marshal(&tempDS)
	if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	// Store the modified DS
	err = s.db.Put([]byte(tempDS.ID), dsBytes, nil)
	if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	s.lastModified = time.Now()
	return tempDS, nil
}

func (s *LevelDBStorage) delete(id string) error {

	ds, err := s.get(id) // for notification
	if err != nil {
		return logger.Errorf("%s", err)
	}

	// Send a delete notification
	err = sendNotification(ds, common.DELETE, s.nt)
	if err != nil {
		return logger.Errorf("%s", err)
	}

	err = s.db.Delete([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return logger.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return logger.Errorf("%s", err)
	}
	delete(s.resources, ds.Resource)

	s.lastModified = time.Now()
	return nil
}

func (s *LevelDBStorage) get(id string) (DataSource, error) {
	// Query from database
	dsBytes, err := s.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return DataSource{}, logger.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return DataSource{}, logger.Errorf("%s", err)
	}

	var ds DataSource
	err = json.Unmarshal(dsBytes, &ds)
	if err != nil {
		return ds, logger.Errorf("%s", err)
	}

	return ds, nil
}

func (s *LevelDBStorage) getMany(page, perPage int) ([]DataSource, int, error) {

	total, err := s.getCount()
	if err != nil {
		return nil, 0, logger.Errorf("%s", err)
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
		return nil, 0, logger.Errorf("%s", err)
	}
	// LevelDB keys are sorted

	// Get the queried page
	offset, limit, err := catalog.GetPagingAttr(total, page, perPage, MaxPerPage)
	if err != nil {
		return nil, 0, logger.Errorf("%s", err)
	}

	// page/registry is empty
	if limit == 0 {
		return []DataSource{}, 0, nil
	}

	datasources := make([]DataSource, 0, limit)

	// a nil Range.Limit is treated as a key after all keys in the DB.
	var end []byte = nil
	if offset+limit < len(keys) {
		end = []byte(keys[offset+limit])
	}

	// Iterate over a latest snapshot of the database
	s.wg.Add(1)
	iter = s.db.NewIterator(
		&util.Range{Start: []byte(keys[offset]), Limit: end},
		nil)
	for iter.Next() {
		dsBytes := iter.Value()
		var ds DataSource
		err = json.Unmarshal(dsBytes, &ds)
		if err != nil {
			return nil, 0, logger.Errorf("%s", err)
		}
		datasources = append(datasources, ds)
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, 0, logger.Errorf("%s", err)
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
		return 0, logger.Errorf("%s", err)
	}

	return counter, nil
}

func (s *LevelDBStorage) modifiedDate() (time.Time, error) {
	return s.lastModified, nil
}

// Path filtering
// Filter one registration
func (s *LevelDBStorage) pathFilterOne(path, op, value string) (*DataSource, error) {
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
			return nil, logger.Errorf("%s", err)
		}

		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return nil, logger.Errorf("%s", err)
		}
		if matched {
			iter.Release()
			s.wg.Done()
			return &ds, nil
		}
	}
	iter.Release()
	s.wg.Done()
	err := iter.Error()
	if err != nil {
		return nil, logger.Errorf("%s", err)
	}

	// No match
	return nil, nil
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
			return []DataSource{}, 0, logger.Errorf("%s", err)
		}

		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []DataSource{}, 0, logger.Errorf("%s", err)
		}
		if matched {
			matchedIDs = append(matchedIDs, ds.ID)
		}
	}
	iter.Release()
	s.wg.Done()
	err := iter.Error()
	if err != nil {
		return nil, 0, logger.Errorf("%s", err)
	}

	// Apply pagination
	slice, err := catalog.GetPageOfSlice(matchedIDs, page, perPage, MaxPerPage)
	if err != nil {
		return nil, 0, logger.Errorf("%s", err)
	}

	// page/registry is empty
	if len(slice) == 0 {
		return []DataSource{}, len(matchedIDs), nil
	}

	datasources := make([]DataSource, len(slice))
	for i, id := range slice {
		ds, err := s.get(id)
		if err != nil {
			return nil, len(matchedIDs), logger.Errorf("%s", err)
		}
		datasources[i] = ds
	}

	return datasources, len(matchedIDs), nil
}
