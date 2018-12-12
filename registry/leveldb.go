// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/sc/service-catalog/utils"
	"github.com/pborman/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDB storage
type LevelDBStorage struct {
	conf         common.RegConf
	db           *leveldb.DB
	event        eventHandler
	wg           sync.WaitGroup
	lastModified time.Time
	resources    map[string]string
}

func NewLevelDBStorage(conf common.RegConf, opts *opt.Options, listeners ...EventListener) (Storage, func() error, error) {
	url, err := url.Parse(conf.Backend.DSN)
	if err != nil {
		return nil, nil, err
	}

	// Open the database
	db, err := leveldb.OpenFile(url.Path, opts)
	if err != nil {
		return nil, nil, err
	}

	s := &LevelDBStorage{
		conf:         conf,
		db:           db,
		event:        listeners,
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
			return nil, nil, fmt.Errorf("Error parsing registry data: %v", err)
		}
		s.resources[ds.Resource] = ds.ID
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, nil, fmt.Errorf("Error loading registry: %v", err)
	}

	return s, s.close, nil
}

func (s *LevelDBStorage) close() error {
	// Wait for pending operations
	s.wg.Wait()
	return s.db.Close()
}

func (s *LevelDBStorage) Add(ds DataSource) (DataSource, error) {
	err := validateCreation(ds, s.conf)
	if err != nil {
		return DataSource{}, fmt.Errorf("%s: %s", ErrConflict, err)
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

	// Send a create event
	err = s.event.created(ds)
	if err != nil {
		return DataSource{}, err
	}

	// Convert to json bytes
	dsBytes, err := ds.MarshalSensitiveJSON()
	if err != nil {
		return DataSource{}, err
	}

	if _, exists := s.resources[ds.Resource]; exists {
		return DataSource{}, fmt.Errorf("%s: Resource name not unique: %s", ErrConflict, ds.Resource)
	}

	// Add the new DataSource to database
	err = s.db.Put([]byte(ds.ID), dsBytes, nil)
	if err != nil {
		return DataSource{}, err
	}
	// Add secondary index
	s.resources[ds.Resource] = ds.ID

	s.lastModified = time.Now()
	return ds, nil
}

func (s *LevelDBStorage) Update(id string, ds DataSource) (DataSource, error) {

	oldDS, err := s.Get(id) // for comparison
	if err == leveldb.ErrNotFound {
		return DataSource{}, fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return DataSource{}, err
	}

	err = validateUpdate(ds, oldDS, s.conf)
	if err != nil {
		return DataSource{}, fmt.Errorf("%s: %s", ErrConflict, err)
	}

	tempDS := oldDS

	// Modify writable elements
	tempDS.Meta = ds.Meta
	tempDS.Connector = ds.Connector
	tempDS.Retention = ds.Retention
	tempDS.Aggregation = ds.Aggregation
	//tempDS.Resource
	//tempDS.Type

	// Re-generate read-only fields
	for i := range tempDS.Aggregation {
		tempDS.Aggregation[i].Make(ds.ID)
	}

	// Send an update event
	err = s.event.updated(oldDS, tempDS)
	if err != nil {
		return DataSource{}, err
	}

	// Convert to json bytes
	dsBytes, err := tempDS.MarshalSensitiveJSON()
	if err != nil {
		return DataSource{}, err
	}

	// Store the modified DS
	err = s.db.Put([]byte(tempDS.ID), dsBytes, nil)
	if err != nil {
		return DataSource{}, err
	}

	s.lastModified = time.Now()
	return tempDS, nil
}

func (s *LevelDBStorage) Delete(id string) error {

	ds, err := s.Get(id) // for notification
	if err != nil {
		return err
	}

	// Send a delete event
	err = s.event.deleted(ds)
	if err != nil {
		return err
	}

	err = s.db.Delete([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return err
	}
	delete(s.resources, ds.Resource)

	s.lastModified = time.Now()
	return nil
}

func (s *LevelDBStorage) Get(id string) (DataSource, error) {
	// Query from database
	dsBytes, err := s.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return DataSource{}, fmt.Errorf("%s: %s", ErrNotFound, err)
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

func (s *LevelDBStorage) GetMany(page, perPage int) ([]DataSource, int, error) {

	total, err := s.getTotal()
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
	offset, limit, err := utils.GetPagingAttr(total, page, perPage, MaxPerPage)
	if err != nil {
		return nil, 0, err
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
			return nil, 0, err
		}
		datasources = append(datasources, ds)
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, 0, err
	}

	return datasources, total, nil
}

func (s *LevelDBStorage) getTotal() (int, error) {
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

func (s *LevelDBStorage) getLastModifiedTime() (time.Time, error) {
	return s.lastModified, nil
}

// Path filtering
// Filter one registration
func (s *LevelDBStorage) FilterOne(path, op, value string) (*DataSource, error) {
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
			return nil, err
		}

		matched, err := utils.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return nil, err
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
		return nil, err
	}

	// No match
	return nil, nil
}

// Filter multiple registrations
func (s *LevelDBStorage) Filter(path, op, value string, page, perPage int) ([]DataSource, int, error) {

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

		matched, err := utils.MatchObject(ds, pathTknz, op, value)
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
		return nil, 0, err
	}

	// Apply pagination
	slice, err := utils.GetPageOfSlice(matchedIDs, page, perPage, MaxPerPage)
	if err != nil {
		return nil, 0, err
	}

	// page/registry is empty
	if len(slice) == 0 {
		return []DataSource{}, len(matchedIDs), nil
	}

	datasources := make([]DataSource, len(slice))
	for i, id := range slice {
		ds, err := s.Get(id)
		if err != nil {
			return nil, len(matchedIDs), err
		}
		datasources[i] = ds
	}

	return datasources, len(matchedIDs), nil
}
