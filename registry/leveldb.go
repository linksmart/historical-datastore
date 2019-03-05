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
	}

	/*	// bootstrap
		// Iterate over a latest snapshot of the database
		s.wg.Add(1)
		iter := s.db.NewIterator(nil, nil)
		for iter.Next() {
			var ds DataStream
			err = json.Unmarshal(iter.Value(), &ds)
			if err != nil {
				return nil, nil, fmt.Errorf("Error parsing registry data: %v", err)
			}

		}
		iter.Release()
		s.wg.Done()
		err = iter.Error()
		if err != nil {
			return nil, nil, fmt.Errorf("Error loading registry: %v", err)
		}*/

	return s, s.close, nil
}

func (s *LevelDBStorage) close() error {
	// Wait for pending operations
	s.wg.Wait()
	return s.db.Close()
}

func (s *LevelDBStorage) Add(ds DataStream) (DataStream, error) {
	err := validateCreation(ds, s.conf)
	if err != nil {
		return DataStream{}, fmt.Errorf("%s: %s", ErrConflict, err)
	}

	// Convert to json bytes
	dsBytes, err := ds.MarshalSensitiveJSON()
	if err != nil {
		return DataStream{}, err
	}

	if has, _ := s.db.Has([]byte(ds.Name), nil); has {
		return DataStream{}, fmt.Errorf("%s: Resource name not unique: %s", ErrConflict, ds.Name)
	}

	// Add the new DataSource to database
	err = s.db.Put([]byte(ds.Name), dsBytes, nil)
	if err != nil {
		return DataStream{}, err
	}

	// Send a create event
	err = s.event.created(ds)
	if err != nil {
		return DataStream{}, err
	}

	s.lastModified = time.Now()
	return ds, nil
}

func (s *LevelDBStorage) Update(name string, ds DataStream) (DataStream, error) {

	oldDS, err := s.Get(name) // for comparison
	if err == leveldb.ErrNotFound {
		return DataStream{}, fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return DataStream{}, err
	}

	err = validateUpdate(ds, oldDS, s.conf)
	if err != nil {
		return DataStream{}, fmt.Errorf("%s: %s", ErrConflict, err)
	}

	tempDS := oldDS

	tempDS.Function = ds.Function
	tempDS.Retention = ds.Retention
	tempDS.Source = ds.Source

	// Send an update event
	err = s.event.updated(oldDS, tempDS)
	if err != nil {
		return DataStream{}, err
	}

	// Convert to json bytes
	dsBytes, err := tempDS.MarshalSensitiveJSON()
	if err != nil {
		return DataStream{}, err
	}

	// Store the modified DS
	err = s.db.Put([]byte(tempDS.Name), dsBytes, nil)
	if err != nil {
		return DataStream{}, err
	}

	s.lastModified = time.Now()
	return tempDS, nil
}

func (s *LevelDBStorage) Delete(name string) error {

	ds, err := s.Get(name) // for notification
	if err != nil {
		return err
	}

	// Send a delete event
	err = s.event.deleted(ds)
	if err != nil {
		return err
	}

	err = s.db.Delete([]byte(name), nil)
	if err == leveldb.ErrNotFound {
		return fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return err
	}

	s.lastModified = time.Now()
	return nil
}

func (s *LevelDBStorage) Get(id string) (DataStream, error) {
	// Query from database
	dsBytes, err := s.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return DataStream{}, fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return DataStream{}, err
	}

	var ds DataStream
	err = json.Unmarshal(dsBytes, &ds)
	if err != nil {
		return ds, err
	}

	return ds, nil
}

func (s *LevelDBStorage) GetMany(page, perPage int) ([]DataStream, int, error) {

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
		return []DataStream{}, 0, nil
	}

	datastreams := make([]DataStream, 0, limit)

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
		var ds DataStream
		err = json.Unmarshal(dsBytes, &ds)
		if err != nil {
			return nil, 0, err
		}
		datastreams = append(datastreams, ds)
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, 0, err
	}

	return datastreams, total, nil
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
func (s *LevelDBStorage) FilterOne(path, op, value string) (*DataStream, error) {
	pathTknz := strings.Split(path, ".")

	// return the first one found
	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ds DataStream
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
func (s *LevelDBStorage) Filter(path, op, value string, page, perPage int) ([]DataStream, int, error) {
	//TODO: Filter based on path (i.e. path in name)
	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ds DataStream
		err := json.Unmarshal(iter.Value(), &ds)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []DataStream{}, 0, err
		}

		matched, err := utils.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []DataStream{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ds.Name)
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
		return []DataStream{}, len(matchedIDs), nil
	}

	datasources := make([]DataStream, len(slice))
	for i, id := range slice {
		ds, err := s.Get(id)
		if err != nil {
			return nil, len(matchedIDs), err
		}
		datasources[i] = ds
	}

	return datasources, len(matchedIDs), nil
}
