// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/service-catalog/v2/utils"
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
			var ts TimeSeries
			err = json.Unmarshal(iter.Value(), &ts)
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

func (s *LevelDBStorage) add(ts TimeSeries) (*TimeSeries, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	// Convert to json bytes
	tsBytes, err := ts.MarshalSensitiveJSON()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrBadRequest, err)
	}

	if has, _ := s.db.Has([]byte(ts.Name), nil); has {
		return nil, fmt.Errorf("%w: Resource name not unique: %s", ErrConflict, ts.Name)
	}

	// Add the new DataSource to database
	err = s.db.Put([]byte(ts.Name), tsBytes, nil)
	if err != nil {
		return nil, err
	}

	// Send a create event
	err = s.event.created(&ts)
	//if create event fails, then undo creation of the event
	if err != nil {
		// Send a delete event
		s.event.deleted(&ts)
		deleteErr := s.db.Delete([]byte(ts.Name), nil)
		if deleteErr != nil {
			err = fmt.Errorf("%w, followed by error undoing the time series creation:%s", err, deleteErr)
		}
		return nil, err
	}

	s.lastModified = time.Now()
	return &ts, nil
}

func (s *LevelDBStorage) update(name string, ts TimeSeries) (*TimeSeries, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	oldTS, err := s.get(name) // for comparison
	if err == leveldb.ErrNotFound {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, err)
	} else if err != nil {
		return nil, err
	}

	err = validateUpdate(ts, *oldTS, s.conf)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrConflict, err)
	}

	tempTS := oldTS

	// Modify writable elements
	tempTS.Source = ts.Source
	tempTS.Meta = ts.Meta
	tempTS.Unit = ts.Unit

	// Send an update event
	err = s.event.updated(oldTS, tempTS)
	if err != nil {
		return nil, err
	}

	// Convert to json bytes
	tsBytes, err := tempTS.MarshalSensitiveJSON()
	if err != nil {
		return nil, err
	}

	// Store the modified TS
	err = s.db.Put([]byte(tempTS.Name), tsBytes, nil)
	if err != nil {
		return nil, err
	}

	s.lastModified = time.Now()
	return tempTS, nil
}

func (s *LevelDBStorage) delete(name string) error {
	s.wg.Add(1)
	defer s.wg.Done()
	ts, err := s.get(name) // for notification
	if err != nil {
		return err
	}

	// Send a delete event
	err = s.event.deleted(ts)
	if err != nil {
		return err
	}

	err = s.db.Delete([]byte(name), nil)
	if err == leveldb.ErrNotFound {
		return fmt.Errorf("%w: %s", ErrNotFound, err)
	} else if err != nil {
		return err
	}

	s.lastModified = time.Now()
	return nil
}

func (s *LevelDBStorage) get(id string) (*TimeSeries, error) {
	// QueryPage from database
	tsBytes, err := s.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, err)
	} else if err != nil {
		return nil, err
	}

	var ts TimeSeries
	err = json.Unmarshal(tsBytes, &ts)
	if err != nil {
		return nil, err
	}

	return &ts, nil
}

func (s *LevelDBStorage) getMany(page, perPage int) ([]TimeSeries, int, error) {

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
		return []TimeSeries{}, 0, nil
	}

	timeSeries := make([]TimeSeries, 0, limit)

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
		tsBytes := iter.Value()
		var ts TimeSeries
		err = json.Unmarshal(tsBytes, &ts)
		if err != nil {
			return nil, 0, err
		}
		timeSeries = append(timeSeries, ts)
	}
	iter.Release()
	s.wg.Done()
	err = iter.Error()
	if err != nil {
		return nil, 0, err
	}

	return timeSeries, total, nil
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
func (s *LevelDBStorage) filterOne(path, op, value string) (*TimeSeries, error) {
	pathTknz := strings.Split(path, ".")

	// return the first one found
	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ts TimeSeries
		err := json.Unmarshal(iter.Value(), &ts)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return nil, err
		}

		matched, err := utils.MatchObject(ts, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return nil, err
		}
		if matched {
			iter.Release()
			s.wg.Done()
			return &ts, nil
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
func (s *LevelDBStorage) filter(path, op, value string, page, perPage int) ([]TimeSeries, int, error) {
	//TODO: Filter based on path (i.e. path in name)
	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var ts TimeSeries
		err := json.Unmarshal(iter.Value(), &ts)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []TimeSeries{}, 0, err
		}

		matched, err := utils.MatchObject(ts, pathTknz, op, value)
		if err != nil {
			iter.Release()
			s.wg.Done()
			return []TimeSeries{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ts.Name)
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
		return []TimeSeries{}, len(matchedIDs), nil
	}

	timeSeries := make([]TimeSeries, len(slice))
	for i, id := range slice {
		ts, err := s.get(id)
		if err != nil {
			return nil, len(matchedIDs), err
		}
		timeSeries[i] = *ts
	}

	return timeSeries, len(matchedIDs), nil
}
