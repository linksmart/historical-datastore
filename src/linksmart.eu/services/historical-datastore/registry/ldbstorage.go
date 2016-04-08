package registry

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/pborman/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/services/historical-datastore/common"
)

// LevelDB storage
type LevelDBStorage struct {
	db *leveldb.DB
	nt chan common.Notification
	wg sync.WaitGroup
}

func NewLevelDBStorage(dsn string, opts *opt.Options) (Storage, *chan common.Notification, func() error, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return &LevelDBStorage{}, nil, nil, err
	}

	// Open the database
	db, err := leveldb.OpenFile(url.Path, opts)
	if err != nil {
		return &LevelDBStorage{}, nil, nil, err
	}

	s := &LevelDBStorage{
		db: db,
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

	// Send a create notification
	err = sendNotification(ds, common.CREATE, s.nt)
	if err != nil {
		return DataSource{}, err
	}

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

	return ds, nil
}

func (s *LevelDBStorage) update(id string, ds DataSource) (DataSource, error) {

	oldDS, err := s.get(id) // for comparison
	if err == leveldb.ErrNotFound {
		return DataSource{}, fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return DataSource{}, err
	}

	err = validateUpdate(ds, oldDS)
	if err != nil {
		return DataSource{}, fmt.Errorf("%s: %s", ErrConflict, err)
	}

	tempDS := oldDS

	// Modify writable elements
	tempDS.Meta = ds.Meta
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
		return DataSource{}, err
	}

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

	return tempDS, nil
}

func (s *LevelDBStorage) delete(id string) error {

	ds, err := s.get(id) // for notification
	if err != nil {
		return err
	}

	// Send a delete notification
	err = sendNotification(ds, common.DELETE, s.nt)
	if err != nil {
		return err
	}

	err = s.db.Delete([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return fmt.Errorf("%s: %s", ErrNotFound, err)
	} else if err != nil {
		return err
	}

	return nil
}

func (s *LevelDBStorage) get(id string) (DataSource, error) {
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
	offset, limit := GetPageOfSlice(keys, page, perPage, MaxPerPage)

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
		return nil, 0, err
	}

	// Apply pagination
	slice := catalog.GetPageOfSlice(matchedIDs, page, perPage, MaxPerPage)

	// page/registry is empty
	if len(slice) == 0 {
		return []DataSource{}, len(matchedIDs), nil
	}

	datasources := make([]DataSource, len(slice))
	for i, id := range slice {
		ds, err := s.get(id)
		if err != nil {
			return nil, len(matchedIDs), err
		}
		datasources[i] = ds
	}

	return datasources, len(matchedIDs), nil
}

// Returns offset and limit representing a subset of the given slice
//	 based on the requested 'page'
func GetPageOfSlice(slice []string, page, perPage, maxPerPage int) (int, int) {
	//keys := []string{}
	page, perPage = catalog.ValidatePagingParams(page, perPage, maxPerPage)

	// Never return more than the defined maximum
	if perPage > maxPerPage || perPage == 0 {
		perPage = maxPerPage
	}

	// if 1, not specified or negative - return the first page
	if page < 2 {
		// first page
		if perPage > len(slice) {
			//keys = slice
			return 0, len(slice)
		} else {
			//keys = slice[:perPage]
			return 0, perPage
		}
	} else if page == int(len(slice)/perPage)+1 {
		// last page
		//keys = slice[perPage*(page-1):]
		return perPage * (page - 1), len(slice) - perPage*(page-1)
	} else if page <= len(slice)/perPage && page*perPage <= len(slice) {
		// slice
		r := page * perPage
		l := r - perPage
		//keys = slice[l:r]
		return l, r - l
	}
	return 0, 0
}
