package registry

import (
	"fmt"
	"sync"

	"sort"
	"strings"

	"github.com/pborman/uuid"
	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/services/historical-datastore/common"
)

// In-memory storage
type MemoryStorage struct {
	data  map[string]DataSource
	mutex sync.RWMutex
	nt    chan common.Notification
}

func NewMemoryStorage() (Storage, *chan common.Notification) {
	ms := &MemoryStorage{
		data: make(map[string]DataSource),
	}

	return ms, &ms.nt
}

func (ms *MemoryStorage) add(ds DataSource) (DataSource, error) {
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

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	// Send a create notification
	err = sendNotification(ds, common.CREATE, ms.nt)
	if err != nil {
		return DataSource{}, err
	}

	// Add the new DataSource to the map
	ms.data[newUUID] = ds

	return ms.data[newUUID], nil
}

func (ms *MemoryStorage) update(id string, ds DataSource) (DataSource, error) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	_, ok := ms.data[id]
	if !ok {
		return DataSource{}, fmt.Errorf("%s: %s", ErrNotFound, "Data source is not found.")
	}

	oldDS := ms.data[id] // for comparison

	err := validateUpdate(ds, oldDS)
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
	err = sendNotification([]DataSource{oldDS, tempDS}, common.UPDATE, ms.nt)
	if err != nil {
		return DataSource{}, err
	}

	// Store the modified DS
	ms.data[id] = tempDS

	return ms.data[id], nil
}

func (ms *MemoryStorage) delete(id string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	_, ok := ms.data[id]
	if !ok {
		return fmt.Errorf("%s: %s", ErrNotFound, "Data source is not found.")
	}

	// Send a delete notification
	err := sendNotification(ms.data[id], common.DELETE, ms.nt)
	if err != nil {
		return err
	}

	delete(ms.data, id)

	return nil
}

func (ms *MemoryStorage) get(id string) (DataSource, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	ds, ok := ms.data[id]
	if !ok {
		return ds, fmt.Errorf("%s: %s", ErrNotFound, "Data source is not found.")
	}

	return ds, nil
}

func (ms *MemoryStorage) getMany(page, perPage int) ([]DataSource, int, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	total, _ := ms.getCount()

	// Extract keys out of maps
	allKeys := make([]string, 0, total)
	for k := range ms.data {
		allKeys = append(allKeys, k)
	}
	// Sort keys
	sort.Strings(allKeys)

	// Get the queried page
	pagedKeys := catalog.GetPageOfSlice(allKeys, page, perPage, MaxPerPage)

	// Registry is empty
	if len(pagedKeys) == 0 {
		return []DataSource{}, total, nil
	}

	datasources := make([]DataSource, 0, len(pagedKeys))
	for _, k := range pagedKeys {
		datasources = append(datasources, ms.data[k])
	}

	return datasources, total, nil
}

func (ms *MemoryStorage) getCount() (int, error) {
	return len(ms.data), nil
}

// Path filtering
// Filter one registration
func (ms *MemoryStorage) pathFilterOne(path, op, value string) (DataSource, error) {
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	// return the first one found
	for _, ds := range ms.data {
		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			return DataSource{}, err
		}
		if matched {
			return ds, nil
		}
	}

	return DataSource{}, nil
}

// Filter multiple registrations
func (ms *MemoryStorage) pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	for _, ds := range ms.data {
		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			return []DataSource{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ds.ID)
		}
	}

	keys := catalog.GetPageOfSlice(matchedIDs, page, perPage, MaxPerPage)
	if len(keys) == 0 {
		return []DataSource{}, len(matchedIDs), nil
	}

	dss := make([]DataSource, 0, len(keys))
	//var dss []DataSource
	for _, k := range keys {
		dss = append(dss, ms.data[k])
	}

	return dss, len(matchedIDs), nil
}
