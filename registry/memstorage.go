package registry

import (
	"fmt"
	"sync"
	//"time"
	"errors"
	"sort"
	"strings"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/code.google.com/p/go-uuid/uuid"
	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/linksmart.eu/lc/core/catalog"
	"linksmart.eu/services/historical-datastore/common"
)

var ErrorNotFound = errors.New("Data source is not found!")

// In-memory storage
type MemoryStorage struct {
	data map[string]DataSource
	//index []string
	mutex  sync.RWMutex
	ntChan chan<- common.Notification // write-only channel
}

func NewMemoryStorage(ntChan chan<- common.Notification) Storage {
	return &MemoryStorage{
		data:   make(map[string]DataSource),
		ntChan: ntChan,
	}
}

func (ms *MemoryStorage) add(ds DataSource) (DataSource, error) {
	// Get a new UUID and convert it to string (UUID type can't be used as map-key)
	newUUID := fmt.Sprint(uuid.NewRandom())

	// Initialize read-only fields
	ds.ID = newUUID
	ds.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, ds.ID)
	ds.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, ds.ID)

	// Add the new DataSource to the map
	ms.mutex.RLock()
	ms.data[newUUID] = ds
	ms.mutex.RUnlock()
	//fmt.Println("New DS: ", ms.data[newUUID])

	// Send a create notification
	ms.sendNotification(&ds, common.CREATE)

	return ms.data[newUUID], nil
}

func (ms *MemoryStorage) update(id string, ds DataSource) (DataSource, error) {
	ms.mutex.Lock()
	_, ok := ms.data[id]
	if !ok {
		ms.mutex.Unlock()
		return DataSource{}, ErrorNotFound
	}

	oldDS := ms.data[id] // for comparison
	tempDS := oldDS

	// Modify writable elements
	tempDS.Meta = ds.Meta
	tempDS.Retention = ds.Retention
	tempDS.Aggregation = ds.Aggregation
	tempDS.Format = ds.Format
	//tempDS.Resource
	//tempDS.Type

	// Store the modified DS
	ms.data[id] = tempDS
	updatedDS := ms.data[id] // for comparison
	ms.mutex.Unlock()

	// Compare DataAPI-related changes
	if oldDS.Retention != updatedDS.Retention { // anything else?
		// Send an update notification
		ms.sendNotification(&updatedDS, common.UPDATE_DATA)
	}
	// Compare AggrAPI-related changes
	//	if(oldDS.Aggregation != updatedDS.Aggregation){
	//		// Send an update notification
	//		regAPI.sendNotification(&updatedDS, common.UPDATE_AGGR)
	//	}

	return ms.data[id], nil
}

func (ms *MemoryStorage) delete(id string) error {
	ms.mutex.Lock()

	_, ok := ms.data[id]
	if !ok {
		ms.mutex.Unlock()
		return ErrorNotFound
	}
	deletedDS := ms.data[id] // for notification

	delete(ms.data, id)
	ms.mutex.Unlock()

	// Send a delete notification
	ms.sendNotification(&deletedDS, common.DELETE)

	return nil
}

func (ms *MemoryStorage) get(id string) (DataSource, error) {
	ms.mutex.RLock()
	ds, ok := ms.data[id]
	if !ok {
		ms.mutex.RUnlock()
		return ds, ErrorNotFound
	}
	ms.mutex.RUnlock()

	return ds, nil
}

func (ms *MemoryStorage) getMany(page, perPage int) ([]DataSource, int, error) {
	ms.mutex.RLock()
	total := ms.getCount()

	// Extract keys out of maps
	allKeys := make([]string, 0, total)
	for k := range ms.data {
		allKeys = append(allKeys, k)
	}
	// Sort keys
	sort.Strings(allKeys)

	// Get the queried page
	pagedKeys := catalog.GetPageOfSlice(allKeys, page, perPage, common.MaxPerPage)

	// Registry is empty
	if len(pagedKeys) == 0 {
		ms.mutex.RUnlock()
		return []DataSource{}, total, nil
	}

	datasources := make([]DataSource, 0, len(pagedKeys))
	for _, k := range pagedKeys {
		datasources = append(datasources, ms.data[k])
	}
	ms.mutex.RUnlock()

	return datasources, total, nil
}

func (ms *MemoryStorage) getCount() int {
	return len(ms.data)
}

// Path filtering
// Filter one registration
func (ms *MemoryStorage) pathFilterOne(path, op, value string) (DataSource, error) {
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	// return the first one found
	for _, ds := range ms.data {
		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			ms.mutex.RUnlock()
			return DataSource{}, err
		}
		if matched {
			ms.mutex.RUnlock()
			return ds, nil
		}
	}
	ms.mutex.RUnlock()
	return DataSource{}, nil
}

// Filter multiple registrations
func (ms *MemoryStorage) pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	for _, ds := range ms.data {
		matched, err := catalog.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			ms.mutex.RUnlock()
			return []DataSource{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ds.ID)
		}
	}

	keys := catalog.GetPageOfSlice(matchedIDs, page, perPage, common.MaxPerPage)
	if len(keys) == 0 {
		ms.mutex.RUnlock()
		return []DataSource{}, len(matchedIDs), nil
	}

	dss := make([]DataSource, 0, len(keys))
	//var dss []DataSource
	for _, k := range keys {
		dss = append(dss, ms.data[k])
	}
	ms.mutex.RUnlock()
	return dss, len(matchedIDs), nil
}

// Sends a Notification{} to channel
func (ms *MemoryStorage) sendNotification(ds *DataSource, ntType common.NotificationTYPE) {
	ms.ntChan <- common.Notification{DS: *ds, TYPE: ntType}
}
