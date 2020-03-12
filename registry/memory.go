// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/service-catalog/v2/utils"
)

// In-memory storage
type MemoryStorage struct {
	conf         common.RegConf
	data         map[string]*DataStream
	mutex        sync.RWMutex
	event        eventHandler
	lastModified time.Time
	resources    map[string]string
}

func NewMemoryStorage(conf common.RegConf, listeners ...EventListener) Storage {
	ms := &MemoryStorage{
		conf:         conf,
		data:         make(map[string]*DataStream),
		lastModified: time.Now(),
		resources:    make(map[string]string),
		event:        listeners,
	}

	return ms
}

func (ms *MemoryStorage) Add(ds DataStream) (*DataStream, error) {
	err := validateCreation(ds, ms.conf)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalid, err)
	}
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if _, exists := ms.resources[ds.Name]; exists {
		return nil, fmt.Errorf("%w: Resource name not unique: %s", ErrConflict, ds.Name)
	}

	// Add the new DataSource to the map
	ms.data[ds.Name] = &ds
	// Add secondary index
	ms.resources[ds.Name] = ds.Name

	// Send a create event
	err = ms.event.created(&ds)
	if err != nil {
		return nil, err
	}
	ms.lastModified = time.Now()
	return ms.data[ds.Name], nil
}

func (ms *MemoryStorage) Update(id string, ds DataStream) (*DataStream, error) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	_, ok := ms.data[id]
	if !ok {
		return nil, fmt.Errorf("%s: %s", ErrNotFound, "Data source is not found.")
	}

	oldDS := ms.data[id] // for comparison

	err := validateUpdate(ds, *oldDS, ms.conf)
	if err != nil {
		return nil, fmt.Errorf("%s: %s", ErrConflict, err)
	}

	tempDS := *oldDS

	// Modify writable elements
	tempDS.Function = ds.Function
	tempDS.Retention = ds.Retention
	tempDS.Source = ds.Source
	tempDS.Meta = ds.Meta

	// Send an update event
	err = ms.event.updated(oldDS, &tempDS)
	if err != nil {
		return nil, err
	}

	// Store the modified DS
	ms.data[id] = &tempDS

	ms.lastModified = time.Now()
	return ms.data[id], nil
}

func (ms *MemoryStorage) Delete(name string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	_, ok := ms.data[name]
	if !ok {
		return fmt.Errorf("%s: %s", ErrNotFound, "Data source is not found.")
	}

	// Send a delete event
	err := ms.event.deleted(ms.data[name])
	if err != nil {
		return err
	}

	delete(ms.resources, ms.data[name].Name)
	delete(ms.data, name)

	ms.lastModified = time.Now()
	return nil
}

func (ms *MemoryStorage) Get(id string) (*DataStream, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	ds, ok := ms.data[id]
	if !ok {
		return nil, fmt.Errorf("%s: %s", ErrNotFound, "Data source is not found.")
	}

	return ds, nil
}

func (ms *MemoryStorage) GetMany(page, perPage int) ([]DataStream, int, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	total, _ := ms.getTotal()

	// Extract keys out of maps
	allKeys := make([]string, 0, total)
	for k := range ms.data {
		allKeys = append(allKeys, k)
	}
	// Sort keys
	sort.Strings(allKeys)

	// Get the queried page
	pagedKeys, err := utils.GetPageOfSlice(allKeys, page, perPage, MaxPerPage)
	if err != nil {
		return []DataStream{}, 0, err
	}

	// DataStreamList is empty
	if len(pagedKeys) == 0 {
		return []DataStream{}, total, nil
	}

	datasources := make([]DataStream, 0, len(pagedKeys))
	for _, k := range pagedKeys {
		datasources = append(datasources, *ms.data[k])
	}

	return datasources, total, nil
}

func (ms *MemoryStorage) getTotal() (int, error) {
	return len(ms.data), nil
}

func (ms *MemoryStorage) getLastModifiedTime() (time.Time, error) {
	return ms.lastModified, nil
}

// Path filtering
// Filter one registration
func (ms *MemoryStorage) FilterOne(path, op, value string) (*DataStream, error) {
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	// return the first one found
	for _, ds := range ms.data {
		matched, err := utils.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			return nil, err
		}
		if matched {
			return ds, nil
		}
	}

	return nil, nil
}

// Filter multiple registrations
func (ms *MemoryStorage) Filter(path, op, value string, page, perPage int) ([]DataStream, int, error) {
	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	for _, ds := range ms.data {
		matched, err := utils.MatchObject(ds, pathTknz, op, value)
		if err != nil {
			return []DataStream{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ds.Name)
		}
	}

	keys, err := utils.GetPageOfSlice(matchedIDs, page, perPage, MaxPerPage)
	if err != nil {
		return []DataStream{}, 0, err
	}
	if len(keys) == 0 {
		return []DataStream{}, len(matchedIDs), nil
	}

	dss := make([]DataStream, 0, len(keys))
	//var dss []DataSource
	for _, k := range keys {
		dss = append(dss, *ms.data[k])
	}

	return dss, len(matchedIDs), nil
}
