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
	data         map[string]*TimeSeries
	mutex        sync.RWMutex
	event        eventHandler
	lastModified time.Time
	resources    map[string]string
}

func NewMemoryStorage(conf common.RegConf, listeners ...EventListener) Storage {
	ms := &MemoryStorage{
		conf:         conf,
		data:         make(map[string]*TimeSeries),
		lastModified: time.Now(),
		resources:    make(map[string]string),
		event:        listeners,
	}

	return ms
}

func (ms *MemoryStorage) Add(ts TimeSeries) (*TimeSeries, error) {
	err := validateCreation(ts, ms.conf)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrBadRequest, err)
	}
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if _, exists := ms.resources[ts.Name]; exists {
		return nil, fmt.Errorf("%w: Resource name not unique: %s", ErrConflict, ts.Name)
	}

	// Add the new time series to the map
	ms.data[ts.Name] = &ts
	// Add secondary index
	ms.resources[ts.Name] = ts.Name

	// Send a create event
	err = ms.event.created(&ts)
	if err != nil {
		return nil, err
	}
	ms.lastModified = time.Now()
	return ms.data[ts.Name], nil
}

func (ms *MemoryStorage) Update(id string, ts TimeSeries) (*TimeSeries, error) {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	_, ok := ms.data[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, "time series is not found.")
	}

	oldTS := ms.data[id] // for comparison

	err := validateUpdate(ts, *oldTS, ms.conf)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrConflict, err)
	}

	tempTS := *oldTS

	// Modify writable elements
	tempTS.Source = ts.Source
	tempTS.Meta = ts.Meta

	// Send an update event
	err = ms.event.updated(oldTS, &tempTS)
	if err != nil {
		return nil, err
	}

	// Store the modified ts
	ms.data[id] = &tempTS

	ms.lastModified = time.Now()
	return ms.data[id], nil
}

func (ms *MemoryStorage) Delete(name string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	_, ok := ms.data[name]
	if !ok {
		return fmt.Errorf("%s: %w", name, ErrNotFound)
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

func (ms *MemoryStorage) Get(id string) (*TimeSeries, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	ts, ok := ms.data[id]
	if !ok {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return ts, nil
}

func (ms *MemoryStorage) GetMany(page, perPage int) ([]TimeSeries, int, error) {
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
		return []TimeSeries{}, 0, err
	}

	// TimeSeriesList is empty
	if len(pagedKeys) == 0 {
		return []TimeSeries{}, total, nil
	}

	datasources := make([]TimeSeries, 0, len(pagedKeys))
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
func (ms *MemoryStorage) FilterOne(path, op, value string) (*TimeSeries, error) {
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	// return the first one found
	for _, ts := range ms.data {
		matched, err := utils.MatchObject(ts, pathTknz, op, value)
		if err != nil {
			return nil, err
		}
		if matched {
			return ts, nil
		}
	}

	return nil, nil
}

// Filter multiple registrations
func (ms *MemoryStorage) Filter(path, op, value string, page, perPage int) ([]TimeSeries, int, error) {
	matchedIDs := []string{}
	pathTknz := strings.Split(path, ".")

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	for _, ts := range ms.data {
		matched, err := utils.MatchObject(ts, pathTknz, op, value)
		if err != nil {
			return []TimeSeries{}, 0, err
		}
		if matched {
			matchedIDs = append(matchedIDs, ts.Name)
		}
	}

	keys, err := utils.GetPageOfSlice(matchedIDs, page, perPage, MaxPerPage)
	if err != nil {
		return []TimeSeries{}, 0, err
	}
	if len(keys) == 0 {
		return []TimeSeries{}, len(matchedIDs), nil
	}

	ts := make([]TimeSeries, 0, len(keys))
	//var ts []DataSource
	for _, k := range keys {
		ts = append(ts, *ms.data[k])
	}

	return ts, len(matchedIDs), nil
}
