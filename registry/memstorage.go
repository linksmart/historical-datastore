package registry

import (
	"fmt"
	"sync"
	//"time"
	"errors"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/code.google.com/p/go-uuid/uuid"
)

var ErrorNotFound = errors.New("NotFound")

// In-memory storage
type MemoryStorage struct {
	data map[string]DataSource
	//index []string
	mutex sync.RWMutex
}

func (ms *MemoryStorage) add(ds *DataSource) error {

	// Get a new UUID and convert it to string (UUID type can't be used as map-key)
	newUUID := fmt.Sprint(uuid.NewRandom())
	fmt.Println("New unique id: ", newUUID)
	ds.ID = newUUID
	ds.URL = fmt.Sprintf("%s/%s", ds.URL, ds.ID)   // append id to url of data source
	ds.Data = fmt.Sprintf("%s/%s", ds.Data, ds.ID) // append id to url of data api

	ms.data = make(map[string]DataSource)
	ms.data[newUUID] = *ds
	fmt.Println("Added DS: ", ms.data[newUUID])

	return nil
}

func (ms *MemoryStorage) update(id string, ds *DataSource) error {
	ms.mutex.Lock()

	_, ok := ms.data[id]
	if !ok {
		ms.mutex.Unlock()
		return ErrorNotFound
	}

	ms.data[id] = *ds
	ms.mutex.Unlock()

	return nil
}

func (ms *MemoryStorage) delete(id string) error {
	ms.mutex.Lock()

	_, ok := ms.data[id]
	if !ok {
		ms.mutex.Unlock()
		return ErrorNotFound
	}

	delete(ms.data, id)
	ms.mutex.Unlock()

	return nil
}

func (ms *MemoryStorage) get(id string) (DataSource, error) {
	fmt.Println("Getting ds with id: ", id)
	fmt.Println("Content: ", ms.data[id])

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
	// TODO
	return []DataSource{}, 0, nil
}

func getCount() int {
	// TODO
	return 0
}

func pathFilterOne(path, op, value string) (DataSource, error) {
	// TODO
	return DataSource{}, nil
}

func pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	// TODO
	return []DataSource{}, 0, nil
}
