// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package catalog

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"

	"github.com/linksmart/service-catalog/v2/utils"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// LevelDB storage
type LevelDBStorage struct {
	db *leveldb.DB
	wg sync.WaitGroup
}

func NewLevelDBStorage(dsn string, opts *opt.Options) (Storage, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return &LevelDBStorage{}, err
	}

	// Open the database file
	db, err := leveldb.OpenFile(url.Path, opts)
	if err != nil {
		return &LevelDBStorage{}, err
	}

	return &LevelDBStorage{db: db}, nil
}

// CRUD
func (ls *LevelDBStorage) add(s *Service) error {

	bytes, err := json.Marshal(s)
	if err != nil {
		return err
	}

	_, err = ls.db.Get([]byte(s.ID), nil)
	if err == nil {
		return &ConflictError{"Service id is not unique."}
	} else if err != leveldb.ErrNotFound {
		return err
	}

	err = ls.db.Put([]byte(s.ID), bytes, nil)
	if err != nil {
		return err
	}

	return nil
}

func (ls *LevelDBStorage) get(id string) (*Service, error) {

	bytes, err := ls.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return nil, &NotFoundError{fmt.Sprintf("Service with id %s is not found", id)}
	} else if err != nil {
		return nil, err
	}

	var s Service
	err = json.Unmarshal(bytes, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func (ls *LevelDBStorage) update(id string, s *Service) error {

	bytes, err := json.Marshal(s)
	if err != nil {
		return err
	}
	err = ls.db.Put([]byte(id), bytes, nil)
	if err == leveldb.ErrNotFound {
		return &NotFoundError{fmt.Sprintf("Service with id %s is not found", id)}
	} else if err != nil {
		return err
	}

	return nil
}

func (ls *LevelDBStorage) delete(id string) error {

	err := ls.db.Delete([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return &NotFoundError{fmt.Sprintf("Service with id %s is not found", id)}
	} else if err != nil {
		return err
	}

	return nil
}

// Utilities

func (ls *LevelDBStorage) list(page int, perPage int) ([]Service, int, error) {

	total, err := ls.total()
	if err != nil {
		return nil, 0, err
	}
	offset, limit, err := utils.GetPagingAttr(total, page, perPage, MaxPerPage)
	if err != nil {
		return nil, 0, &BadRequestError{fmt.Sprintf("Unable to paginate: %s", err)}
	}

	// TODO: is there a better way to do this?
	// github.com/syndtr/goleveldb/leveldb/iterator
	services := make([]Service, limit)

	ls.wg.Add(1)
	defer ls.wg.Done()
	iter := ls.db.NewIterator(nil, nil)
	defer iter.Release()

	i := 0
	for iter.Next() {
		var s Service
		err = json.Unmarshal(iter.Value(), &s)
		if err != nil {
			return nil, 0, err
		}

		if i >= offset && i < offset+limit {
			services[i-offset] = s
		} else if i >= offset+limit {
			break
		}
		i++
	}

	err = iter.Error()
	if err != nil {
		return nil, 0, err
	}

	return services, total, nil
}

func (s *LevelDBStorage) total() (int, error) {
	c := 0
	s.wg.Add(1)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		c++
	}
	iter.Release()
	s.wg.Done()
	err := iter.Error()
	if err != nil {
		return 0, err
	}

	return c, nil
}

func (s *LevelDBStorage) iterator() <-chan *Service {
	serviceIter := make(chan *Service)

	go func() {
		defer close(serviceIter)

		s.wg.Add(1)
		defer s.wg.Done()
		iter := s.db.NewIterator(nil, nil)
		defer iter.Release()

		for iter.Next() {
			var service Service
			err := json.Unmarshal(iter.Value(), &service)
			if err != nil {
				logger.Printf("LevelDB Error: %s", err)
				return
			}
			serviceIter <- &service
		}

		err := iter.Error()
		if err != nil {
			logger.Printf("LevelDB Error: %s", err)
		}
	}()

	return serviceIter
}

func (s *LevelDBStorage) Close() error {
	s.wg.Wait()
	return s.db.Close()
}
