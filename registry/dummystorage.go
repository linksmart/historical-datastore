// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import "time"

type DummyRegistryStorage struct{}

func (s *DummyRegistryStorage) add(ds DataSource) (DataSource, error) {
	return DataSource{}, nil
}
func (s *DummyRegistryStorage) update(id string, ds DataSource) (DataSource, error) {
	return DataSource{}, nil
}
func (s *DummyRegistryStorage) delete(id string) error {
	return nil
}
func (s *DummyRegistryStorage) get(id string) (DataSource, error) {
	if id == "12345" {
		return DataSource{
			ID:       "12345",
			Resource: "http://example.com/sensor1",
		}, nil
	} else if id == "67890" {
		return DataSource{
			ID:       "67890",
			Resource: "http://example.com/sensor2",
		}, nil
	} else if id == "1337" {
		return DataSource{
			ID:       "1337",
			Resource: "http://example.com/sensor3",
		}, nil
	}
	return DataSource{}, nil
}
func (s *DummyRegistryStorage) getMany(page, perPage int) ([]DataSource, int, error) {
	return []DataSource{}, 0, nil
}
func (s *DummyRegistryStorage) getCount() (int, error) {
	return 0, nil
}
func (s *DummyRegistryStorage) pathFilterOne(path, op, value string) (*DataSource, error) {
	return nil, nil
}
func (s *DummyRegistryStorage) pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	return []DataSource{}, 0, nil
}
func (s *DummyRegistryStorage) modifiedDate() (time.Time, error) {
	return time.Now(), nil
}