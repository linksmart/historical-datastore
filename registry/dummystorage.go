// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import "time"

type DummyRegistryStorage struct{}

func (s *DummyRegistryStorage) Add(ds DataSource) (DataSource, error) {
	return DataSource{}, nil
}
func (s *DummyRegistryStorage) Update(id string, ds DataSource) (DataSource, error) {
	return DataSource{}, nil
}
func (s *DummyRegistryStorage) Delete(id string) error {
	return nil
}
func (s *DummyRegistryStorage) Get(id string) (DataSource, error) {
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
func (s *DummyRegistryStorage) GetMany(page, perPage int) ([]DataSource, int, error) {
	return []DataSource{}, 0, nil
}
func (s *DummyRegistryStorage) getTotal() (int, error) {
	return 0, nil
}
func (s *DummyRegistryStorage) FilterOne(path, op, value string) (*DataSource, error) {
	return nil, nil
}
func (s *DummyRegistryStorage) Filter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	return []DataSource{}, 0, nil
}
func (s *DummyRegistryStorage) getLastModifiedTime() (time.Time, error) {
	return time.Now(), nil
}
