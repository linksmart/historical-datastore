package registry

import "net/url"

// TODO: remove when memstorage is implemented (and rewrite data api tests)
type DummyRegistryStorage struct{}

func (s *DummyRegistryStorage) add(ds *DataSource) error {
	// TODO
	return nil
}

func (s *DummyRegistryStorage) update(id string, ds *DataSource) error {
	// TODO
	return nil
}

func (s *DummyRegistryStorage) delete(id string) error {
	// TODO
	return nil
}

func (s *DummyRegistryStorage) get(id string) (DataSource, error) {
	if id == "12345" {
		u, _ := url.Parse("http://example.com/sensor1")
		return DataSource{
			ID:       "12345",
			Resource: *u,
		}, nil
	} else if id == "67890" {
		u, _ := url.Parse("http://example.com/sensor2")
		return DataSource{
			ID:       "67890",
			Resource: *u,
		}, nil
	} else if id == "1337" {
		u, _ := url.Parse("http://example.com/sensor3")
		return DataSource{
			ID:       "1337",
			Resource: *u,
		}, nil
	}
	return DataSource{}, nil
}

func (s *DummyRegistryStorage) getMany(page, perPage int) ([]DataSource, int, error) {
	// TODO
	return []DataSource{}, 0, nil
}

func (s *DummyRegistryStorage) getCount() int {
	// TODO
	return 0
}

func (s *DummyRegistryStorage) pathFilterOne(path, op, value string) (DataSource, error) {
	// TODO
	return DataSource{}, nil
}

func (s *DummyRegistryStorage) pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	// TODO
	return []DataSource{}, 0, nil
}
