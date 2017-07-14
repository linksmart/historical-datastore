// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

// LocalClient implements local registry client
type LocalClient struct {
	storage Storage
}

// NewLocalClient returns a new LocalClient given a storage
func NewLocalClient(storage Storage) Client {
	return &LocalClient{
		storage,
	}
}

// Add creates a DataSource
func (c *LocalClient) Add(r DataSource) (DataSource, error) {
	return c.storage.add(r)
}

// Update updates a DataSource
func (c *LocalClient) Update(id string, r DataSource) (DataSource, error) {
	return c.storage.update(id, r)
}

// Delete deletes a DataSource
func (c *LocalClient) Delete(id string) error {
	return c.storage.delete(id)
}

// Get retrieves a DataSource
func (c *LocalClient) Get(id string) (DataSource, error) {
	return c.storage.get(id)
}

// GetDataSources returns a slice of DataSources given:
// page - page in the collection
// perPage - number of entries per page
func (c *LocalClient) GetDataSources(page int, perPage int) ([]DataSource, int, error) {
	return c.storage.getMany(page, perPage)
}

// FindDataSource returns a single DataSource given: path, operation, value
func (c *LocalClient) FindDataSource(path, op, value string) (*DataSource, error) {
	return c.storage.pathFilterOne(path, op, value)
}

// FindDataSources returns a slice of DataSources given: path, operation, value, page, perPage
func (c *LocalClient) FindDataSources(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	return c.storage.pathFilter(path, op, value, page, perPage)
}
