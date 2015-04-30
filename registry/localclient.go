package registry

import (
	"fmt"
	"linksmart.eu/services/historical-datastore/common"
	"math/rand"
	"time"
)

// LocalClient implements local registry client
type LocalClient struct {
	storage Storage
}

// NewLocalClient returns a new LocalClient given a storage
func NewLocalClient(storage Storage) *LocalClient {
	return &LocalClient{
		storage,
	}
}

// Add creates a DataSource
func (c *LocalClient) Add(r *DataSource) error {
	return c.storage.add(r)
}

// Update updates a DataSource
func (c *LocalClient) Update(id string, r *DataSource) error {
	return c.storage.update(id, r)
}

// Delete deletes a DataSource
func (c *LocalClient) Delete(id string) error {
	return c.storage.delete(id)
}

// Get retrieves a DataSource
func (c *LocalClient) Get(id string) (*DataSource, error) {
	ds, err := c.storage.get(id)
	return &ds, err
}

// GetDataSources returns a slice of DataSources given:
// page - page in the collection
// perPage - number of entries per page
func (c *LocalClient) GetDataSources(page int, perPage int) ([]DataSource, int, error) {
	return c.storage.getMany(page, perPage)
}

// FindDataSource returns a single DataSource given: path, operation, value
func (c *LocalClient) FindDataSource(path, op, value string) (*DataSource, error) {
	ds, err := c.storage.pathFilterOne(path, op, value)
	return &ds, err
}

// FindDataSources returns a slice of DataSources given: path, operation, value, page, perPage
func (c *LocalClient) FindDataSources(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	return c.storage.pathFilter(path, op, value, page, perPage)
}

// Generate dummy data sources
func (c *LocalClient) GenerateDummyData(quantity int) {
	rand.Seed(time.Now().UTC().UnixNano())

	fmt.Printf(">>> NOTE: GENERATING %d DUMMY DATASOURCES <<<\n", quantity)
	for i := 1; i <= quantity; i++ {
		var ds DataSource
		ds.Resource = fmt.Sprintf("http://example.com/sensor%d", i)
		ds.Meta = make(map[string]interface{})
		ds.Meta["SerialNumber"] = randInt(10000, 99999)
		ds.Retention.Policy = fmt.Sprintf("%d%s", randInt(1, 20), common.RetentionPeriods()[randInt(0, 3)])
		ds.Retention.Duration = fmt.Sprintf("%d%s", randInt(1, 20), common.RetentionPeriods()[randInt(0, 3)])
		//ds.Aggregation TODO
		ds.Type = common.SupportedTypes()[randInt(0, 2)]
		ds.Format = "application/senml+json"

		c.storage.add(&ds)
	}

}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
