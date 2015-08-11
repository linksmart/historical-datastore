package registry

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"linksmart.eu/services/historical-datastore/common"
)

func setupMemStorage() Storage {
	storage, _ := NewMemoryStorage()
	return storage
}

func TestMemstorageAdd(t *testing.T) {
	var ds DataSource
	ds.Resource = "any_url"
	ds.Meta = make(map[string]interface{})
	ds.Meta["SerialNumber"] = 12345
	ds.Retention.Policy = "1w"
	ds.Retention.Duration = "2w"
	//ds.Aggregation TODO
	ds.Type = "string"
	ds.Format = "application/senml+json"

	storage := setupMemStorage()
	addedDS, err := storage.add(ds)
	if err != nil {
		t.Errorf("Received unexpected error on add: %v", err.Error())
	}

	getDS, err := storage.get(addedDS.ID)
	if err != nil {
		t.Errorf("Received unexpected error on get: %v", err.Error())
	}

	// compare added and retrieved data
	if !reflect.DeepEqual(addedDS, getDS) {
		t.Fatalf("Mismatch added:\n%v\n and retrieved:\n%v\n", addedDS, getDS)
	}
}

func TestMemstorageGet(t *testing.T) {
	t.Skip("Tested in TestMemstorageAdd")
}

func TestMemstorageUpdate(t *testing.T) {
	storage := setupMemStorage()
	ID := generateDummyData(1, NewLocalClient(storage))[0]

	ds, err := storage.get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	// Update the following
	ds.Meta = make(map[string]interface{})
	ds.Meta["SerialNumber"] = 12345
	ds.Retention.Policy = "10w"
	ds.Retention.Duration = "20w"
	//ds.Aggregation TODO
	ds.Format = "new_format"

	updatedDS, err := storage.update(ID, ds)
	if err != nil {
		t.Fatalf("Unexpected error on update: %v", err.Error())
	}

	// compare the updated and stored structs
	if !reflect.DeepEqual(updatedDS, ds) {
		t.Fatalf("Mismatch updated:\n%v\n and stored:\n%v\n", updatedDS, ds)
	}
}

func TestMemstorageDelete(t *testing.T) {
	storage := setupMemStorage()
	ID := generateDummyData(1, NewLocalClient(storage))[0]

	err := storage.delete(ID)
	if err != nil {
		t.Error("Unexpected error on delete: %v", err.Error())
	}

	err = storage.delete(ID)
	if err != ErrorNotFound {
		t.Error("The previous call hasn't deleted the Service!")
	}
}

func TestMemstorageGetMany(t *testing.T) {
	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage := setupMemStorage()
		generateDummyData(TOTAL, NewLocalClient(storage))

		_, total, _ := storage.getMany(1, perPage)
		if total != TOTAL {
			t.Errorf("Returned total is %d instead of %d", total, TOTAL)
		}

		pages := int(math.Ceil(float64(TOTAL) / float64(perPage)))
		for page := 1; page <= pages; page++ {
			// Find out how many items should be expected on this page
			inThisPage := perPage
			if (TOTAL - (page-1)*perPage) < perPage {
				inThisPage = int(math.Mod(float64(TOTAL), float64(perPage)))
			}

			DSs, _, _ := storage.getMany(page, perPage)
			if len(DSs) != inThisPage {
				t.Errorf("Wrong number of entries per page. Returned %d instead of %d", len(DSs), inThisPage)
			}
		}
	}

	subTest(0, 10)
	subTest(10, 10)
	subTest(55, 10)
	subTest(55, 0)
}

func TestMemstorageGetCount(t *testing.T) {
	storage := setupMemStorage()
	total := 5
	generateDummyData(total, NewLocalClient(storage))

	if storage.getCount() != total {
		t.Errorf("Stored %d but counted %d", total, storage.getCount())
	}
}

func TestMemstoragePathFilterOne(t *testing.T) {
	storage := setupMemStorage()
	ID := generateDummyData(10, NewLocalClient(storage))[0]

	targetDS, _ := storage.get(ID)
	matchedDS, err := storage.pathFilterOne("id", "equals", targetDS.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// check if target is returned
	if !reflect.DeepEqual(targetDS, matchedDS) {
		t.Fatalf("Looking for:\n%v\n but matched:\n%v\n", targetDS, matchedDS)
	}
}

func TestMemstoragePathFilter(t *testing.T) {
	storage := setupMemStorage()
	IDs := generateDummyData(10, NewLocalClient(storage))
	expected := 3

	// Modify some of them
	if len(IDs) < expected {
		t.Fatalf("Need more dummies!")
	}
	for i := 0; i < expected; i++ {
		ds, _ := storage.get(IDs[i])
		ds.Format = "newtype/newsubtype"
		storage.update(ds.ID, ds)
	}

	// Query for format with prefix "newtype"
	_, total, err := storage.pathFilter("format", "prefix", "newtype", 1, 100)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if total != expected {
		t.Fatalf("Returned %d matches instead of %d", total, expected)
	}
}

// Generate dummy data sources
func generateDummyData(quantity int, c Client) []string {
	rand.Seed(time.Now().UTC().UnixNano())

	randInt := func(min int, max int) int {
		return min + rand.Intn(max-min)
	}

	var IDs []string
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

		newDS, _ := c.Add(ds)
		IDs = append(IDs, newDS.ID) // add the generated id
	}

	return IDs
}