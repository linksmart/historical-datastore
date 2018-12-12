// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
)

// Generate dummy data sources
func generateDummyData(quantity int, storage Storage) ([]string, error) {
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
		//ds.Retention = fmt.Sprintf("%d%s", randInt(1, 20), []string{"m", "h", "d", "w"}[randInt(0, 3)])
		//ds.Aggregation TODO
		ds.Type = []string{"float", "bool", "string"}[randInt(0, 2)]

		newDS, err := storage.Add(ds)
		if err != nil {
			return nil, fmt.Errorf("Error adding dummy: %s", err)
		}
		IDs = append(IDs, newDS.ID) // add the generated id
	}

	return IDs, nil
}

func setupMemStorage() Storage {
	return NewMemoryStorage(common.RegConf{})
}

func TestMemstorageAdd(t *testing.T) {
	var ds DataSource
	ds.Resource = "any_url"
	ds.Meta = make(map[string]interface{})
	ds.Meta["SerialNumber"] = 12345
	ds.Retention = ""
	//ds.Aggregation TODO
	ds.Type = "string"

	storage := setupMemStorage()
	addedDS, err := storage.Add(ds)
	if err != nil {
		t.Errorf("Received unexpected error on add: %v", err.Error())
	}

	getDS, err := storage.Get(addedDS.ID)
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
	IDs, err := generateDummyData(1, storage)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	ds, err := storage.Get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	// Update the following
	ds.Meta = make(map[string]interface{})
	ds.Meta["SerialNumber"] = 12345
	ds.Retention = "20w"
	//ds.Aggregation TODO

	updatedDS, err := storage.Update(ID, ds)
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

	IDs, err := generateDummyData(1, storage)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	err = storage.Delete(ID)
	if err != nil {
		t.Errorf("Unexpected error on delete: %v", err.Error())
	}

	err = storage.Delete(ID)
	if err == nil {
		t.Errorf("The previous call hasn't deleted the data source: %v", err.Error())
	}
}

func TestMemstorageGetMany(t *testing.T) {
	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage := setupMemStorage()
		generateDummyData(TOTAL, storage)

		_, total, _ := storage.GetMany(1, perPage)
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

			DSs, _, _ := storage.GetMany(page, perPage)
			if len(DSs) != inThisPage {
				t.Errorf("Wrong number of entries per page. Returned %d instead of %d", len(DSs), inThisPage)
			}
		}
	}
	subTest(0, 10)
	subTest(10, 10)
	subTest(55, 10)
	subTest(55, 1)
}

func TestMemstorageGetCount(t *testing.T) {
	storage := setupMemStorage()
	const total = 5
	generateDummyData(total, storage)

	c, _ := storage.getTotal()
	if c != total {
		t.Errorf("Stored %d but counted %d", total, c)
	}
}

func TestMemstoragePathFilterOne(t *testing.T) {
	storage := setupMemStorage()

	IDs, err := generateDummyData(10, storage)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	targetDS, _ := storage.Get(ID)
	matchedDS, err := storage.FilterOne("id", "equals", targetDS.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// check if target is returned
	if !reflect.DeepEqual(&targetDS, matchedDS) {
		t.Fatalf("Looking for:\n%v\n but matched:\n%v\n", &targetDS, matchedDS)
	}
}

func TestMemstoragePathFilter(t *testing.T) {
	storage := setupMemStorage()

	IDs, err := generateDummyData(10, storage)
	if err != nil {
		t.Fatal(err.Error())
	}
	expected := 3

	// Modify some of them
	if len(IDs) < expected {
		t.Fatalf("Need more dummies!")
	}
	for i := 0; i < expected; i++ {
		ds, _ := storage.Get(IDs[i])
		ds.Meta["newkey"] = "a/b"
		storage.Update(ds.ID, ds)
	}

	// Query for format with prefix "newtype"
	_, total, err := storage.Filter("meta.newkey", "prefix", "a", 1, 100)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if total != expected {
		t.Fatalf("Returned %d matches instead of %d", total, expected)
	}
}
