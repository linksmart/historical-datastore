// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/linksmart/historical-datastore/common"
)

// Generate dummy time series
func generateDummyData(quantity int, storage Storage) ([]string, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	randInt := func(min int, max int) int {
		return min + rand.Intn(max-min)
	}

	var IDs []string
	for i := 1; i <= quantity; i++ {
		var ts TimeSeries
		ts.Name = fmt.Sprintf("http://example.com/sensor%d", i)
		ts.Meta = make(map[string]interface{})
		ts.Meta["SerialNumber"] = randInt(10000, 99999)
		//ts.Retention = fmt.Sprintf("%d%s", randInt(1, 20), []string{"m", "h", "d", "w"}[randInt(0, 3)])
		//ts.Aggregation TODO
		ts.Type = []ValueType{Float, Bool, String}[randInt(0, 2)]

		newTS, err := storage.Add(ts)
		if err != nil {
			return nil, fmt.Errorf("error adding dummy: %s", err)
		}
		IDs = append(IDs, newTS.Name) // add the generated id
	}

	return IDs, nil
}

func setupMemStorage() Storage {
	return NewMemoryStorage(common.RegConf{})
}

func TestMemstorageAdd(t *testing.T) {
	var ts TimeSeries
	ts.Name = "any_url"
	//ts.Aggregation TODO
	ts.Type = String

	storage := setupMemStorage()
	addedTS, err := storage.Add(ts)
	if err != nil {
		t.Fatalf("Received unexpected error on add: %v", err.Error())
	}

	getTS, err := storage.Get(addedTS.Name)
	if err != nil {
		t.Errorf("Received unexpected error on get: %v", err.Error())
	}

	// compare added and retrieved data
	if !reflect.DeepEqual(addedTS, getTS) {
		t.Fatalf("Mismatch added:\n%v\n and retrieved:\n%v\n", addedTS, getTS)
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

	ts, err := storage.Get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	//ts.Aggregation TODO

	updatedDS, err := storage.Update(ID, *ts)
	if err != nil {
		t.Fatalf("Unexpected error on update: %v", err.Error())
	}

	// compare the updated and stored structs
	if !reflect.DeepEqual(updatedDS, ts) {
		t.Fatalf("Mismatch updated:\n%v\n and stored:\n%v\n", updatedDS, ts)
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
		t.Errorf("The previous call hasn't deleted the time series")
	}
}

func TestMemstorageGetMany(t *testing.T) {
	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage := setupMemStorage()
		_, err := generateDummyData(TOTAL, storage)
		if err != nil {
			t.Errorf("Unexpected error on generateDummyData: %v", err.Error())
		}
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
	_, err := generateDummyData(total, storage)
	if err != nil {
		t.Errorf("error generating dummy data")
	}
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

	targetTS, _ := storage.Get(ID)
	matchedTS, err := storage.FilterOne("name", "equals", targetTS.Name)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// check if target is returned
	if !reflect.DeepEqual(targetTS, matchedTS) {
		t.Fatalf("Looking for:\n%v\n but matched:\n%v\n", &targetTS, matchedTS)
	}
}

func TestMemstoragePathFilter(t *testing.T) {
	//t.Skip("Skip until there are more meta to add")
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
		ts, _ := storage.Get(IDs[i])
		ts.Meta["newkey"] = "a/b"
		_, err := storage.Update(ts.Name, *ts)
		if err != nil {
			t.Errorf("Error updating")
		}
	}

	// QueryPage for format with prefix "newtype"
	_, total, err := storage.Filter("meta.newkey", "prefix", "a", 1, 100)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if total != expected {
		t.Fatalf("Returned %d matches instead of %d", total, expected)
	}
}
