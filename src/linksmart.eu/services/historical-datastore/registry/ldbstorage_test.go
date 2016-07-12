// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"linksmart.eu/services/historical-datastore/common"
)

func setupLevelDB() (Storage, func() error, error) {
	// Temp database file
	// Replace Windows-based backslashes with slash (not parsed as Path by net/url)
	os_temp := strings.Replace(os.TempDir(), "\\", "/", -1)
	temp_file := fmt.Sprintf("%s/hds-test.ldb/%d.ldb", os_temp, time.Now().UnixNano())
	storage, in, closeDB, err := NewLevelDBStorage(temp_file, nil)
	if err != nil {
		return nil, nil, err
	}
	out := dummyListener()
	common.StartNotifier(in, out)

	return storage, closeDB, nil
}

// Remove temporary database files
func clean() {
	temp_dir := fmt.Sprintf("%s%chds-test.ldb", os.TempDir(), os.PathSeparator)
	err := os.RemoveAll(temp_dir)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestLevelDBAdd(t *testing.T) {
	storage, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean()
	defer closeDB()

	var ds DataSource
	ds.Resource = "any_url"
	ds.Meta = make(map[string]interface{})
	ds.Meta["SerialNumber"] = 12345
	ds.Retention = "2w"
	//ds.Aggregation TODO
	ds.Type = "string"
	ds.Format = "application/senml+json"

	addedDS, err := storage.add(ds)
	if err != nil {
		t.Fatalf("Received unexpected error on add: %v", err.Error())
	}

	getDS, err := storage.get(addedDS.ID)
	if err != nil {
		t.Fatalf("Received unexpected error on get: %v", err.Error())
	}

	// compare added and retrieved data
	addedBytes, _ := json.Marshal(&addedDS)
	getBytes, _ := json.Marshal(&getDS)
	if string(getBytes) != string(addedBytes) {
		t.Fatalf("Mismatch:\n added:\n%v\n retrieved:\n%v\n", string(addedBytes), string(getBytes))
	}
}

func TestLevelDBGet(t *testing.T) {
	t.Skip("Tested in TestLevelDBAdd")
}

func TestLevelDBUpdate(t *testing.T) {
	storage, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean()
	defer closeDB()

	IDs, err := generateDummyData(1, NewLocalClient(storage))
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	ds, err := storage.get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	// Update the following
	ds.Meta = make(map[string]interface{})
	ds.Meta["SerialNumber"] = 12345
	ds.Retention = "20w"
	//ds.Aggregation TODO
	ds.Format = "new_format"

	updatedDS, err := storage.update(ID, ds)
	if err != nil {
		t.Fatalf("Unexpected error on update: %v", err.Error())
	}

	// compare the updated and stored structs
	updatedBytes, _ := json.Marshal(&updatedDS)
	dsBytes, _ := json.Marshal(&ds)
	if string(updatedBytes) != string(dsBytes) {
		t.Fatalf("Mismatch updated:\n%v\n and stored:\n%v\n", string(updatedBytes), string(dsBytes))
	}
}

func TestLevelDBDelete(t *testing.T) {
	storage, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean()
	defer closeDB()

	IDs, err := generateDummyData(1, NewLocalClient(storage))
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	err = storage.delete(ID)
	if err != nil {
		t.Errorf("Unexpected error on delete: %v\n", err.Error())
	}

	_, err = storage.get(ID)
	if err == nil {
		t.Error("The previous call hasn't deleted the datasource!")
	}
}

func TestLevelDBGetMany(t *testing.T) {

	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage, closeDB, err := setupLevelDB()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer clean()
		defer closeDB()

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

func TestLevelDBGetCount(t *testing.T) {
	storage, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean()
	defer closeDB()

	// Get the current total
	c1, err := storage.getCount()
	if err != nil {
		t.Fatal(err.Error())
	}

	// Add few datasources
	const total = 5
	generateDummyData(total, NewLocalClient(storage))

	c2, err := storage.getCount()
	if err != nil {
		t.Fatal(err.Error())
	}
	if total != c2-c1 {
		t.Fatalf("Created %d but counted %d datasources!", total, c2-c1)
	}
}

func TestLevelDBPathFilterOne(t *testing.T) {
	storage, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean()
	defer closeDB()

	IDs, err := generateDummyData(10, NewLocalClient(storage))
	if err != nil {
		t.Fatalf(err.Error())
	}
	ID := IDs[0]

	targetDS, _ := storage.get(ID)
	matchedDS, err := storage.pathFilterOne("id", "equals", targetDS.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// check if target is returned
	targetBytes, _ := json.Marshal(&targetDS)
	matchedBytes, _ := json.Marshal(&matchedDS)
	if string(targetBytes) != string(matchedBytes) {
		t.Fatalf("Looking for:\n%v\n but matched:\n%v\n", string(targetBytes), string(matchedBytes))
	}
}

func TestLevelDBPathFilter(t *testing.T) {
	storage, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean()
	defer closeDB()

	IDs, err := generateDummyData(10, NewLocalClient(storage))
	if err != nil {
		t.Fatal(err.Error())
	}
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
