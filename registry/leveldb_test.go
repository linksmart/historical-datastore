// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/linksmart/historical-datastore/common"
)

func setupLevelDB() (Storage, string, func() error, error) {
	// Temp database file
	// Replace Windows-based backslashes with slash (not parsed as Path by net/url)
	os_temp := strings.Replace(os.TempDir(), "\\", "/", -1)
	dbName := fmt.Sprintf("%d.ldb", time.Now().UnixNano())
	temp_file := fmt.Sprintf("%s/hds-test/%s", os_temp, dbName)
	conf := common.RegConf{
		Backend: common.RegBackendConf{
			DSN: temp_file,
		},
	}
	storage, closeDB, err := NewLevelDBStorage(conf, nil)
	if err != nil {
		return nil, dbName, nil, err
	}

	return storage, dbName, closeDB, nil
}

// Remove temporary database files
func clean(dbName string) {
	temp_dir := fmt.Sprintf("%s/hds-test/%s", os.TempDir(), dbName)
	err := os.RemoveAll(temp_dir)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestLevelDBAdd(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	var ds DataStream
	ds.Name = "any_url"
	//ds.Retention = ""
	//ds.Aggregation TODO
	ds.Type = String

	addedDS, err := storage.Add(ds)
	if err != nil {
		t.Fatalf("Received unexpected error on add: %v", err.Error())
	}

	getDS, err := storage.Get(addedDS.Name)
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
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	IDs, err := generateDummyData(1, storage)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	ds, err := storage.Get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	updatedDS, err := storage.Update(ID, *ds)
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
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	IDs, err := generateDummyData(1, storage)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	err = storage.Delete(ID)
	if err != nil {
		t.Errorf("Unexpected error on delete: %v\n", err.Error())
	}

	_, err = storage.Get(ID)
	if err == nil {
		t.Error("The previous call hasn't deleted the datastream!")
	}
}

func TestLevelDBGetMany(t *testing.T) {

	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage, dbName, closeDB, err := setupLevelDB()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer clean(dbName)
		defer closeDB()

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

func TestLevelDBGetCount(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	// Get the current total
	c1, err := storage.getTotal()
	if err != nil {
		t.Fatal(err.Error())
	}

	// Add few datastreams
	const total = 5
	generateDummyData(total, storage)

	c2, err := storage.getTotal()
	if err != nil {
		t.Fatal(err.Error())
	}
	if total != c2-c1 {
		t.Fatalf("Created %d but counted %d datastreams!", total, c2-c1)
	}
}

func TestLevelDBPathFilterOne(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	IDs, err := generateDummyData(10, storage)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ID := IDs[0]

	targetDS, _ := storage.Get(ID)
	matchedDS, err := storage.FilterOne("name", "equals", targetDS.Name)
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
	//t.Skip("Skip until there are more meta to add")
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

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
		storage.Update(ds.Name, *ds)
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
